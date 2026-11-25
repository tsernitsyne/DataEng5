import argparse
import csv
import hashlib
import io
import json
import os
import re
from collections import Counter
from contextlib import closing, contextmanager, ExitStack
from itertools import combinations
from pathlib import Path
from tempfile import TemporaryDirectory

BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
PHP_IN_TITLE = re.compile(r"(?<!\w)php(?!\w)", re.IGNORECASE)
MIB = 1024**2
csv.field_size_limit(20_000_000)


def s3_client(endpoint, **credentials):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3", endpoint_url=endpoint, region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"), **credentials,
    )


class S3Stream(io.RawIOBase):
    def __init__(self, body):
        self.body = body

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.body.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def close(self):
        self.body.close()
        super().close()


@contextmanager
def open_csv(path, s3=None):
    if s3 is None:
        with open(path, encoding="utf-8-sig", newline="") as stream:
            yield csv.DictReader(stream)
    else:
        body = s3.get_object(Bucket=BUCKET, Key=path)["Body"]
        with io.TextIOWrapper(io.BufferedReader(S3Stream(body), 128 * 1024),
                              encoding="utf-8-sig", newline="") as stream:
            yield csv.DictReader(stream)


# Filtering and normalization are the size-2 branch of the HW3 mapper.
def count_pairs(rows):
    counts = Counter()
    selected = 0
    input_rows = 0
    for row in rows:
        input_rows += 1
        role = (row["role"] or "").strip().lower()
        if role != "программист, разработчик" or not PHP_IN_TITLE.search(row["name"] or ""):
            continue
        selected += 1
        skills = sorted({part.strip().lower() for part in (row["key_skills"] or "").split(";")
                         if part.strip()})
        counts.update(combinations(skills, 2))
    return counts, {"input_rows": input_rows, "selected_rows": selected}


def split_csv(reader, paths):
    # Parse logical CSV records, including fields containing embedded newlines.
    with ExitStack() as opened:
        writers = [
            csv.DictWriter(opened.enter_context(path.open("w", encoding="utf-8", newline="")),
                           fieldnames=reader.fieldnames)
            for path in paths
        ]
        for writer in writers:
            writer.writeheader()
        row_counts = [0] * len(writers)
        for index, row in enumerate(reader):
            part = index % len(writers)
            writers[part].writerow(row)
            row_counts[part] += 1
    return row_counts


def serialize_counts(counts):
    text = io.StringIO(newline="")
    writer = csv.writer(text, lineterminator="\n")
    writer.writerow(("skills", "frequency"))
    for skills, count in sorted(counts.items()):
        writer.writerow((json.dumps(skills, ensure_ascii=False), count))
    return text.getvalue()


def read_counts(rows):
    return Counter({tuple(json.loads(row["skills"])): int(row["frequency"]) for row in rows})


def ranked(counts):
    return sorted(counts.items(), key=lambda row: (-row[1], row[0]))[:10]


def verify(counts, reference):
    if counts != reference:
        raise RuntimeError("Pair counts differ from the HW3 reference")
    return {"all_pairs_equal": True, "top10": ranked(counts)}


def summary(counts):
    return {"distinct_pairs": len(counts), "pair_occurrences": sum(counts.values())}


def merge_counts(parts):
    counts = Counter()
    for part in parts:
        counts.update(part)
    return counts


def peak_memory():
    candidates = (
        ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory.max"),
        ("/sys/fs/cgroup/memory/memory.max_usage_in_bytes",
         "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    )
    for peak, limit in candidates:
        if Path(peak).is_file():
            return {"peak_bytes": int(Path(peak).read_text()),
                    "cgroup_limit": Path(limit).read_text().strip(), "peak_source": peak}
    raise RuntimeError("Container cgroup peak counter is unavailable")


def log(record):
    print(json.dumps(record, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    local = sub.add_parser("local")
    for option in ("csv", "reference", "output"):
        local.add_argument("--" + option, type=Path, required=True)
    for mode in ("count", "split", "map", "reduce", "identity"):
        command = sub.add_parser(mode)
        if mode != "identity":
            command.add_argument("--prefix", required=True)
        if mode in ("split", "reduce"):
            command.add_argument("--parts", required=True, type=int)
    args = parser.parse_args()

    if args.mode == "local":
        with open_csv(args.csv) as rows:
            counts, stats = count_pairs(rows)
        with open_csv(args.reference) as rows:
            comparison = verify(counts, read_counts(rows))
        args.output.write_text(serialize_counts(counts), encoding="utf-8", newline="")
        log({"mode": "local", "input_bytes": args.csv.stat().st_size,
             "input_sha256": file_sha256(args.csv), **stats, **summary(counts), **comparison})
        return

    log({"mode": args.mode, "stage": "start"})
    s3 = s3_client(os.environ["S3_ENDPOINT"])
    if args.mode == "identity":
        result = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
        with closing(result["Body"]) as body:
            digest = hashlib.sha256()
            for chunk in body.iter_chunks(1024**2):
                digest.update(chunk)
        log({"mode": "identity", "bytes": result["ContentLength"], "sha256": digest.hexdigest()})
        return

    prefix = args.prefix
    if args.mode == "split":
        with TemporaryDirectory() as temp:
            paths = [Path(temp) / f"part-{part}.csv" for part in range(args.parts)]
            with open_csv(RAW_KEY, s3) as rows:
                row_counts = split_csv(rows, paths)
            for part, path in enumerate(paths):
                s3.upload_file(str(path), BUCKET, f"{prefix}/parts/{part}.csv")
        log({"mode": "split", "rows_per_part": row_counts, "input_rows": sum(row_counts)})
        return

    if args.mode == "reduce":
        def read_maps():
            for part in range(args.parts):
                with open_csv(f"{prefix}/maps/{part}.csv", s3) as rows:
                    yield read_counts(rows)

        counts = merge_counts(read_maps())
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}/pairs.csv",
                      Body=serialize_counts(counts).encode("utf-8"))
        log({"mode": "reduce", **summary(counts), "top10": ranked(counts)})
        return

    index = int(os.environ["JOB_COMPLETION_INDEX"]) if args.mode == "map" else None
    key = RAW_KEY if index is None else f"{prefix}/parts/{index}.csv"
    output = f"{prefix}/single.csv" if index is None else f"{prefix}/maps/{index}.csv"
    with open_csv(key, s3) as rows:
        log({"mode": args.mode, "stage": "count", "key": key, "index": index})
        counts, stats = count_pairs(rows)
    s3.put_object(Bucket=BUCKET, Key=output, Body=serialize_counts(counts).encode("utf-8"))
    log({"mode": args.mode, "index": index, "key": key, "output": output,
         **stats, **summary(counts), **peak_memory()})


def file_sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


if __name__ == "__main__":
    main()
