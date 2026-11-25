import argparse
import json
import re
import shutil
import subprocess
import textwrap
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parent
REPORT = ROOT / "report"
MIB = 1024**2
TEX_SPECIAL = str.maketrans({"&": r"\&", "%": r"\%", "_": r"\_", "#": r"\#",
                             "$": r"\$", "{": r"\{", "}": r"\}",
                             "^": r"\textasciicircum{}", "~": r"\textasciitilde{}",
                             "\\": r"\textbackslash{}"})


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def integer(value):
    return f"{value:,}".replace(",", r"\,")


def decimal(value):
    return f"{value:.2f}".replace(".", ",")


def table_rows(rows):
    return "\n".join(" & ".join(map(str, row)) + r" \\" for row in rows)


def event_text(path):
    event = read_json(path)
    return event["reason"] + "\n" + textwrap.fill(event["message"], width=62,
                                                     break_long_words=False)


def worker_overlap(path):
    pods = read_json(path)
    intervals = [pod["status"]["containerStatuses"][0]["state"]["terminated"] for pod in pods]
    started = max(datetime.fromisoformat(state["startedAt"]) for state in intervals)
    finished = min(datetime.fromisoformat(state["finishedAt"]) for state in intervals)
    overlap = (finished - started).total_seconds()
    if overlap > 0:
        return (f"По отметкам запуска и завершения все четыре контейнера работали "
                f"одновременно {int(overlap)}~с при точности отметок до секунды.")
    return "Временные отметки с точностью до секунды позволяют сверить интервалы работы контейнеров."


def substitutions(directory, run):
    versions = (directory / "versions.log").read_text(encoding="utf-8")
    kube = re.search(r"^Server Version: (\S+)", versions, re.MULTILINE)
    if kube is None:
        raise ValueError("Kubernetes server version is missing")
    oom_pod, = read_json(directory / "pairs-oom.pods.json")
    oom_log = (directory / f"{oom_pod['metadata']['name']}.log").read_text(encoding="utf-8")
    oom_stage = ("Контейнер успел начать чтение CSV." if '"stage": "count"' in oom_log else
                 "Журнал контейнера обрывается после старта программы, до отметки о чтении CSV.")
    probe = run["memory_probe"]
    final = run["memory_final"]
    capacity = run["scheduler_capacity"]
    quota = run["quota"]
    digest = run["s3_identity"]["sha256"]
    return {
        "KUBE": kube.group(1),
        "OOM_CODE": run["oom"]["exit_code"],
        "OOM_STAGE": oom_stage,
        "PEAK_SOURCE": probe["peak_source"],
        "PROBE_PEAK": decimal(probe["peak_bytes"] / MIB),
        "FINAL_PEAK": decimal(final["peak_bytes"] / MIB),
        "LIMIT": run["selected_limit_mib"],
        "MIGRATION_ROWS": table_rows(
            (r"\texttt{" + item["key"].translate(TEX_SPECIAL) + "}", integer(item["bytes"]))
            for item in run["s3_migration"]["objects"]),
        "SHA": digest[:32] + "\n" + digest[32:],
        "WORKER_ROWS": table_rows(
            (row["index"], integer(row["input_rows"]), integer(row["selected_rows"]),
             integer(row["distinct_pairs"])) for row in run["workers"]),
        "OVERLAP": worker_overlap(directory / "pairs-map.pods.json"),
        "INPUT_ROWS": integer(final["input_rows"]),
        "SELECTED": integer(final["selected_rows"]),
        "DISTINCT": integer(final["distinct_pairs"]),
        "OCCURRENCES": integer(final["pair_occurrences"]),
        "TOP_ROWS": table_rows(
            (rank, " + ".join(pair).translate(TEX_SPECIAL), integer(count), integer(count))
            for rank, (pair, count) in enumerate(run["comparison"]["top10"], 1)),
        "ALLOCATABLE": capacity["allocatable_m"],
        "RESERVED": capacity["already_requested_m"],
        "CPU": capacity["lab_request"].removesuffix("m"),
        "CPU_EVENT": event_text(directory / "cpu-wait.event.json"),
        "PREEMPT_EVENT": event_text(directory / "cpu-preemption.event.json"),
        "VICTIM": run["preemption"]["victim"],
        "WINNER": run["preemption"]["winner"],
        "QUOTA_HARD": quota["hard"]["requests.cpu"],
        "QUOTA_USED": quota["used"]["requests.cpu"],
        "QUOTA_REQUEST": quota["requested"],
        "QUOTA_EVENT": event_text(directory / "quota-rejected.event.json"),
        "RUN": directory.name,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=Path, required=True, help="Evidence directory containing run.json")
    parser.add_argument("--engine", default="tectonic", help="Tectonic executable")
    parser.add_argument("--output", type=Path, default=ROOT / "report.pdf")
    args = parser.parse_args()
    engine = shutil.which(args.engine)
    if engine is None:
        parser.error("Tectonic executable was not found")
    directory = args.run.resolve()
    run = read_json(directory / "run.json")
    if run.get("status") != "complete" or not run.get("s3_migration"):
        parser.error("the report requires a completed run with an S3-to-S3 migration")
    values = substitutions(directory, run)
    template = (REPORT / "main.tex.in").read_text(encoding="utf-8")
    tex = re.sub(r"@@([A-Z_]+)@@", lambda match: str(values[match.group(1)]), template)
    with TemporaryDirectory() as temporary:
        build = Path(temporary)
        shutil.copytree(REPORT / "fonts", build / "fonts")
        (build / "report.tex").write_text(tex, encoding="utf-8")
        subprocess.run([str(Path(engine).resolve()), "report.tex"], cwd=build, check=True)
        shutil.copyfile(build / "report.pdf", args.output)


if __name__ == "__main__":
    main()
