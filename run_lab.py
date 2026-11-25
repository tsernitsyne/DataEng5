import argparse
import json
import math
import os
import re
import secrets
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from pairs import BUCKET, MIB, RAW_KEY, file_sha256, open_csv, read_counts, s3_client, verify

ROOT = Path(__file__).resolve().parent
K8S = ROOT / "k8s"
REFERENCE = ROOT / "reference"
LOW = "php-pairs-lab-low"
HIGH = "php-pairs-lab-high"
STORAGE = json.loads((REFERENCE / "storage.json").read_text(encoding="utf-8"))
PARTS = 4
TIMEOUT = 900


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def wait_for(description, predicate, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {description}")


def milli_cpu(quantity):
    return int(quantity[:-1]) if quantity.endswith("m") else int(float(quantity) * 1000)


class Lab:
    def __init__(self, args, output):
        self.args = args
        self.out = output
        self.main = output.name
        self.quota = self.main + "-quota"
        self.prefix = f"runs/{self.main}"
        self.facts = {"context": args.context, "namespace": self.main,
                      "quota_namespace": self.quota, "status": "started"}

    def record(self, **facts):
        self.facts.update(facts)
        write_json(self.out / "run.json", self.facts)

    def command(self, *args, stdin=None, check=True, log=None):
        result = subprocess.run(args, cwd=ROOT, input=stdin, capture_output=True,
                                encoding="utf-8", errors="replace", timeout=TIMEOUT)
        if log or result.returncode:
            target = self.out / (log or "command-errors.log")
            with target.open("a", encoding="utf-8") as stream:
                stream.write(f"$ {' '.join(str(arg) for arg in args)}\n")
                stream.write(result.stdout)
                stream.write(result.stderr)
                stream.write(f"\nexit={result.returncode}\n")
        if check and result.returncode:
            raise RuntimeError(f"{args[0]} failed: {result.stderr or result.stdout}")
        return result.stdout

    def k(self, *args, ns=None, **kwargs):
        command = ["kubectl", "--context", self.args.context]
        if ns:
            command.extend(("-n", ns))
        return self.command(*command, *args, **kwargs)

    def obj(self, *args, ns=None):
        return json.loads(self.k("get", *args, "-o", "json", ns=ns))

    def apply(self, resource, ns=None):
        self.k("apply", "-f", "-", ns=ns, stdin=json.dumps(resource))

    def namespace(self, name):
        self.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})

    def submit(self, name, args=(), *, ns=None, cpu="200m", memory="64Mi",
               limit="512Mi", cpu_limit="1", indexed=False, priority=None, node=None):
        ns = ns or self.main
        job = json.loads((K8S / "job.json").read_text(encoding="utf-8"))
        job["metadata"] = {"name": name, "namespace": ns}
        spec = job["spec"]
        pod = spec["template"]["spec"]
        container = pod["containers"][0]
        container.update(image=self.args.image, args=list(args))
        container["resources"] = {"requests": {"cpu": cpu, "memory": memory},
                                  "limits": {"cpu": cpu_limit, "memory": limit}}
        if indexed:
            spec.update(completionMode="Indexed", completions=PARTS, parallelism=PARTS)
        if priority:
            pod["priorityClassName"] = priority
        if node:
            pod["nodeSelector"] = {"kubernetes.io/hostname": node}
        if not args:
            container.pop("env")
            container.pop("envFrom")
            container["command"] = ["python", "-c", "import time; time.sleep(300)"]
        write_json(self.out / f"{name}.manifest.json", job)
        self.apply(job, ns)

    def job_pods(self, name, ns=None):
        return self.obj("pods", "-l", f"job-name={name}", ns=ns or self.main)["items"]

    def save(self, name, ns=None):
        ns = ns or self.main
        job = self.obj("job", name, ns=ns)
        pods = self.job_pods(name, ns)
        write_json(self.out / f"{name}.job.json", job)
        write_json(self.out / f"{name}.pods.json", pods)
        for pod in pods:
            pod_name = pod["metadata"]["name"]
            states = pod["status"].get("containerStatuses", [])
            state = states[0]["state"] if states else {}
            if "running" in state or "terminated" in state:
                content = self.k("logs", pod_name, ns=ns)
                (self.out / f"{pod_name}.log").write_text(content, encoding="utf-8")
        return pods

    def finish(self, name, failed=False):
        def condition():
            conditions = self.obj("job", name, ns=self.main)["status"].get("conditions", [])
            return next((c["type"] for c in conditions
                         if c["status"] == "True" and c["type"] in {"Complete", "Failed"}), None)

        try:
            state = wait_for(name, condition)
        finally:
            pods = self.save(name)
            self.k("get", "events", "--sort-by=.metadata.creationTimestamp",
                   ns=self.main, check=False, log=f"{name}.events.log")
        if state != ("Failed" if failed else "Complete"):
            raise RuntimeError(f"Unexpected state {state} for {name}")
        return pods

    def records(self, pods):
        for pod in pods:
            path = self.out / (pod["metadata"]["name"] + ".log")
            yield json.loads(path.read_text(encoding="utf-8").splitlines()[-1])

    def boot(self):
        print("Checking local Kubernetes and building the workload image", flush=True)
        self.k("version", log="versions.log")
        self.k("get", "nodes", "-o", "wide", log="nodes.log")
        self.command("docker", "version", log="docker-version.log")
        self.command("docker", "build", "-t", self.args.image, ".", log="build.log")
        if self.args.kind_cluster:
            self.command("kind", "load", "docker-image", self.args.image,
                         "--name", self.args.kind_cluster, log="image-load.log")

        self.namespace(self.main)
        credentials = {"AWS_ACCESS_KEY_ID": "lab" + secrets.token_hex(6),
                       "AWS_SECRET_ACCESS_KEY": secrets.token_hex(24)}
        self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "s3-access"},
                    "type": "Opaque", "stringData": credentials}, self.main)
        self.k("apply", "-f", str(K8S / "rustfs.yaml"), ns=self.main)
        try:
            self.k("rollout", "status", "deployment/rustfs", "--timeout=240s", ns=self.main)
        finally:
            self.k("get", "deploy,svc,pvc,pod", "-o", "wide", ns=self.main, log="rustfs.log")
            self.k("get", "events", ns=self.main, check=False, log="rustfs.events.log")
        return credentials

    def data(self, credentials):
        print("Migrating data into RustFS and checking the input object", flush=True)
        with self.forwarded_s3(credentials) as s3:
            if self.args.source_endpoint:
                self.copy_source_bucket(s3)
            if self.args.csv:
                digest = file_sha256(self.args.csv)
                if digest != STORAGE["raw_sha256"]:
                    raise RuntimeError(f"Input CSV SHA-256 differs from HW3: {digest}")
                self.ensure_bucket(s3, BUCKET)
                s3.upload_file(str(self.args.csv), BUCKET, RAW_KEY)
                self.record(source_csv_sha256=digest)

        self.submit("s3-identity", ["identity"], limit="256Mi")
        identity, = self.records(self.finish("s3-identity"))
        if (identity["bytes"], identity["sha256"]) != (STORAGE["raw_bytes"], STORAGE["raw_sha256"]):
            raise RuntimeError("S3 object does not match the original HW3 input")
        self.record(s3_identity=identity)

    def ensure_bucket(self, target, name):
        existing = target.list_buckets()["Buckets"]
        if name not in {bucket["Name"] for bucket in existing}:
            target.create_bucket(Bucket=name)

    def copy_source_bucket(self, target):
        credentials = {}
        if self.args.source_env:
            variables = dict(line.strip().split("=", 1)
                             for line in self.args.source_env.read_text(encoding="utf-8-sig").splitlines()
                             if "=" in line and not line.lstrip().startswith("#"))
            credentials = {
                "aws_access_key_id": variables["AWS_ACCESS_KEY_ID"].strip().strip("\"'"),
                "aws_secret_access_key": variables["AWS_SECRET_ACCESS_KEY"].strip().strip("\"'"),
            }
        source = s3_client(self.args.source_endpoint, **credentials)
        bucket = self.args.source_bucket
        self.ensure_bucket(target, bucket)
        transferred = []
        with TemporaryDirectory() as directory:
            local = Path(directory) / "object"
            for page in source.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    source.download_file(bucket, key, str(local))
                    target.upload_file(str(local), bucket, key)
                    size = target.head_object(Bucket=bucket, Key=key)["ContentLength"]
                    if size != item["Size"]:
                        raise RuntimeError(f"S3 object size differs after copy: {key}")
                    transferred.append({"key": key, "bytes": size})
        self.record(s3_migration={"bucket": bucket, "objects": transferred})

    @contextmanager
    def forwarded_s3(self, credentials):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        with (self.out / "port-forward.log").open("a", encoding="utf-8") as log:
            process = subprocess.Popen(
                ["kubectl", "--context", self.args.context, "-n", self.main,
                 "port-forward", "--address", "127.0.0.1", "service/rustfs", f"{port}:9000"],
                stdout=log, stderr=subprocess.STDOUT,
            )
            try:
                def reachable():
                    if process.poll() is not None:
                        raise RuntimeError("RustFS port-forward stopped, see port-forward.log")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=1):
                            return True
                    except OSError:
                        return False

                wait_for("the RustFS port-forward", reachable, timeout=45)
                yield s3_client(f"http://127.0.0.1:{port}",
                                aws_access_key_id=credentials["AWS_ACCESS_KEY_ID"],
                                aws_secret_access_key=credentials["AWS_SECRET_ACCESS_KEY"])
            finally:
                process.terminate()
                process.wait(timeout=10)

    def memory(self):
        print("Running the OOM trial, measuring the peak and repeating with a derived limit", flush=True)
        args = ["count", "--prefix", self.prefix]
        self.submit("pairs-oom", args, memory="24Mi", limit="24Mi")
        victims = self.finish("pairs-oom", failed=True)
        states = [container["state"].get("terminated", {}) for pod in victims
                  for container in pod["status"].get("containerStatuses", [])]
        death = next((state for state in states if state.get("reason") == "OOMKilled"), None)
        if not death:
            raise RuntimeError("The failed Job did not report OOMKilled")
        self.record(oom={"limit_mib": 24, "reason": death["reason"], "exit_code": death["exitCode"]})

        self.submit("pairs-probe", args)
        probe, = self.records(self.finish("pairs-probe"))
        if int(probe["cgroup_limit"]) != 512 * MIB:
            raise RuntimeError("Peak probe did not read the expected container cgroup")
        chosen = max(32, 16 * math.ceil(probe["peak_bytes"] * 1.25 / (16 * MIB)))
        self.record(memory_probe=probe, selected_limit_mib=chosen)
        self.submit("pairs-single", args, memory=f"{chosen}Mi", limit=f"{chosen}Mi")
        single, = self.records(self.finish("pairs-single"))
        if int(single["cgroup_limit"]) != chosen * MIB:
            raise RuntimeError("Final Job did not read the expected container cgroup")
        self.record(memory_final=single)

    def parallel(self, credentials):
        print("Splitting the CSV, running four indexed workers and reducing their results", flush=True)
        args = ["--prefix", self.prefix]
        self.submit("pairs-split", ["split", *args, "--parts", str(PARTS)], limit="768Mi")
        split, = self.records(self.finish("pairs-split"))
        self.record(split=split)
        self.submit("pairs-map", ["map", *args], limit="256Mi", indexed=True)
        workers = sorted(self.records(self.finish("pairs-map")), key=lambda row: row["index"])
        if [row["index"] for row in workers] != list(range(PARTS)):
            raise RuntimeError("Missing or repeated indexed results")
        if [row["input_rows"] for row in workers] != split["rows_per_part"]:
            raise RuntimeError("Worker and splitter row counts disagree")
        if sum(row["selected_rows"] for row in workers) != self.facts["memory_final"]["selected_rows"]:
            raise RuntimeError("Worker and single-run PHP row counts disagree")
        self.record(workers=workers)
        self.submit("pairs-reduce", ["reduce", *args, "--parts", str(PARTS)], limit="256Mi")
        reducer, = self.records(self.finish("pairs-reduce"))
        self.record(reducer=reducer)
        with self.forwarded_s3(credentials) as s3:
            for name in ("single.csv", "pairs.csv"):
                s3.download_file(BUCKET, f"{self.prefix}/{name}", str(self.out / name))
        with open_csv(REFERENCE / "skills_2.csv") as rows:
            expected = read_counts(rows)
        for name in ("single.csv", "pairs.csv"):
            with open_csv(self.out / name) as rows:
                comparison = verify(read_counts(rows), expected)
        self.record(comparison=comparison)

    def node_cpu(self):
        nodes = self.obj("nodes")["items"]
        node = next((item for item in nodes
                     if item["metadata"]["labels"].get("kubernetes.io/os") == "linux"
                     and not item["spec"].get("unschedulable")
                     and any(c["type"] == "Ready" and c["status"] == "True"
                             for c in item["status"]["conditions"])), None)
        if node is None:
            raise RuntimeError("No Ready, schedulable Linux node")
        name = node["metadata"]["name"]
        host = node["metadata"]["labels"]["kubernetes.io/hostname"]
        allocatable = milli_cpu(node["status"]["allocatable"]["cpu"])
        pods = self.obj("pods", "--all-namespaces", "--field-selector", f"spec.nodeName={name}")
        for pod in pods["items"]:
            if pod["status"]["phase"] not in {"Succeeded", "Failed"} and pod["spec"].get("priority", 0) < -1000:
                raise RuntimeError("The chosen node has an unrelated Pod that might be preempted")
        description = self.k("describe", "node", name, log="node-reservations.log")
        match = re.search(r"^\s+cpu\s+(\S+)\s+\(\d+%\)", description, re.MULTILINE)
        if not match:
            raise RuntimeError("Could not read allocated CPU requests from kubectl describe node")
        allocated = milli_cpu(match.group(1))
        free = allocatable - allocated
        if free < 300:
            raise RuntimeError(f"Only {free}m CPU is free for the scheduling experiment")
        request = f"{free * 2 // 3}m"
        self.record(scheduler_capacity={"node": name, "allocatable_m": allocatable,
                                        "already_requested_m": allocated, "lab_request": request})
        return host, request

    def pod_state(self, job, phase, ns=None):
        return next((pod for pod in self.job_pods(job, ns) if pod["status"]["phase"] == phase), None)

    def event(self, uid, reason, phrase, ns):
        items = self.obj("events", "--field-selector", f"involvedObject.uid={uid}", ns=ns)["items"]
        return next((event for event in items if event["reason"] == reason
                     and phrase.lower() in event["message"].lower()), None)

    def scheduler(self):
        print("Checking Pending due to CPU and priority preemption", flush=True)
        self.k("apply", "-f", str(K8S / "priorities.yaml"))
        host, cpu = self.node_cpu()
        resources = dict(cpu=cpu, cpu_limit=cpu, memory="32Mi", limit="64Mi", node=host)
        submitted = []
        try:
            self.submit("cpu-low", priority=LOW, **resources)
            submitted.append("cpu-low")
            low = wait_for("the low-priority Pod to run",
                           lambda: self.pod_state("cpu-low", "Running"), timeout=90)
            self.save("cpu-low")

            self.submit("cpu-wait", priority=LOW, **resources)
            submitted.append("cpu-wait")
            pending = wait_for("the second Pod to become Pending",
                               lambda: self.pod_state("cpu-wait", "Pending"), timeout=60)
            event = wait_for("Insufficient cpu", lambda: self.event(
                pending["metadata"]["uid"], "FailedScheduling", "Insufficient cpu", self.main), timeout=90)
            self.save("cpu-wait")
            write_json(self.out / "cpu-wait.event.json", event)
            self.record(cpu_pending={"pod": pending["metadata"]["name"], "phase": "Pending",
                                     "message": event["message"]})
            self.k("delete", "job", "cpu-wait", ns=self.main)
            submitted.remove("cpu-wait")

            self.submit("cpu-high", priority=HIGH, **resources)
            submitted.append("cpu-high")
            event = wait_for("the scheduler to preempt the low-priority Pod", lambda: self.event(
                low["metadata"]["uid"], "Preempted", "", self.main), timeout=90)
            write_json(self.out / "cpu-preemption.event.json", event)
            high = wait_for("the high-priority Pod to run",
                            lambda: self.pod_state("cpu-high", "Running"), timeout=90)
            self.save("cpu-high")
            self.record(preemption={"victim": low["metadata"]["name"], "winner": high["metadata"]["name"],
                                    "message": event["message"]})
        finally:
            self.k("get", "events", "--sort-by=.metadata.creationTimestamp", ns=self.main,
                   check=False, log="scheduler.events.log")
            if submitted:
                self.k("delete", "job", *submitted, "--ignore-not-found", ns=self.main,
                       check=False, log="scheduler.cleanup.log")

    def resource_quota(self):
        print("Checking namespace ResourceQuota admission", flush=True)
        self.namespace(self.quota)
        self.k("apply", "-f", str(K8S / "quota.yaml"), ns=self.quota)
        wait_for("the quota to become active", lambda: self.obj(
            "quota", "cpu-budget", ns=self.quota)["status"].get("hard"), timeout=40)
        self.submit("quota-admitted", ns=self.quota, cpu="400m", cpu_limit="400m")
        try:
            accepted = wait_for("the 400m Pod to run", lambda: self.pod_state(
                "quota-admitted", "Running", self.quota), timeout=90)
            self.save("quota-admitted", self.quota)
            self.submit("quota-rejected", ns=self.quota, cpu="200m", cpu_limit="200m")
            job = self.obj("job", "quota-rejected", ns=self.quota)
            event = wait_for("the 200m Pod to be rejected at admission", lambda: self.event(
                job["metadata"]["uid"], "FailedCreate", "exceeded quota", self.quota), timeout=90)
            if self.save("quota-rejected", self.quota):
                raise RuntimeError("Quota-rejected Job unexpectedly has a Pod")
            quota = self.obj("quota", "cpu-budget", ns=self.quota)
            write_json(self.out / "quota.status.json", quota)
            write_json(self.out / "quota-rejected.event.json", event)
            self.record(quota={"admitted": accepted["metadata"]["name"], "requested": "200m",
                               "hard": quota["status"]["hard"], "used": quota["status"]["used"],
                               "message": event["message"]})
        finally:
            self.k("get", "events", ns=self.quota, check=False, log="quota.events.log")
            self.k("delete", "job", "quota-admitted", "quota-rejected", "--ignore-not-found",
                   ns=self.quota, check=False, log="quota.cleanup.log")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path)
    parser.add_argument("--source-endpoint")
    parser.add_argument("--source-env", type=Path)
    parser.add_argument("--source-bucket", default=BUCKET)
    parser.add_argument("--context", default="docker-desktop")
    parser.add_argument("--kind-cluster")
    parser.add_argument("--image", default="php-pairs-hw5:local")
    args = parser.parse_args()
    if not args.csv and not args.source_endpoint:
        parser.error("provide --csv or --source-endpoint")
    if args.csv and not args.csv.is_file():
        parser.error(f"file does not exist: {args.csv}")
    run = datetime.now(timezone.utc).strftime("php-pairs-%Y%m%d-%H%M%S-") + secrets.token_hex(2)
    output = ROOT / "logs" / run
    output.mkdir(parents=True)
    print(f"Run evidence: {output}", flush=True)
    lab = Lab(args, output)
    try:
        credentials = lab.boot()
        lab.data(credentials)
        lab.memory()
        lab.parallel(credentials)
        lab.scheduler()
        lab.resource_quota()
    except Exception as error:
        lab.record(status="failed", error=str(error))
        raise
    lab.record(status="complete")
    print("All experiments verified.", flush=True)


if __name__ == "__main__":
    main()
