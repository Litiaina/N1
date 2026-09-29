#!/usr/bin/env python3
"""Isolated two-process EC 1+1 export proof (one host, NOT hardware qualification).

Uses only loopback, new temporary directories and child PIDs. Never installs a
service, reads deployment credentials, or removes existing data. Requires built
engine/gateway binaries and openssl. Evidence and logs are retained in --out.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import zipfile


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--gateway", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    engine, gateway = args.engine.resolve(strict=True), args.gateway.resolve(strict=True)
    args.out.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="n2-export-proof-", dir=args.out.resolve()))
    print(f"Evidence: {root}", flush=True)
    processes, logs = {}, []
    context = ssl._create_unverified_context()  # self-signed, loopback-only fixture
    token = ""
    secret = secrets.token_hex(32)
    ports = []
    while len(ports) < 5:
        port = free_port()
        if port not in ports:
            ports.append(port)
    bases = [f"https://127.0.0.1:{ports[0]}", f"https://127.0.0.1:{ports[2]}"]
    gate = f"https://127.0.0.1:{ports[4]}"

    def request(base, path, method="GET", data=None, headers=None, expected=(200, 201, 202, 204), auth=True):
        request_headers = dict(headers or {})
        if token and auth:
            request_headers["Authorization"] = f"Bearer {token}"
        if isinstance(data, (dict, list)):
            data = json.dumps(data).encode()
            request_headers["Content-Type"] = "application/json"
        req = urllib.request.Request(base + path, data=data, headers=request_headers, method=method)
        try:
            response = urllib.request.urlopen(req, context=context, timeout=120)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            assert response.status in expected, (method, path.split('?')[0], response.status, body[:500])
            return response.status, dict(response.headers), body

    def api(base, path, method="GET", data=None, **kwargs):
        body = request(base, path, method, data, **kwargs)[2]
        return json.loads(body) if body else None

    def start(name, binary, cwd):
        log = open(root / f"{name}.log", "ab")
        logs.append(log)
        env = dict(os.environ, N2_RUNTIME_WORKER_THREADS="4", N2_RUNTIME_MAX_BLOCKING_THREADS="16", RUST_LOG="info")
        processes[name] = subprocess.Popen([str(binary)], cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT)

    def stop(name):
        process = processes.pop(name, None)
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    def wait_up(base, path="/noa/v1/ping"):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            try:
                request(base, path, auth=False)
                return
            except (AssertionError, OSError, ValueError):
                time.sleep(0.2)
        raise AssertionError(f"fixture did not start: {base}; inspect logs")

    def create(payload, base=None):
        return api(base or bases[0], "/noa/v1/exports", "POST", payload)["export_id"]

    def status(export_id, base=None):
        return api(base or gate, f"/noa/v1/exports/{export_id}")

    def wait_phase(export_id, wanted, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            current = status(export_id)
            if current["phase"] in wanted:
                return current
            assert current["phase"] not in ("failed", "cancelled"), current
            time.sleep(0.025)
        raise AssertionError(f"export phase timeout: {export_id}")

    def download(export_id):
        signed = api(gate, f"/noa/v1/exports/{export_id}/signed-url", "POST", {})
        return signed["path"], request(gate, signed["path"], auth=False)

    def check_archive(data, kind, expected):
        if kind == "zip":
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                assert set(archive.namelist()) == set(expected)
                for name, content in expected.items():
                    assert archive.read(name) == content, name
        else:
            with tarfile.open(fileobj=io.BytesIO(data)) as archive:
                assert set(archive.getnames()) == set(expected)
                for name, content in expected.items():
                    assert archive.extractfile(name).read() == content, name

    results = []
    try:
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", str(root / "key.pem"), "-out", str(root / "cert.pem"),
                        "-days", "1", "-subj", "/CN=localhost"], check=True, capture_output=True)
        nodes = []
        for i, name in enumerate(("node-a", "node-b")):
            cwd = root / name
            (cwd / "state").mkdir(parents=True)
            (cwd / "disk").mkdir()
            nodes.append(dict(slot_id=name, instance_id=name, binding_epoch=1, endpoint=bases[i],
                              native_endpoint=f"n2://127.0.0.1:{ports[i*2+1]}", state="online", weight=1,
                              storage_roots=[dict(id=f"root-{i}", path=str(cwd / "disk"))], joined_epoch=1))
            (cwd / "n2.env").write_text(f"JWT_SECRET={secret}\nMASTER_KEY={secret}\nAUTH_KEY={secret}\nADMIN_KEY={secret}\nN2_CLUSTER_SECRET={secret}\n")
            (cwd / "n2.env").chmod(0o600)
            (cwd / "n2_config.ini").write_text(f"""[server]
host=127.0.0.1
https_port={ports[i*2]}
native_port={ports[i*2+1]}
cert_path={root}/cert.pem
key_path={root}/key.pem
[disk_config]
storage_roots=root-{i}={cwd}/disk
runtime_path={cwd}/state
[cluster_config]
node_id={name}
pool_id=proof
pool_generation=1
node_endpoint={bases[i]}
native_endpoint=n2://127.0.0.1:{ports[i*2+1]}
membership_file={cwd}/state/n2_membership.json
object_data_shards=1
object_parity_shards=1
auto_rebalance=false
[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=1
retention_seconds=2
[auto_healing]
enabled=false
[reserve_set]
enabled=false
automatic_promotion=false
[export]
retention_seconds=120
cleanup_interval_seconds=5
max_concurrent_jobs=1
artifact_chunk_size_bytes=1048576
object_read_concurrency=1
artifact_write_concurrency=1
max_buffered_bytes_per_job=16777216
max_buffered_bytes_global=33554432
max_retained_bytes=1073741824
[set_limits]
max_file_size=1073741824
[jwt_token_config]
login_token_expiration=3600
[rate_limiter_config]
enable_rate_limiting=false
auth_limit_per_second=100
auth_limit_burst_size=200
view_limit_per_second=1000
view_limit_burst_size=2000
upload_limit_per_second=1000
upload_limit_burst_size=4000
global_limit_per_second=2000
global_limit_burst_size=4000
[upload_config]
max_concurrent_io=16
adaptive_concurrency_enabled=false
writer_buffer_size=1048576
upload_memory_budget_bytes=67108864
monitor_interval_seconds=3600
monitor_deep_scan_interval_seconds=3600
max_part_size=33554432
data_shards=1
parity_shards=1
stripe_block_size=262144
""")
        membership = dict(format_version=5, cluster_id="isolated-export-proof", epoch=1, binding_epoch=1,
                          pools=[dict(id="proof", generation=1, policy=dict(object_data_shards=1, object_parity_shards=1),
                                      state="writable", slot_ids=["node-a", "node-b"])], slot_bindings=nodes,
                          reserve_nodes=[], retired_instances=[])
        for name in ("node-a", "node-b"):
            (root / name / "state/n2_membership.json").write_text(json.dumps(membership))
            start(name, engine, root / name)
        for base in bases:
            wait_up(base)
        (root / "gateway").mkdir()
        (root / "gateway/n2_gateway_config.ini").write_text(f"""[server]
host=127.0.0.1
https_port={ports[4]}
cert_path={root}/cert.pem
key_path={root}/key.pem
[routing]
health_interval_seconds=1
request_timeout_seconds=15
[node.node-a]
http_url={bases[0]}
native_url=n2://127.0.0.1:{ports[1]}
[node.node-b]
http_url={bases[1]}
native_url=n2://127.0.0.1:{ports[3]}
""")
        start("gateway", gateway, root / "gateway")
        wait_up(gate, "/gateway/health")
        fragment = api(bases[0], "/noa/v1/fragments", "POST", dict(name="export-proof", secret=secret, key=secret))
        token = api(bases[0], "/noa/v1/auth/fragment", "POST", dict(fragment=fragment["fragment_hash"], secret=secret,
                    read=True, write=True, modify=True, delete=True, expires_in=3600))["access_token"]
        source = bytes(range(256)) * 4096
        api(bases[1], "/noa/v1/upload/one-shot/docs/source.bin", "POST", source)
        api(bases[1], "/noa/v1/upload/one-shot/docs/note.txt", "POST", b"export proof\n")
        expected = {"docs/source.bin": source, "docs/note.txt": b"export proof\n"}
        for kind in ("zip", "tar"):
            job = create(dict(prefixes=["docs"], format=kind, filename="proof"))
            ready = wait_phase(job, {"ready"})
            signed_path, (_, headers, data) = download(job)
            check_archive(data, kind, expected)
            etag = '"' + hashlib.sha256(data).hexdigest() + '"'
            assert headers["etag"] == etag == ready["etag"]
            assert int(headers["content-length"]) == len(data)
            offset = len(data) // 2
            _, ranged_headers, suffix = request(gate, signed_path, headers={"Range": f"bytes={offset}-", "If-Range": etag}, expected=(206,), auth=False)
            assert data[:offset] + suffix == data
            assert ranged_headers["content-range"] == f"bytes {offset}-{len(data)-1}/{len(data)}"
            request(gate, signed_path, headers={"Range": f"bytes={len(data)}-"}, expected=(416,), auth=False)
            request(gate, signed_path.replace("signature=", "signature=bad"), expected=(401,), auth=False)
            results.append(f"{kind}: complete contents, SHA-256 ETag, range resume, 416, signed authority")
        expiring_job, expires_at = job, ready["expires_at_unix"]
        other_fragment = api(bases[0], "/noa/v1/fragments", "POST", dict(name="other-export-proof", secret=secret, key=secret))
        other_token = api(bases[0], "/noa/v1/auth/fragment", "POST", dict(fragment=other_fragment["fragment_hash"], secret=secret,
                          read=True, write=False, modify=False, delete=False, expires_in=3600))["access_token"]
        for suffix, method in (("", "GET"), ("/download", "GET"), ("/signed-url", "POST"), ("", "DELETE")):
            request(gate, f"/noa/v1/exports/{job}{suffix}", method, {} if method == "POST" else None,
                    headers={"Authorization": f"Bearer {other_token}"}, expected=(403,), auth=False)
        results.append("fragment isolation for status, download, signing and cancellation")
        bad = create(dict(keys=["docs/note.txt", "missing.txt"], format="zip"))
        assert wait_phase(bad, {"failed"})["phase"] == "failed"
        request(gate, f"/noa/v1/exports/{bad}/download", expected=(409,))
        results.append("missing selected object fails whole export; partial artifact unavailable")

        # Mapped duplicates create a slow enough build to stop after capture.
        entries = [dict(object_key="docs/source.bin", archive_path=f"copy-{i:03}.bin") for i in range(64)]
        interrupted = create(dict(entries=entries, format="zip"))
        wait_phase(interrupted, {"building"})
        manifest = root / "node-a/state/.n2-exports" / interrupted / "manifest.ndjson"
        captured = hashlib.sha256(manifest.read_bytes()).hexdigest()
        stop("node-a")
        # Requests must not fall back to the surviving node's unrelated state.
        request(gate, f"/noa/v1/exports/{interrupted}", expected=(502, 503))
        assert (root / "node-b/state/.n2-export-pins" / f"{interrupted}.json").exists()
        # Holder restart must not forget the pin; even an authenticated purge
        # arriving while the coordinator is offline must fail closed.
        stop("node-b")
        start("node-b", engine, root / "node-b")
        wait_up(bases[1])
        first_entry = json.loads(manifest.read_text().splitlines()[0])
        request(bases[1], "/n2/peer/lifecycle/purge-object", "POST",
                dict(fragment=fragment["stable_fragment_id"], object_id=first_entry["object_id"], lifecycle_mutation_id="export-proof-must-not-purge"),
                headers={"x-n2-cluster-secret": secret}, expected=(409,), auth=False)
        results.append("durable source pin blocks holder purge after coordinator loss and holder restart")
        start("node-a", engine, root / "node-a")
        wait_up(bases[0])
        ready = wait_phase(interrupted, {"ready"})
        assert captured == hashlib.sha256(manifest.read_bytes()).hexdigest()
        _, (_, _, data) = download(interrupted)
        check_archive(data, "zip", {f"copy-{i:03}.bin": source for i in range(64)})
        results.append("owner SIGKILL during build, durable manifest reuse, no gateway takeover, exact recovered ZIP64")

        ranged = create(dict(entries=[dict(object_key="docs/source.bin", archive_path="slice.bin", offset_bytes=10, length_bytes=8192)], format="tar"))
        wait_phase(ranged, {"ready"})
        check_archive(download(ranged)[1][2], "tar", {"slice.bin": source[10:8202]})
        results.append("mapped immutable source byte range")
        cancelled = create(dict(entries=entries, format="tar"))
        wait_phase(cancelled, {"building"})
        api(gate, f"/noa/v1/exports/{cancelled}", "DELETE")
        request(gate, f"/noa/v1/exports/{cancelled}/download", expected=(404, 409))
        stop("node-a")
        cancelled_state = root / "node-a/state/.n2-exports" / cancelled / "state.json"
        if cancelled_state.exists():
            assert json.loads(cancelled_state.read_text())["phase"] == "cancelled"
        start("node-a", engine, root / "node-a")
        wait_up(bases[0])
        request(gate, f"/noa/v1/exports/{cancelled}/download", expected=(404, 409))
        results.append("acknowledged cancellation survives owner restart and prevents artifact delivery")
        print("Crash/integrity checks passed; waiting for natural artifact expiry...", flush=True)
        deadline = time.monotonic() + 135
        job_dir = root / "node-a/state/.n2-exports" / expiring_job
        while job_dir.exists() and time.monotonic() < deadline:
            time.sleep(0.25)
        assert not job_dir.exists(), "retention cleanup did not remove expired state"
        request(gate, f"/noa/v1/exports/{expiring_job}", expected=(404,))
        for name in ("node-a", "node-b"):
            assert not (root / name / "disk/.n2-export-artifacts" / expiring_job).exists()
        assert request(bases[0], "/noa/v1/objects/download?object_key=docs%2Fnote.txt")[2] == expected["docs/note.txt"]
        results.append("natural retention removes state/artifact chunks without touching source objects")
        (root / "summary.json").write_text(json.dumps(dict(result="PASS", topology="one host, two processes, EC 1+1; not hardware qualification", checks=results), indent=2))
        print(json.dumps(dict(result="PASS", checks=results), indent=2), flush=True)
    finally:
        for name in list(processes):
            stop(name)
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()
