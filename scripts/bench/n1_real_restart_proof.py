#!/usr/bin/env python3
import argparse
import concurrent.futures as futures
import hashlib
import json
import os
import signal
import sqlite3
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a real N1 HTTP upload/restart/indexdb proof on real disk roots."
    )
    parser.add_argument("--binary", required=True, help="Path to litiaina-n1 binary")
    parser.add_argument("--source-dir", required=True, help="Directory to upload")
    parser.add_argument(
        "--disk-roots",
        required=True,
        help="Comma-separated physical disk roots, for example /mnt/disk-00,/mnt/disk-01",
    )
    parser.add_argument("--out-dir", required=True, help="Proof output directory")
    parser.add_argument("--port", type=int, default=50491)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--prefix", default="proof/")
    parser.add_argument("--quota-bytes", type=int, default=10 * 1024 * 1024 * 1024)
    parser.add_argument("--quota-backend", default="xfs_project")
    parser.add_argument("--master-key", default="real-proof-master-key")
    parser.add_argument("--fragment-key", default="real-proof-fragment-key")
    parser.add_argument("--fragment-name", default="")
    parser.add_argument("--cleanup-roots", action="store_true")
    return parser.parse_args()


class ProofFailure(RuntimeError):
    pass


class N1Proof:
    def __init__(self, args):
        self.args = args
        self.binary = Path(args.binary).resolve()
        self.source_dir = Path(args.source_dir).resolve()
        self.out_dir = Path(args.out_dir).resolve()
        self.disk_roots = [
            Path(item).resolve()
            for item in args.disk_roots.split(",")
            if item.strip()
        ]
        self.stamp = time.strftime("%Y%m%d-%H%M%S")
        self.base_url = f"https://127.0.0.1:{args.port}"
        self.ctx = ssl._create_unverified_context()
        self.server = None
        self.fragment = None
        self.token = None
        self.index_roots = []
        self.blob_roots = []
        self.server_log = None
        self.max_server_rss_kib = 0
        self.summary = {
            "base_url": self.base_url,
            "source_dir": str(self.source_dir),
            "disk_roots": [str(path) for path in self.disk_roots],
            "started_at_unix": int(time.time()),
        }

    def validate(self):
        if not self.binary.exists() or not os.access(self.binary, os.X_OK):
            raise ProofFailure(f"binary is not executable: {self.binary}")
        if not self.source_dir.is_dir():
            raise ProofFailure(f"source dir does not exist: {self.source_dir}")
        if len(self.disk_roots) < 2:
            raise ProofFailure("real restart proof requires at least two disk roots")
        for root in self.disk_roots:
            if not root.is_dir():
                raise ProofFailure(f"disk root does not exist: {root}")
        if self.args.quota_backend == "xfs_project" and hasattr(os, "geteuid"):
            if os.geteuid() != 0:
                raise ProofFailure("xfs_project proof must run as root or through sudo")
        if not self.args.prefix.endswith("/"):
            self.args.prefix += "/"

    def prepare_dirs(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        proof_root_name = f"n1-real-restart-proof-{self.stamp}"
        for disk in self.disk_roots:
            root = disk / proof_root_name
            index_root = root / "index"
            blob_root = root / "blob"
            index_root.mkdir(parents=True, exist_ok=True)
            blob_root.mkdir(parents=True, exist_ok=True)
            self.index_roots.append(index_root)
            self.blob_roots.append(blob_root)
        (self.out_dir / "fs").mkdir(parents=True, exist_ok=True)
        (self.out_dir / "refresh").mkdir(parents=True, exist_ok=True)

    def run_cmd(self, argv, **kwargs):
        return subprocess.run(argv, check=True, text=True, **kwargs)

    def write_config(self):
        cert = self.out_dir / "cert.pem"
        key = self.out_dir / "key.pem"
        self.run_cmd(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-sha256",
                "-days",
                "1",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-subj",
                "/CN=127.0.0.1",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        (self.out_dir / "n1.env").write_text(
            "\n".join(
                [
                    "JWT_SECRET=real-proof-jwt-secret",
                    f"MASTER_KEY={self.args.master_key}",
                    f"AUTH_KEY={self.args.master_key}",
                    f"ADMIN_KEY={self.args.master_key}",
                    "ACCESS_TOKEN_DEFAULT_TTL_SECS=3600",
                    "ACCESS_TOKEN_MAX_TTL_SECS=7200",
                    "REFRESH_TOKEN_TTL_SECS=604800",
                    "",
                ]
            )
        )
        config = f"""[server]
host=127.0.0.1
https_port={self.args.port}
cert_path={cert}
key_path={key}

[disk_config]
index_store_disks={','.join(str(path) for path in self.index_roots)}
blob_store_disks={','.join(str(path) for path in self.blob_roots)}
fs_storage_path={self.out_dir / 'fs'}

[quota_config]
fragment_quota_backend={self.args.quota_backend}

[index_config]
partition_count=64
per_db_cache_bytes=auto
max_cached_dbs=auto

[namespace_config]
max_loaded_facades=auto
max_loaded_dbs=auto

[garbage_collector]
cleanup_interval_seconds=86400
file_ttl_seconds=3600
retention_seconds=2592000

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=900

[rate_limiter_config]
enable_rate_limiting=false
auth_limit_per_second=5000
auth_limit_burst_size=10000
view_limit_per_second=5000
view_limit_burst_size=10000
upload_limit_per_second=5000
upload_limit_burst_size=10000
global_limit_per_second=5000
global_limit_burst_size=10000

[upload_config]
max_concurrent_io=auto
adaptive_concurrency_enabled=true
adaptive_concurrency_interval_ms=auto
writer_buffer_size=auto
upload_memory_budget_bytes=auto
upload_part_strict_durability_default=false
upload_io_gate_enabled=false
stream_max_concurrency=auto
stream_chunk_size=auto
stream_readahead_bytes=auto
stream_locality_bias_enabled=true
monitor_interval_seconds=auto
monitor_deep_scan_interval_seconds=auto
max_part_size=52428800
data_shards=1
parity_shards=1
stripe_block_size=262144
"""
        (self.out_dir / "n1_config.ini").write_text(config)

    def start_server(self, suffix):
        log_path = self.out_dir / f"server-{suffix}.log"
        env = os.environ.copy()
        env.update(
            {
                "RUST_LOG": env.get("RUST_LOG", "info"),
                "RUST_BACKTRACE": "1",
                "N1_REFRESH_STORE_ROOT": str(self.out_dir / "refresh"),
            }
        )
        log = log_path.open("wb")
        self.server_log = log
        self.server = subprocess.Popen(
            [str(self.binary)],
            cwd=str(self.out_dir),
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        for _ in range(120):
            if self.server.poll() is not None:
                raise ProofFailure(
                    f"N1 server exited during startup; see {log_path}"
                )
            try:
                status, body = self.http("GET", f"{self.base_url}/n1/ping")
                if status == 200 and body == b"pong":
                    self.summary[f"server_{suffix}_pid"] = self.server.pid
                    self.sample_server_rss()
                    return
            except Exception:
                pass
            time.sleep(0.5)
        raise ProofFailure(f"N1 server did not become ready; see {log_path}")

    def stop_server(self):
        self.sample_server_rss()
        if self.server and self.server.poll() is None:
            self.server.send_signal(signal.SIGINT)
            try:
                self.server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=10)
        if self.server_log:
            self.server_log.close()
        self.server = None
        self.server_log = None

    def sample_server_rss(self):
        if not self.server or self.server.poll() is not None:
            return None
        status_path = Path(f"/proc/{self.server.pid}/status")
        try:
            for line in status_path.read_text().splitlines():
                if line.startswith("VmRSS:"):
                    rss_kib = int(line.split()[1])
                    self.max_server_rss_kib = max(self.max_server_rss_kib, rss_kib)
                    self.summary["max_server_rss_kib"] = self.max_server_rss_kib
                    return rss_kib
        except Exception:
            return None
        return None

    def http(self, method, url, body=None, headers=None, timeout=240):
        headers = dict(headers or {})
        data = body
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            headers.setdefault("Content-Type", "application/json")
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, context=self.ctx, timeout=timeout) as response:
                raw = response.read()
                ctype = response.headers.get("content-type", "")
                if "json" in ctype or raw.startswith(b"{") or raw.startswith(b"["):
                    return response.status, json.loads(raw.decode() or "null")
                return response.status, raw
        except urllib.error.HTTPError as error:
            raw = error.read()
            detail = raw.decode(errors="replace")
            raise ProofFailure(f"{method} {url} -> HTTP {error.code}: {detail}") from None

    def create_fragment(self):
        name = self.args.fragment_name or f"real-restart-proof-{self.stamp}"
        status, body = self.http(
            "POST",
            f"{self.base_url}/n1/fragments",
            {
                "name": name,
                "secret": self.args.master_key,
                "key": self.args.fragment_key,
                "quota_bytes": self.args.quota_bytes,
            },
        )
        if status != 201:
            raise ProofFailure(f"fragment create returned {status}")
        self.fragment = body["fragment_hash"]
        expected = hashlib.sha256(name.encode()).hexdigest()
        if self.fragment != expected:
            raise ProofFailure(f"fragment hash mismatch {self.fragment} != {expected}")
        self.summary.update(
            {
                "fragment_name": name,
                "fragment_hash": self.fragment,
                "quota_bytes": self.args.quota_bytes,
            }
        )

    def auth(self):
        status, body = self.http(
            "POST",
            f"{self.base_url}/n1/auth/fragment",
            {
                "fragment": self.fragment,
                "secret": self.args.fragment_key,
                "read": True,
                "write": True,
                "modify": True,
                "delete": True,
                "expires_in": 3600,
            },
        )
        if status != 200:
            raise ProofFailure(f"fragment auth returned {status}")
        self.token = body["access_token"]

    def bearer(self):
        return {"Authorization": f"Bearer {self.token}"}

    def files_to_upload(self):
        files = [
            path
            for path in self.source_dir.rglob("*")
            if path.is_file() and ".git" not in path.parts
        ]
        files.sort()
        if self.args.max_files > 0:
            files = files[: self.args.max_files]
        if not files:
            raise ProofFailure(f"no files found under source dir {self.source_dir}")
        expected = [
            self.args.prefix + str(path.relative_to(self.source_dir)).replace(os.sep, "/")
            for path in files
        ]
        return files, expected

    def upload_all(self, upload_items):
        headers = self.bearer()
        headers["Content-Type"] = "application/octet-stream"
        start = time.monotonic()
        progress = {"count": 0, "bytes": 0}

        def upload_one(item):
            path, logical = item
            payload = path.read_bytes()
            quoted = urllib.parse.quote(logical, safe="/")
            url = f"{self.base_url}/n1/upload/one-shot/{quoted}"
            last_error = None
            for attempt in range(3):
                try:
                    status, body = self.http(
                        "POST", url, body=payload, headers=headers, timeout=300
                    )
                    if status != 201:
                        raise ProofFailure(f"upload returned {status} for {logical}")
                    if body.get("object_key") != logical:
                        raise ProofFailure(
                            f"upload logical path mismatch for {logical}: {body.get('object_key')}"
                        )
                    return len(payload)
                except Exception as err:
                    last_error = err
                    time.sleep(0.25 * (attempt + 1))
            raise last_error

        print(
            f"REAL_N1_PROOF upload_start files={len(upload_items)} concurrency={self.args.concurrency}",
            flush=True,
        )
        with futures.ThreadPoolExecutor(max_workers=self.args.concurrency) as pool:
            running = [pool.submit(upload_one, item) for item in upload_items]
            for future in futures.as_completed(running):
                size = future.result()
                progress["count"] += 1
                progress["bytes"] += size
                every = max(1, self.args.progress_every)
                if progress["count"] % every == 0 or progress["count"] == len(upload_items):
                    rss_kib = self.sample_server_rss()
                    print(
                        "REAL_N1_PROOF upload_progress "
                        f"{progress['count']}/{len(upload_items)} bytes={progress['bytes']} "
                        f"elapsed_s={time.monotonic() - start:.2f} rss_kib={rss_kib}",
                        flush=True,
                    )
        self.sample_server_rss()
        self.summary["upload_elapsed_s"] = round(time.monotonic() - start, 3)
        self.summary["uploaded_bytes"] = progress["bytes"]

    def list_all(self):
        out = []
        pages = 0
        token = None
        while True:
            params = {"prefix": self.args.prefix, "limit": "1000"}
            if token:
                params["continuation_token"] = token
            url = f"{self.base_url}/n1/objects?{urllib.parse.urlencode(params)}"
            status, page = self.http("GET", url, headers=self.bearer())
            if status != 200:
                raise ProofFailure(f"list returned {status}")
            pages += 1
            out.extend(obj["object_key"] for obj in page.get("objects", []))
            token = page.get("continuation_token")
            if not token:
                break
        return sorted(out), pages

    def assert_listing(self, phase, expected):
        actual, pages = self.list_all()
        actual_hash = hashlib.sha256("\n".join(actual).encode()).hexdigest()
        expected_hash = hashlib.sha256("\n".join(sorted(expected)).encode()).hexdigest()
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        print(
            f"REAL_N1_PROOF {phase}_list_count={len(actual)} expected={len(expected)} "
            f"pages={pages} missing={len(missing)} extra={len(extra)}",
            flush=True,
        )
        if missing or extra or actual_hash != expected_hash:
            raise ProofFailure(
                f"{phase} listing mismatch missing={missing[:10]} extra={extra[:10]}"
            )
        self.summary[f"{phase}_list_count"] = len(actual)
        self.summary[f"{phase}_list_pages"] = pages
        self.summary[f"{phase}_paths_sha256"] = actual_hash
        return actual

    def download_samples(self, actual):
        samples = [actual[0], actual[len(actual) // 2], actual[-1]]
        results = []
        source_by_logical = {
            self.args.prefix
            + str(path.relative_to(self.source_dir)).replace(os.sep, "/"): path
            for path in self.source_dir.rglob("*")
            if path.is_file() and ".git" not in path.parts
        }
        for logical in samples:
            params = urllib.parse.urlencode({"object_key": logical})
            status, payload = self.http(
                "GET",
                f"{self.base_url}/n1/objects/download?{params}",
                headers=self.bearer(),
            )
            if status != 200:
                raise ProofFailure(f"download returned {status} for {logical}")
            expected_hash = hashlib.sha256(source_by_logical[logical].read_bytes()).hexdigest()
            actual_hash = hashlib.sha256(payload).hexdigest()
            ok = actual_hash == expected_hash
            print(
                f"REAL_N1_PROOF download_check path={logical} bytes={len(payload)} sha256_ok={ok}",
                flush=True,
            )
            if not ok:
                raise ProofFailure(f"download hash mismatch for {logical}")
            results.append({"path": logical, "bytes": len(payload), "sha256": actual_hash})
        self.summary["download_checks"] = results

    def inspect_indexdb(self, expected_count):
        inspections = []
        for index_root, blob_root in zip(self.index_roots, self.blob_roots):
            indexdb_dir = index_root / self.fragment / "indexdb"
            shard_paths = sorted(indexdb_dir.glob("shard_*.ndxdb"))
            if not shard_paths:
                raise ProofFailure(f"no sharded indexdb files under {indexdb_dir}")
            blob_path = blob_root / self.fragment / "blob"
            checks = {
                "shard_files": len(shard_paths),
                "integrity_check": "ok",
                "namespace_nodes": 0,
                "namespace_file_nodes": 0,
                "namespace_dir_nodes": 0,
                "object_heads": 0,
                "object_versions": 0,
                "object_parts": 0,
                "part_shards": 0,
                "pending_upload_bindings": 0,
                "upload_sessions": 0,
                "trash_entries": 0,
            }

            def count_or_zero(cur, sql):
                try:
                    cur.execute(sql)
                    return cur.fetchone()[0]
                except sqlite3.OperationalError as err:
                    if "no such table" in str(err).lower():
                        return 0
                    raise

            for shard_path in shard_paths:
                conn = sqlite3.connect(f"file:{shard_path}?mode=ro", uri=True)
                cur = conn.cursor()
                cur.execute("PRAGMA integrity_check")
                integrity = cur.fetchone()[0]
                if integrity != "ok":
                    checks["integrity_check"] = integrity
                for label, sql in [
                    ("namespace_nodes", "SELECT COUNT(*) FROM namespace_nodes"),
                    ("namespace_file_nodes", "SELECT COUNT(*) FROM namespace_nodes WHERE kind='file'"),
                    ("namespace_dir_nodes", "SELECT COUNT(*) FROM namespace_nodes WHERE kind='directory'"),
                    ("object_heads", "SELECT COUNT(*) FROM object_heads"),
                    ("object_versions", "SELECT COUNT(*) FROM object_versions"),
                    ("object_parts", "SELECT COUNT(*) FROM object_parts"),
                    ("part_shards", "SELECT COUNT(*) FROM part_shards"),
                    ("pending_upload_bindings", "SELECT COUNT(*) FROM pending_upload_bindings"),
                    ("upload_sessions", "SELECT COUNT(*) FROM upload_sessions"),
                    ("trash_entries", "SELECT COUNT(*) FROM trash_entries"),
                ]:
                    checks[label] += count_or_zero(cur, sql)
                conn.close()
            blob_version_dirs = (
                sum(1 for path in blob_path.iterdir() if path.is_dir())
                if blob_path.exists()
                else 0
            )
            checks["blob_version_dirs"] = blob_version_dirs
            print(
                "REAL_N1_PROOF indexdb_inspect "
                f"root={index_root} integrity={checks['integrity_check']} "
                f"shard_files={checks['shard_files']} "
                f"namespace_file_nodes={checks['namespace_file_nodes']} "
                f"object_heads={checks['object_heads']} "
                f"object_versions={checks['object_versions']} "
                f"part_shards={checks['part_shards']} "
                f"blob_version_dirs={blob_version_dirs}",
                flush=True,
            )
            if checks["integrity_check"] != "ok":
                raise ProofFailure(f"indexdb integrity failed under {indexdb_dir}")
            for label in ["namespace_file_nodes", "object_heads", "object_versions", "object_parts"]:
                if checks[label] != expected_count:
                    raise ProofFailure(
                        f"{label}={checks[label]} expected={expected_count} under {indexdb_dir}"
                    )
            if checks["part_shards"] != expected_count * 2:
                raise ProofFailure(
                    f"part_shards={checks['part_shards']} expected={expected_count * 2} under {indexdb_dir}"
                )
            if blob_version_dirs != expected_count:
                raise ProofFailure(
                    f"blob_version_dirs={blob_version_dirs} expected={expected_count} under {blob_path}"
                )
            inspections.append(
                {
                    "index_root": str(index_root),
                    "blob_root": str(blob_root),
                    "indexdb_dir": str(indexdb_dir),
                    **checks,
                }
            )
        self.summary["indexdb_inspection"] = inspections

    def write_summary(self, status):
        self.summary["status"] = status
        self.summary["finished_at_unix"] = int(time.time())
        path = self.out_dir / "summary.json"
        path.write_text(json.dumps(self.summary, indent=2, sort_keys=True))
        print(f"REAL_N1_PROOF summary={path}", flush=True)

    def cleanup_roots(self):
        for root in self.index_roots + self.blob_roots:
            proof_parent = root.parent
            if proof_parent.name.startswith("n1-real-restart-proof-") and proof_parent.exists():
                for child in proof_parent.iterdir():
                    if child.is_dir():
                        for nested in child.iterdir():
                            if nested.is_dir():
                                subprocess.run(["rm", "-rf", str(nested)], check=False)
                subprocess.run(["rm", "-rf", str(proof_parent)], check=False)

    def run(self):
        self.validate()
        self.prepare_dirs()
        self.write_config()
        files, expected = self.files_to_upload()
        upload_items = list(zip(files, expected))
        expected_sorted = sorted(expected)
        self.summary.update(
            {
                "file_count": len(files),
                "source_bytes": sum(path.stat().st_size for path in files),
                "expected_paths_sha256": hashlib.sha256(
                    "\n".join(expected_sorted).encode()
                ).hexdigest(),
                "index_roots": [str(path) for path in self.index_roots],
                "blob_roots": [str(path) for path in self.blob_roots],
            }
        )
        try:
            self.start_server("before")
            self.create_fragment()
            self.auth()
            self.upload_all(upload_items)
            self.assert_listing("before_restart", expected_sorted)
            self.stop_server()
            self.start_server("after")
            self.auth()
            actual = self.assert_listing("after_restart", expected_sorted)
            self.download_samples(actual)
            self.inspect_indexdb(len(expected_sorted))
            self.write_summary("PASS")
            if self.args.cleanup_roots:
                self.cleanup_roots()
        except Exception:
            self.write_summary("FAIL")
            raise
        finally:
            self.stop_server()


def main():
    proof = N1Proof(parse_args())
    try:
        proof.run()
    except Exception as err:
        print(f"REAL_N1_PROOF FAIL {err}", file=sys.stderr, flush=True)
        return 1
    print("REAL_N1_PROOF PASS", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
