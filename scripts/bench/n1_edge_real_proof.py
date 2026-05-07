#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import time
import urllib.parse
from io import BytesIO
from pathlib import Path

from n1_real_restart_proof import N1Proof, ProofFailure


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a real N1 Edge HTTP/filesystem proof on real disk roots."
    )
    parser.add_argument("--binary", required=True, help="Path to litiaina-n1-edge binary")
    parser.add_argument("--source-dir", required=True, help="Directory to upload")
    parser.add_argument("--disk-roots", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--port", type=int, default=50561)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--prefix", default="edge-proof/")
    parser.add_argument("--quota-bytes", type=int, default=10 * 1024 * 1024 * 1024)
    parser.add_argument("--quota-backend", default="disabled")
    parser.add_argument("--master-key", default="edge-proof-master-key")
    parser.add_argument("--fragment-key", default="edge-proof-fragment-key")
    parser.add_argument("--fragment-name", default="")
    parser.add_argument("--cleanup-roots", action="store_true")
    parser.add_argument("--simulate-disk-format-repair", action="store_true")
    parser.add_argument("--skip-redistribution", action="store_true")
    parser.add_argument("--repair-timeout-seconds", type=int, default=1800)
    return parser.parse_args()


class N1EdgeProof(N1Proof):
    def __init__(self, args):
        super().__init__(args)
        self.active_disk_count = 0
        self.data_shards = 1
        self.parity_shards = 1
        self.expected_payloads = {}

    def active_index_roots(self):
        count = self.active_disk_count or len(self.index_roots)
        return self.index_roots[:count]

    def active_blob_roots(self):
        count = self.active_disk_count or len(self.blob_roots)
        return self.blob_roots[:count]

    def write_config(self):
        cert = self.out_dir / "cert.pem"
        key = self.out_dir / "key.pem"
        if not cert.exists() or not key.exists():
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
                    "JWT_SECRET=edge-proof-jwt-secret",
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
index_store_disks={','.join(str(path) for path in self.active_index_roots())}
blob_store_disks={','.join(str(path) for path in self.active_blob_roots())}
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
file_ttl_seconds=30
retention_seconds=2592000

[set_limits]
max_file_size=10737418240

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
data_shards={self.data_shards}
parity_shards={self.parity_shards}
stripe_block_size=262144
"""
        (self.out_dir / "n1_config.ini").write_text(config)

    def bearer_json(self):
        headers = self.bearer()
        headers["Content-Type"] = "application/json"
        return headers

    def quote_path(self, path):
        return urllib.parse.quote(path, safe="/")

    def upload_bytes(self, logical, payload, content_type="application/octet-stream"):
        headers = self.bearer()
        headers["Content-Type"] = content_type
        status, body = self.http(
            "POST",
            f"{self.base_url}/n1/upload/one-shot/{self.quote_path(logical)}",
            body=payload,
            headers=headers,
            timeout=600,
        )
        if status != 201:
            raise ProofFailure(f"one-shot upload returned {status} for {logical}")
        if body.get("object_key") != logical:
            raise ProofFailure(f"one-shot object key mismatch: {body}")
        self.expected_payloads[logical] = payload
        return body

    def download_bytes(self, logical):
        status, payload = self.http(
            "GET",
            f"{self.base_url}/n1/objects/download?{urllib.parse.urlencode({'object_key': logical})}",
            headers=self.bearer(),
            timeout=600,
        )
        if status != 200:
            raise ProofFailure(f"download returned {status} for {logical}")
        return payload

    def assert_download_hash(self, logical, expected_payload):
        payload = self.download_bytes(logical)
        actual = hashlib.sha256(payload).hexdigest()
        expected = hashlib.sha256(expected_payload).hexdigest()
        if actual != expected:
            raise ProofFailure(f"download hash mismatch for {logical}")

    def posix_page(self, path="", trash=False, token=None):
        params = {"path": path, "limit": "1000"}
        if trash:
            params["trash"] = "true"
        if token:
            params["continuation_token"] = token
        status, page = self.http(
            "GET",
            f"{self.base_url}/n1/posix/list?{urllib.parse.urlencode(params)}",
            headers=self.bearer(),
        )
        if status != 200:
            raise ProofFailure(f"posix list returned {status} for {path!r}")
        return page

    def posix_entries(self, path="", trash=False):
        entries = []
        token = None
        while True:
            page = self.posix_page(path, trash=trash, token=token)
            entries.extend(page.get("entries", []))
            token = page.get("continuation_token")
            if not token:
                return entries

    def posix_walk_files(self, root):
        root = root.strip("/")
        files = []
        dirs = [root]
        while dirs:
            current = dirs.pop(0)
            for entry in self.posix_entries(current):
                if entry.get("kind") == "directory":
                    dirs.append(entry["path"])
                elif entry.get("kind") == "file":
                    files.append(entry["path"])
        return sorted(files)

    def list_all(self):
        root = self.args.prefix.strip("/")
        files = self.posix_walk_files(root)
        return files, max(1, len(files) // 1000 + 1)

    def download_samples(self, actual):
        candidates = [actual[0], actual[len(actual) // 2], actual[-1]]
        source_by_logical = {
            self.args.prefix
            + str(path.relative_to(self.source_dir)).replace(os.sep, "/"): path
            for path in self.source_dir.rglob("*")
            if path.is_file() and ".git" not in path.parts
        }
        results = []
        for logical in candidates:
            expected_payload = self.expected_payloads.get(logical)
            if expected_payload is None:
                expected_payload = source_by_logical[logical].read_bytes()
            self.assert_download_hash(logical, expected_payload)
            results.append(
                {
                    "path": logical,
                    "bytes": len(expected_payload),
                    "sha256": hashlib.sha256(expected_payload).hexdigest(),
                }
            )
        self.summary["download_checks"] = results

    def posix_json(self, route, payload):
        status, body = self.http(
            "POST",
            f"{self.base_url}{route}",
            body=payload,
            headers=self.bearer_json(),
        )
        if status not in (200, 201, 202):
            raise ProofFailure(f"{route} returned {status}: {body}")
        return body

    def edge_posix_crud(self, expected):
        start = time.monotonic()
        base = f"{self.args.prefix}edge-crud"
        self.posix_json("/n1/posix/mkdir", {"path": f"{base}/folder"})
        old_file = f"{base}/rename-me.txt"
        new_file = f"{base}/renamed.txt"
        payload = b"edge rename payload\n"
        self.upload_bytes(old_file, payload, "text/plain")
        self.posix_json(
            "/n1/posix/rename",
            {"source_path": old_file, "destination_path": new_file},
        )
        self.assert_download_hash(new_file, payload)
        expected.append(new_file)

        child = f"{base}/folder/child.txt"
        moved_child = f"{base}/folder-renamed/child.txt"
        child_payload = b"edge folder rename payload\n"
        self.upload_bytes(child, child_payload, "text/plain")
        self.posix_json(
            "/n1/posix/rename",
            {"source_path": f"{base}/folder", "destination_path": f"{base}/folder-renamed"},
        )
        self.assert_download_hash(moved_child, child_payload)

        trash = self.posix_json("/n1/posix/trash", {"path": f"{base}/folder-renamed"})
        trash_id = trash.get("trash_entry_id")
        if not trash_id:
            raise ProofFailure("folder trash did not return trash_entry_id")
        trash_entries = self.posix_entries(base, trash=True)
        if not any(entry.get("trash_entry_id") == trash_id for entry in trash_entries):
            raise ProofFailure("trashed folder not visible in trash listing")
        self.posix_json(
            "/n1/posix/recover",
            {"path": f"{base}/folder-renamed", "trash_entry_id": trash_id},
        )
        self.assert_download_hash(moved_child, child_payload)
        expected.append(moved_child)

        delete_me = f"{base}/delete-me.bin"
        delete_payload = b"edge gc purge payload\n" * 1024
        meta = self.upload_bytes(delete_me, delete_payload)
        version_id = meta["version_id"]
        trash = self.posix_json("/n1/posix/trash", {"path": delete_me})
        self.posix_json(
            "/n1/posix/purge",
            {"path": delete_me, "trash_entry_id": trash.get("trash_entry_id")},
        )
        self.run_gc_and_wait()
        if self.blob_version_exists(version_id):
            raise ProofFailure(f"GC did not reclaim purged version {version_id}")
        self.summary["posix_crud_elapsed_s"] = round(time.monotonic() - start, 3)

    def edge_multipart_proof(self, expected):
        logical = f"{self.args.prefix}edge-multipart/video-parted.bin"
        part_size = 2 * 1024 * 1024
        expected_parts = 4
        payload = deterministic_payload_bytes(8 * 1024 * 1024, b"edge-multipart-proof")
        params = {
            "content_type": "application/octet-stream",
            "total_size_bytes": len(payload),
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        }
        start = time.monotonic()
        status, init = self.http(
            "POST",
            f"{self.base_url}/n1/upload/multipart/init/{self.quote_path(logical)}",
            body=params,
            headers=self.bearer_json(),
        )
        if status != 201:
            raise ProofFailure(f"multipart init returned {status}: {init}")
        version_id = init["version_id"]
        session_id = init["session_id"]
        for idx in range(expected_parts):
            part = payload[idx * part_size : (idx + 1) * part_size]
            status, _ = self.http(
                "PUT",
                f"{self.base_url}/n1/upload/multipart/{version_id}/{idx}?durability=batched",
                body=part,
                headers={**self.bearer(), "Content-Type": "application/octet-stream"},
                timeout=600,
            )
            if status not in (200, 201):
                raise ProofFailure(f"multipart part {idx} returned {status}")
        status, body = self.http(
            "POST",
            f"{self.base_url}/n1/upload/multipart/{version_id}/{session_id}/finalize?{urllib.parse.urlencode({'object_key': logical})}",
            body=params,
            headers=self.bearer_json(),
            timeout=600,
        )
        if status != 200:
            raise ProofFailure(f"multipart finalize returned {status}: {body}")
        elapsed = time.monotonic() - start
        self.assert_download_hash(logical, payload)
        self.expected_payloads[logical] = payload
        expected.append(logical)
        self.summary["multipart"] = {
            "bytes": len(payload),
            "elapsed_s": round(elapsed, 3),
            "mib_s": round((len(payload) / 1024 / 1024) / max(elapsed, 0.001), 3),
            "version_id": version_id,
        }

    def run_gc_and_wait(self):
        status, _ = self.http(
            "POST",
            f"{self.base_url}/n1/maintenance/fragments/{self.fragment}/gc/start?mode=scrub&fast_mode=true",
            headers=self.bearer(),
        )
        if status != 202:
            raise ProofFailure(f"GC start returned {status}")
        self.wait_job("/n1/maintenance/gc/status", "GC")

    def run_redist_and_wait(self):
        params = urllib.parse.urlencode(
            {
                "mode": "scrub",
                "fast_mode": "true",
                "durability": "batched",
                "object_concurrency": "8",
                "status_batch": "512",
            }
        )
        status, _ = self.http(
            "POST",
            f"{self.base_url}/n1/maintenance/fragments/{self.fragment}/redistribution/start?{params}",
            headers=self.bearer(),
        )
        if status != 202:
            raise ProofFailure(f"redistribution start returned {status}")
        self.wait_job("/n1/maintenance/redistribution/status", "redistribution")

    def wait_job(self, path, label, timeout_s=1800):
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            status, body = self.http("GET", f"{self.base_url}{path}", headers=self.bearer())
            if status != 200:
                raise ProofFailure(f"{label} status returned {status}")
            last = body
            if not body.get("is_running"):
                if str(body.get("phase", "")).lower() == "failed" or body.get("last_error"):
                    raise ProofFailure(f"{label} failed: {body}")
                return body
            time.sleep(1)
        raise ProofFailure(f"{label} timed out; last={last}")

    def blob_version_exists(self, version_id):
        return any((root / self.fragment / ".blobs" / version_id).exists() for root in self.blob_roots)

    def edge_archive_proof(self, expected):
        status, raw = self.http(
            "POST",
            f"{self.base_url}/n1/objects/archive",
            body={"prefixes": [self.args.prefix], "format": "tar", "filename": "edge-proof.tar"},
            headers=self.bearer_json(),
            timeout=900,
        )
        if status != 200:
            raise ProofFailure(f"archive returned {status}")
        with tarfile.open(fileobj=BytesIO(raw), mode="r:*") as tar:
            names = set(tar.getnames())
        sample = expected[0]
        if sample not in names:
            raise ProofFailure(f"archive missing sample {sample}")
        self.summary["archive"] = {"bytes": len(raw), "members": len(names)}

    def simulate_recover_on_read(self, logical):
        versions_status, versions = self.http(
            "GET",
            f"{self.base_url}/n1/objects/versions/{self.quote_path(logical)}",
            headers=self.bearer(),
        )
        if versions_status != 200 or not versions:
            raise ProofFailure(f"versions unavailable for recover-on-read {logical}")
        version_id = versions[-1]["version_id"]
        version_dir = self.blob_roots[0] / self.fragment / ".blobs" / version_id
        shard_files = sorted(path for path in version_dir.iterdir() if path.is_file())
        if not shard_files:
            raise ProofFailure(f"no shard files for recover-on-read {version_id}")
        removed = shard_files[0]
        backup = removed.read_bytes()
        removed.unlink()
        self.download_bytes(logical)
        restored = False
        for _ in range(120):
            if removed.exists():
                restored = True
                break
            time.sleep(0.25)
        if not restored:
            removed.write_bytes(backup)
            raise ProofFailure(f"recover-on-read did not restore {removed}")
        self.summary["recover_on_read"] = {"version_id": version_id, "restored": str(removed)}

    def edge_redistribution_proof(self, expected):
        if len(self.index_roots) < 2:
            return
        self.stop_server()
        self.active_disk_count = 1
        self.parity_shards = 0
        self.write_config()
        self.start_server("one-disk")
        self.auth()
        self.assert_listing("one_disk_before_redistribution", sorted(expected))
        self.run_redist_and_wait()
        one_disk_key = f"{self.args.prefix}edge-redistribution/one-disk.txt"
        one_disk_payload = b"edge one disk redistribution payload\n"
        self.upload_bytes(one_disk_key, one_disk_payload, "text/plain")
        expected.append(one_disk_key)
        self.assert_download_hash(one_disk_key, one_disk_payload)

        self.stop_server()
        self.active_disk_count = len(self.index_roots)
        self.parity_shards = 1
        self.write_config()
        self.start_server("two-disk-again")
        self.auth()
        self.run_redist_and_wait()
        self.run_repair_and_wait()
        self.assert_listing("two_disk_after_redistribution", sorted(expected))
        self.assert_download_hash(one_disk_key, one_disk_payload)

    def inspect_edge_index(self, expected_count):
        inspections = []
        for index_root, blob_root in zip(self.index_roots, self.blob_roots):
            fragment_root = index_root / self.fragment
            if (fragment_root / "indexdb").exists():
                raise ProofFailure(f"Edge fragment must not contain indexdb: {fragment_root}")
            live_ndx = []
            trash_ndx = []
            upload_receipts = []
            namespace_symlinks = []
            for path in fragment_root.rglob("*"):
                rel_parts = path.relative_to(fragment_root).parts
                if path.is_symlink() and ".trash" not in rel_parts:
                    namespace_symlinks.append(path)
                if not path.is_file():
                    continue
                if path.name.endswith(".receipt"):
                    upload_receipts.append(path)
                if not path.name.endswith(".ndx"):
                    continue
                if ".uploads" in rel_parts:
                    continue
                if ".trash" in rel_parts:
                    trash_ndx.append(path)
                elif path.parent.name.startswith("@o_"):
                    live_ndx.append(path)
            blob_path = blob_root / self.fragment / ".blobs"
            blob_version_dirs = (
                sum(1 for path in blob_path.iterdir() if path.is_dir())
                if blob_path.exists()
                else 0
            )
            print(
                "REAL_N1_EDGE_PROOF inspect "
                f"index_root={index_root} live_ndx={len(live_ndx)} "
                f"trash_ndx={len(trash_ndx)} namespace_symlinks={len(namespace_symlinks)} "
                f"receipts={len(upload_receipts)} "
                f"blob_version_dirs={blob_version_dirs}",
                flush=True,
            )
            if len(live_ndx) != expected_count:
                raise ProofFailure(
                    f"live_ndx={len(live_ndx)} expected={expected_count} under {fragment_root}"
                )
            inspections.append(
                {
                    "index_root": str(index_root),
                    "blob_root": str(blob_root),
                    "live_ndx": len(live_ndx),
                    "trash_ndx": len(trash_ndx),
                    "namespace_symlinks": len(namespace_symlinks),
                    "upload_receipts": len(upload_receipts),
                    "blob_version_dirs": blob_version_dirs,
                }
            )
        self.summary["edge_index_inspection"] = inspections

    def run(self):
        self.validate()
        self.prepare_dirs()
        self.write_config()
        files, expected = self.files_to_upload()
        upload_items = list(zip(files, expected))
        expected_sorted = sorted(expected)
        self.summary.update(
            {
                "engine": "n1-edge",
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
            self.assert_posix_visible("after_restart")
            self.download_samples(actual)

            live_expected = list(expected_sorted)
            self.edge_posix_crud(live_expected)
            self.edge_multipart_proof(live_expected)
            live_expected = sorted(live_expected)
            self.edge_archive_proof(live_expected)
            self.simulate_recover_on_read(live_expected[0])
            if self.args.simulate_disk_format_repair:
                self.simulate_disk_format_and_repair(live_expected)
            if not self.args.skip_redistribution:
                self.edge_redistribution_proof(live_expected)
                live_expected = sorted(live_expected)
            self.inspect_edge_index(len(live_expected))
            self.write_summary("PASS")
            if self.args.cleanup_roots:
                self.cleanup_roots()
        except Exception:
            self.write_summary("FAIL")
            raise
        finally:
            self.stop_server()


def deterministic_payload_bytes(size, seed):
    digest = hashlib.sha256(seed).digest()
    repeats = (size + len(digest) - 1) // len(digest)
    return (digest * repeats)[:size]


def main():
    proof = N1EdgeProof(parse_args())
    try:
        proof.run()
    except Exception as err:
        print(f"REAL_N1_EDGE_PROOF FAIL {err}", file=sys.stderr, flush=True)
        return 1
    print("REAL_N1_EDGE_PROOF PASS", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
