#!/usr/bin/env python3
import argparse
import concurrent.futures as futures
import hashlib
import json
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def parse_args():
    parser = argparse.ArgumentParser(
        description="Drive a live N1 server with parallel small-object uploads."
    )
    parser.add_argument("--base-url", default="https://127.0.0.1:50001")
    parser.add_argument("--master-key", required=True)
    parser.add_argument("--fragment-key", default="storm-fragment-key")
    parser.add_argument("--users", type=int, default=4)
    parser.add_argument("--objects-per-user", type=int, default=3000)
    parser.add_argument("--per-user-concurrency", type=int, default=4)
    parser.add_argument("--quota-bytes", type=int, default=20 * 1024 * 1024 * 1024)
    parser.add_argument("--prefix", default="")
    parser.add_argument("--pid", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=250)
    parser.add_argument(
        "--posix-list",
        dest="posix_list",
        action="store_true",
        help="Count uploaded objects through recursive /n1/posix/list for N1.",
    )
    parser.add_argument(
        "--multipart-size-mib",
        type=int,
        default=0,
        help="Upload one multipart object per user after the small-object storm.",
    )
    parser.add_argument("--multipart-part-mib", type=int, default=4)
    return parser.parse_args()


class StormFailure(RuntimeError):
    pass


class Client:
    def __init__(self, base_url, ctx):
        self.base_url = base_url.rstrip("/")
        self.ctx = ctx

    def http(self, method, path, payload=None, headers=None, timeout=300):
        headers = dict(headers or {})
        body = None
        if payload is not None:
            if isinstance(payload, (bytes, bytearray)):
                body = bytes(payload)
            else:
                body = json.dumps(payload).encode()
                headers.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(
            f"{self.base_url}{path}", data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=self.ctx) as res:
                raw = res.read()
                if not raw:
                    return res.status, {}
                try:
                    return res.status, json.loads(raw.decode())
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return res.status, raw
        except urllib.error.HTTPError as err:
            raw = err.read().decode(errors="replace")
            raise StormFailure(f"{method} {path} -> HTTP {err.code}: {raw}") from None


def deterministic_payload_bytes(size, seed):
    digest = hashlib.sha256(seed).digest()
    repeats = (size + len(digest) - 1) // len(digest)
    return (digest * repeats)[:size]


def deterministic_payload(user_index, object_index):
    size = 1024 + ((user_index * 7919 + object_index * 3571) % (16 * 1024))
    return deterministic_payload_bytes(size, f"{user_index}:{object_index}".encode())


def deterministic_multipart_part(user_index, part_index, size):
    return deterministic_payload_bytes(
        size,
        f"multipart:{user_index}:{part_index}".encode(),
    )


def deterministic_multipart_hash(user_index, size, part_size):
    digest = hashlib.sha256()
    offset = 0
    part_index = 0
    while offset < size:
        current = min(part_size, size - offset)
        digest.update(deterministic_multipart_part(user_index, part_index, current))
        offset += current
        part_index += 1
    return digest.hexdigest()


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def create_fragment(client, master_key, fragment_key, name, quota_bytes):
    status, body = client.http(
        "POST",
        "/n1/fragments",
        {
            "name": name,
            "secret": master_key,
            "key": fragment_key,
            "quota_bytes": quota_bytes,
        },
    )
    if status != 201:
        raise StormFailure(f"fragment create returned {status} for {name}")
    return body["fragment_hash"]


def auth_fragment(client, fragment, fragment_key):
    status, body = client.http(
        "POST",
        "/n1/auth/fragment",
        {
            "fragment": fragment,
            "secret": fragment_key,
            "read": True,
            "write": True,
            "modify": True,
            "delete": True,
            "expires_in": 3600,
        },
    )
    if status != 200:
        raise StormFailure(f"fragment auth returned {status} for {fragment}")
    return body["access_token"]


def posix_page(client, token, path, cursor=None):
    params = {"path": path.strip("/"), "limit": "1000"}
    if cursor:
        params["continuation_token"] = cursor
    status, page = client.http(
        "GET",
        f"/n1/posix/list?{urllib.parse.urlencode(params)}",
        headers=bearer(token),
    )
    if status != 200:
        raise StormFailure(f"posix list returned {status}")
    return page


def posix_entries(client, token, path):
    out = []
    cursor = None
    while True:
        page = posix_page(client, token, path, cursor)
        out.extend(page.get("entries", []))
        cursor = page.get("continuation_token")
        if not cursor:
            return out


def posix_list_count(client, token, prefix):
    count = 0
    dirs = [prefix.strip("/")]
    while dirs:
        current = dirs.pop(0)
        for entry in posix_entries(client, token, current):
            if entry.get("kind") == "directory":
                dirs.append(entry["path"])
            elif entry.get("kind") == "file":
                count += 1
    return count


def list_count(client, token, prefix, posix_list=False):
    if posix_list:
        return posix_list_count(client, token, prefix)
    count = 0
    cursor = None
    while True:
        params = {"prefix": prefix, "limit": "1000"}
        if cursor:
            params["continuation_token"] = cursor
        status, page = client.http(
            "GET", f"/n1/objects?{urllib.parse.urlencode(params)}", headers=bearer(token)
        )
        if status != 200:
            raise StormFailure(f"list returned {status}")
        entries = page.get("objects", [])
        count += len(entries)
        cursor = page.get("continuation_token")
        if not cursor:
            return count


def sample_process(pid):
    if pid <= 0:
        return {}
    try:
        out = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "pid=,%cpu=,%mem=,rss=,nlwp="],
            text=True,
        ).strip()
    except subprocess.CalledProcessError:
        return {}
    parts = out.split()
    if len(parts) < 5:
        return {}
    return {
        "pid": int(parts[0]),
        "cpu_percent": float(parts[1]),
        "mem_percent": float(parts[2]),
        "rss_kib": int(parts[3]),
        "threads": int(parts[4]),
    }


def record_sample(progress, sample):
    if not sample:
        return
    progress["max_rss_kib"] = max(progress.get("max_rss_kib", 0), sample.get("rss_kib", 0))
    progress["max_threads"] = max(progress.get("max_threads", 0), sample.get("threads", 0))


def upload_multipart_user(args, user_index, token, user_prefix):
    if args.multipart_size_mib <= 0:
        return None
    ctx = ssl._create_unverified_context()
    client = Client(args.base_url, ctx)
    total_size = args.multipart_size_mib * 1024 * 1024
    part_size = args.multipart_part_mib * 1024 * 1024
    expected_parts = (total_size + part_size - 1) // part_size
    logical = f"{user_prefix}multipart-user-{user_index:02d}.bin"
    params = {
        "content_type": "application/octet-stream",
        "total_size_bytes": total_size,
        "expected_parts": expected_parts,
        "part_size_bytes": part_size,
    }
    headers = bearer(token)
    headers["Content-Type"] = "application/json"
    start = time.monotonic()
    status, init = client.http(
        "POST",
        "/n1/upload/multipart/init/" + urllib.parse.quote(logical, safe="/"),
        payload=params,
        headers=headers,
        timeout=300,
    )
    if status != 201:
        raise StormFailure(f"multipart init returned {status} for {logical}")
    version_id = init["version_id"]
    session_id = init["session_id"]
    offset = 0
    for part_index in range(expected_parts):
        current = min(part_size, total_size - offset)
        part = deterministic_multipart_part(user_index, part_index, current)
        status, _ = client.http(
            "PUT",
            f"/n1/upload/multipart/{version_id}/{part_index}?durability=batched",
            payload=part,
            headers={**bearer(token), "Content-Type": "application/octet-stream"},
            timeout=600,
        )
        if status not in (200, 201):
            raise StormFailure(f"multipart part {part_index} returned {status} for {logical}")
        offset += current
    status, _ = client.http(
        "POST",
        f"/n1/upload/multipart/{version_id}/{session_id}/finalize?"
        + urllib.parse.urlencode({"object_key": logical}),
        payload=params,
        headers=headers,
        timeout=600,
    )
    if status != 200:
        raise StormFailure(f"multipart finalize returned {status} for {logical}")
    status, payload = client.http(
        "GET",
        "/n1/objects/download?" + urllib.parse.urlencode({"object_key": logical}),
        headers=bearer(token),
        timeout=600,
    )
    if status != 200:
        raise StormFailure(f"multipart download returned {status} for {logical}")
    if not isinstance(payload, (bytes, bytearray)):
        raise StormFailure("multipart download did not return raw bytes")
    actual = hashlib.sha256(payload).hexdigest()
    expected = deterministic_multipart_hash(user_index, total_size, part_size)
    if actual != expected:
        raise StormFailure(f"multipart hash mismatch for {logical}")
    elapsed = time.monotonic() - start
    return {
        "logical": logical,
        "bytes": total_size,
        "parts": expected_parts,
        "elapsed_s": round(elapsed, 3),
        "mib_s": round((total_size / 1024 / 1024) / max(elapsed, 0.001), 3),
        "expected_sha256": expected,
        "actual_sha256": actual,
    }


def upload_user(args, user_index, fragment, token, progress, lock):
    ctx = ssl._create_unverified_context()
    client = Client(args.base_url, ctx)
    prefix = args.prefix or f"storm-{int(time.time())}/"
    user_prefix = f"{prefix}user-{user_index:02d}/"
    headers = bearer(token)
    headers["Content-Type"] = "application/octet-stream"

    def upload_one(object_index):
        logical = (
            f"{user_prefix}dir-{object_index // 100:03d}/"
            f"nested-{object_index % 17:02d}/file-{object_index:04d}.bin"
        )
        payload = deterministic_payload(user_index, object_index)
        path = "/n1/upload/one-shot/" + urllib.parse.quote(logical, safe="/")
        last_err = None
        for attempt in range(8):
            try:
                status, body = client.http("POST", path, payload=payload, headers=headers)
                if status != 201:
                    raise StormFailure(f"upload returned {status} for {logical}")
                if body.get("object_key") != logical:
                    raise StormFailure(f"object key mismatch for {logical}")
                return len(payload)
            except Exception as err:
                last_err = err
                time.sleep(min(8.0, 0.25 * (2**attempt)))
        raise last_err

    total_bytes = 0
    start = time.monotonic()
    with futures.ThreadPoolExecutor(max_workers=args.per_user_concurrency) as pool:
        running = [pool.submit(upload_one, i) for i in range(args.objects_per_user)]
        for future in futures.as_completed(running):
            size = future.result()
            total_bytes += size
            with lock:
                progress["objects"] += 1
                progress["bytes"] += size
                done = progress["objects"]
                if done % args.progress_every == 0:
                    sample = sample_process(args.pid)
                    record_sample(progress, sample)
                    print(
                        "N1_STORM progress "
                        f"objects={done}/{args.users * args.objects_per_user} "
                        f"bytes={progress['bytes']} sample={sample}",
                        flush=True,
                    )
    multipart = upload_multipart_user(args, user_index, token, user_prefix)
    return {
        "user": user_index,
        "fragment": fragment,
        "prefix": user_prefix,
        "objects": args.objects_per_user,
        "bytes": total_bytes,
        "multipart": multipart,
        "elapsed_s": round(time.monotonic() - start, 3),
    }


def main():
    args = parse_args()
    ctx = ssl._create_unverified_context()
    client = Client(args.base_url, ctx)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    fragments = []
    print(
        "N1_STORM start "
        f"users={args.users} objects_per_user={args.objects_per_user} "
        f"per_user_concurrency={args.per_user_concurrency}",
        flush=True,
    )
    for user_index in range(args.users):
        key = f"{args.fragment_key}-{stamp}-{user_index}"
        name = f"storm-{stamp}-u{user_index}"
        fragment = create_fragment(client, args.master_key, key, name, args.quota_bytes)
        token = auth_fragment(client, fragment, key)
        fragments.append((user_index, fragment, key, token))
        print(f"N1_STORM fragment user={user_index} fragment={fragment}", flush=True)

    progress = {"objects": 0, "bytes": 0, "max_rss_kib": 0, "max_threads": 0}
    record_sample(progress, sample_process(args.pid))
    lock = threading.Lock()
    start = time.monotonic()
    with futures.ThreadPoolExecutor(max_workers=args.users) as pool:
        running = [
            pool.submit(upload_user, args, user_index, fragment, token, progress, lock)
            for user_index, fragment, _, token in fragments
        ]
        results = [future.result() for future in futures.as_completed(running)]

    for result in sorted(results, key=lambda item: item["user"]):
        count = list_count(
            client,
            fragments[result["user"]][3],
            result["prefix"],
            posix_list=args.posix_list,
        )
        result["listed_objects"] = count
        expected_count = result["objects"] + (1 if result.get("multipart") else 0)
        if count != expected_count:
            raise StormFailure(
                f"user {result['user']} list mismatch: {count} != {expected_count}"
            )

    elapsed = time.monotonic() - start
    multipart_bytes = sum(
        item["multipart"]["bytes"] for item in results if item.get("multipart")
    )
    total_bytes = progress["bytes"] + multipart_bytes
    sample = sample_process(args.pid)
    record_sample(progress, sample)
    summary = {
        "elapsed_s": round(elapsed, 3),
        "total_objects": progress["objects"],
        "total_listed_objects": sum(item["listed_objects"] for item in results),
        "small_object_bytes": progress["bytes"],
        "multipart_bytes": multipart_bytes,
        "total_bytes": total_bytes,
        "objects_per_second": round(progress["objects"] / max(elapsed, 0.001), 3),
        "mib_per_second": round((total_bytes / 1024 / 1024) / max(elapsed, 0.001), 3),
        "max_rss_kib": progress["max_rss_kib"],
        "max_threads": progress["max_threads"],
        "process_sample": sample,
        "results": sorted(results, key=lambda item: item["user"]),
    }
    print("N1_STORM summary " + json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"N1_STORM failed: {err}", file=sys.stderr, flush=True)
        sys.exit(1)
