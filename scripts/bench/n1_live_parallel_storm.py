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
                except json.JSONDecodeError:
                    return res.status, {"raw": raw.decode(errors="replace")}
        except urllib.error.HTTPError as err:
            raw = err.read().decode(errors="replace")
            raise StormFailure(f"{method} {path} -> HTTP {err.code}: {raw}") from None


def deterministic_payload(user_index, object_index):
    size = 1024 + ((user_index * 7919 + object_index * 3571) % (16 * 1024))
    seed = hashlib.sha256(f"{user_index}:{object_index}".encode()).digest()
    repeats = (size + len(seed) - 1) // len(seed)
    return (seed * repeats)[:size]


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


def list_count(client, token, prefix):
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
                    print(
                        "N1_STORM progress "
                        f"objects={done}/{args.users * args.objects_per_user} "
                        f"bytes={progress['bytes']} sample={sample}",
                        flush=True,
                    )
    return {
        "user": user_index,
        "fragment": fragment,
        "prefix": user_prefix,
        "objects": args.objects_per_user,
        "bytes": total_bytes,
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

    progress = {"objects": 0, "bytes": 0}
    lock = threading.Lock()
    start = time.monotonic()
    with futures.ThreadPoolExecutor(max_workers=args.users) as pool:
        running = [
            pool.submit(upload_user, args, user_index, fragment, token, progress, lock)
            for user_index, fragment, _, token in fragments
        ]
        results = [future.result() for future in futures.as_completed(running)]

    for result in sorted(results, key=lambda item: item["user"]):
        count = list_count(client, fragments[result["user"]][3], result["prefix"])
        result["listed_objects"] = count
        if count != result["objects"]:
            raise StormFailure(
                f"user {result['user']} list mismatch: {count} != {result['objects']}"
            )

    summary = {
        "elapsed_s": round(time.monotonic() - start, 3),
        "total_objects": progress["objects"],
        "total_bytes": progress["bytes"],
        "process_sample": sample_process(args.pid),
        "results": sorted(results, key=lambda item: item["user"]),
    }
    print("N1_STORM summary " + json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"N1_STORM failed: {err}", file=sys.stderr, flush=True)
        sys.exit(1)
