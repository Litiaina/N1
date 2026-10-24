#!/usr/bin/env python3
"""Real two-process NCNX stage-lease proof, with controlled peer-message barriers.

Runs only child engine processes, loopback TLS and new HDD fixture directories.
It neither installs services nor accesses existing cluster data. This is a
same-host protocol proof, not independent-node, power-loss or scale qualification.
"""

import argparse
import base64
import concurrent.futures
import hashlib
import http.server
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


LEASE_PATH = "/n2/peer/ncnx-v4/lease"
PEER_PATH = "/n2/peer/ncnx-v4"
HDD = Path("/mnt/PROJECT-DISK-1TB")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(predicate, description, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError(f"timed out waiting for {description}")


class PeerBarrier:
    def __init__(self):
        self.lock = threading.Lock()
        self.armed = None
        self.entered = threading.Event()
        self.released = threading.Event()
        self.timeline = []

    def arm(self, action, key):
        with self.lock:
            assert self.armed is None
            self.entered.clear()
            self.released.clear()
            self.armed = (action, key)

    def observe(self, path, payload):
        if not isinstance(payload, dict):
            return
        action = payload.get("action")
        keys = [item.get("logical_key") for item in payload.get("items", [])]
        if "logical_key" in payload:
            keys.append(payload["logical_key"])
        with self.lock:
            self.timeline.append({"at_monotonic": time.monotonic(), "path": path,
                                  "action": action, "keys": keys})
            matched = self.armed and action == self.armed[0] and self.armed[1] in keys
            if matched:
                self.armed = None
                self.entered.set()
        if matched and not self.released.wait(45):
            raise TimeoutError("controlled peer barrier was not released")

    def release(self):
        self.released.set()
        with self.lock:
            self.armed = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    engine = args.engine.resolve(strict=True)
    out = args.out.resolve()
    assert out.is_relative_to(HDD.resolve()), "all proof evidence must be on the project HDD"
    assert engine.is_relative_to(HDD.resolve()), "use the engine built on the project HDD"
    out.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="n2-ncnx-coordination-", dir=out))
    print(f"Evidence: {root}", flush=True)
    context = ssl._create_unverified_context()  # self-signed loopback fixture only
    secret, token, fragment = secrets.token_hex(32), "", ""
    processes, logs, results = {}, [], []
    background_stops = []
    barrier = PeerBarrier()
    proxy = None
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
    ports = []
    while len(ports) < 5:
        candidate = free_port()
        if candidate not in ports:
            ports.append(candidate)
    bases = [f"https://127.0.0.1:{ports[0]}", f"https://127.0.0.1:{ports[2]}"]
    advertised_b = f"https://127.0.0.1:{ports[4]}"

    def request(base, path, method="GET", data=None, expected=(200, 201, 202, 204),
                peer=False, auth=True, timeout=90):
        headers = {}
        if token and auth:
            headers["Authorization"] = f"Bearer {token}"
        if peer:
            headers["x-n2-cluster-secret"] = secret
        if isinstance(data, (dict, list)):
            data = json.dumps(data).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(base + path, data=data, method=method, headers=headers)
        try:
            response = urllib.request.urlopen(req, context=context, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            assert response.status in expected, (method, path, response.status, body[:600])
            return response.status, body

    def api(base, path, method="GET", data=None, **kwargs):
        status, body = request(base, path, method, data, **kwargs)
        return json.loads(body) if body else {"http_status": status}

    def start(index):
        name = f"node-{'ab'[index]}"
        handle = open(root / f"{name}.log", "ab")
        logs.append(handle)
        env = dict(os.environ, TMPDIR=str(root), N2_RUNTIME_WORKER_THREADS="4",
                   N2_RUNTIME_MAX_BLOCKING_THREADS="16", RUST_LOG="info")
        processes[index] = subprocess.Popen([str(engine)], cwd=root / name, env=env,
                                            stdout=handle, stderr=subprocess.STDOUT)

    def stop(index):
        process = processes.pop(index, None)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    def wait_up(index):
        def ready():
            assert processes[index].poll() is None, f"node {index} exited; inspect fixture logs"
            try:
                request(bases[index], "/noa/v1/ping", auth=False, timeout=2)
                return True
            except (AssertionError, OSError):
                return False
        wait_for(ready, f"engine {index} readiness", timeout=45)

    def case(name, function):
        begin = time.monotonic()
        detail = function()
        entry = {"name": name, "status": "PASS", "duration_seconds": time.monotonic() - begin}
        if detail is not None:
            entry["details"] = detail
        results.append(entry)
        print(f"PASS {name}", flush=True)

    def live(index, key):
        return root / f"node-{'ab'[index]}" / "disk" / f"@f_{fragment}" / ".namespace" / f"{key}.ncnx"

    def upload(key, content=b"NCNX key coordination fixture\n", index=0):
        return api(bases[index], f"/noa/v1/upload/one-shot/{key}", "POST", content)

    def rename(source, destination, expected=(200,)):
        return request(bases[0], "/noa/v1/objects/rename", "POST", {
            "source_fragment": fragment, "destination_fragment": fragment,
            "source_key": source, "destination_key": destination,
        }, expected=expected)

    def cycle():
        current = api(bases[0], "/noa/v1/cluster/healing/cycle", "POST", {})
        assert current["owned_fragments"] >= 1, current
        assert current["scanned_namespace_records"] >= 1, current
        assert not current["running"] and current["finished_at_unix"] is not None, current
        assert not current["safety_read_only"], current
        return current

    def lease(index, key, action="acquire", owner=None, grant=None, ttl_ms=3000,
              expected=(200,), **changes):
        payload = dict(action=action, cluster_id=cluster_id, pool_id="proof", pool_generation=1,
                       target_slot_id=f"node-{'ab'[index]}", target_instance_id=f"node-{'ab'[index]}",
                       binding_epoch=1, fragment_id=fragment, owner_id=owner or secrets.token_hex(16),
                       ttl_ms=ttl_ms, items=[dict(logical_key=key, root_id=f"root-{index}", token=grant)])
        payload.update(changes)
        return api(bases[index], LEASE_PATH, "POST", payload, peer=True, auth=False, expected=expected)

    def peer_write(index, key, content, grant=None, action="repair_live", expected=(200,), **changes):
        payload = dict(action=action, pool_id="proof", pool_generation=1,
                       target_slot_id=f"node-{'ab'[index]}", target_instance_id=f"node-{'ab'[index]}",
                       binding_epoch=1, fragment_id=fragment, logical_key=key, root_id=f"root-{index}",
                       bytes=list(content), lease_token=grant)
        payload.update(changes)
        return api(bases[index], PEER_PATH, "POST", payload, peer=True, auth=False, expected=expected)

    # Select the real production healer coordinator deterministically. With a
    # one-node healer pool the chosen node owns every fixture fragment.
    def healer_score(cluster, node):
        fields = [b"n2-healer-pool-v1", cluster.encode(), (1).to_bytes(8, "big"), b"", node.encode()]
        return hashlib.sha256(b"".join(field + b"\0" for field in fields)).digest()
    suffix = 0
    while True:
        cluster_id = f"isolated-ncnx-coordination-{suffix}"
        if healer_score(cluster_id, "node-a") > healer_score(cluster_id, "node-b"):
            break
        suffix += 1

    class ProxyHandler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            self.forward()

        def do_GET(self):
            self.forward()

        def do_PUT(self):
            self.forward()

        def do_DELETE(self):
            self.forward()

        def forward(self):
            content = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            try:
                payload = json.loads(content) if content else None
            except ValueError:
                payload = None
            try:
                barrier.observe(self.path, payload)
                headers = {key: value for key, value in self.headers.items()
                           if key.lower() not in ("host", "connection", "content-length")}
                req = urllib.request.Request(bases[1] + self.path, method=self.command,
                                             data=content if content else None, headers=headers)
                try:
                    response = urllib.request.urlopen(req, context=context, timeout=90)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    body = response.read()
                    self.send_response(response.status)
                    for key, value in response.headers.items():
                        if key.lower() not in ("connection", "transfer-encoding", "content-length"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
            except (OSError, TimeoutError) as error:
                self.send_error(502, str(error))

        def log_message(self, *_):
            pass  # no credentials or grant tokens in proof logs

    report = {"status": "RUNNING", "scope": "two real engine processes, one HDD, loopback TLS",
              "limitations": ["not independent failure domains", "not physical power-loss qualification",
                              "not scale qualification", "not cross-pool logical-key linearizability"],
              "cases": results}
    try:
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", str(root / "key.pem"), "-out", str(root / "cert.pem"),
                        "-days", "1", "-subj", "/CN=localhost"], check=True, capture_output=True)
        nodes = []
        for index, name in enumerate(("node-a", "node-b")):
            cwd = root / name
            (cwd / "state").mkdir(parents=True)
            (cwd / "disk").mkdir()
            endpoint = bases[0] if index == 0 else advertised_b
            nodes.append(dict(slot_id=name, instance_id=name, binding_epoch=1, endpoint=endpoint,
                              native_endpoint=f"n2://127.0.0.1:{ports[index*2+1]}", state="online", weight=1,
                              storage_roots=[dict(id=f"root-{index}", path=str(cwd / "disk"))], joined_epoch=1))
            env_path = cwd / "n2.env"
            env_path.write_text(f"JWT_SECRET={secret}\nMASTER_KEY={secret}\nAUTH_KEY={secret}\n"
                                f"ADMIN_KEY={secret}\nN2_CLUSTER_SECRET={secret}\n")
            env_path.chmod(0o600)
            (cwd / "n2_config.ini").write_text(f"""[server]
host=127.0.0.1
https_port={ports[index*2]}
native_port={ports[index*2+1]}
cert_path={root}/cert.pem
key_path={root}/key.pem
[disk_config]
storage_roots=root-{index}={cwd}/disk
runtime_path={cwd}/state
[cluster_config]
node_id={name}
pool_id=proof
pool_generation=1
node_endpoint={endpoint}
native_endpoint=n2://127.0.0.1:{ports[index*2+1]}
membership_file={cwd}/state/n2_membership.json
object_data_shards=1
object_parity_shards=1
auto_rebalance=false
[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
retention_seconds=3600
[auto_healing]
enabled=false
coordinator_nodes=1
max_parallel_fragments=1
lease_seconds=10
max_objects_per_fragment_cycle=1000
max_duration_seconds=30
[reserve_set]
enabled=false
automatic_promotion=false
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
        membership = dict(format_version=5, cluster_id=cluster_id, epoch=1, binding_epoch=1,
                          pools=[dict(id="proof", generation=1,
                                      policy=dict(object_data_shards=1, object_parity_shards=1),
                                      state="writable", slot_ids=["node-a", "node-b"])],
                          slot_bindings=nodes, reserve_nodes=[], retired_instances=[])
        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", ports[4]), ProxyHandler)
        proxy.daemon_threads = True
        proxy_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        proxy_context.load_cert_chain(root / "cert.pem", root / "key.pem")
        proxy.socket = proxy_context.wrap_socket(proxy.socket, server_side=True)
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        for index, name in enumerate(("node-a", "node-b")):
            (root / name / "state/n2_membership.json").write_text(json.dumps(membership))
            start(index)
        for index in range(2):
            wait_up(index)
        created = api(bases[0], "/noa/v1/fragments", "POST", dict(name="key-coordination", secret=secret, key=secret))
        token = api(bases[0], "/noa/v1/auth/fragment", "POST", dict(fragment=created["fragment_hash"],
                    secret=secret, read=True, write=True, modify=True, delete=True, expires_in=3600))["access_token"]
        encoded_claims = token.split(".")[1]
        fragment = json.loads(base64.urlsafe_b64decode(encoded_claims + "=" * (-len(encoded_claims) % 4)))["fragment"]
        upload("race/source.bin")
        upload("tokens/source.bin")
        original = live(0, "tokens/source.bin").read_bytes()
        immutable_snapshot = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                              for index in range(2)
                              for path in (root / f"node-{'ab'[index]}" / "disk" / f"@f_{fragment}" / ".objects").rglob("*")
                              if path.is_file()}
        assert immutable_snapshot, "proof must exercise real immutable NCDX and payload files"

        def tokenless_rejected():
            peer_write(1, "tokens/source.bin", original, expected=(400, 403, 409))
            assert live(1, "tokens/source.bin").read_bytes() == original
            unauthenticated = dict(action="acquire", cluster_id=cluster_id, pool_id="proof", pool_generation=1,
                                   target_slot_id="node-b", target_instance_id="node-b", binding_epoch=1,
                                   fragment_id=fragment, owner_id="unauthenticated-owner", ttl_ms=1000,
                                   items=[dict(logical_key="tokens/source.bin", root_id="root-1")])
            request(bases[1], LEASE_PATH, "POST", unauthenticated, auth=False, expected=(401, 403))
        case("tokenless mutation and unauthenticated grant fail closed", tokenless_rejected)

        def remove_vs_heal():
            barrier.arm("remove_live", "race/source.bin")
            mutation = pool.submit(rename, "race/source.bin", "race/destination.bin")
            assert barrier.entered.wait(15), "production source remove did not hit peer barrier"
            wait_for(lambda: not live(0, "race/source.bin").exists(), "first exact holder removal")
            assert live(1, "race/source.bin").exists()
            upload("independent/during-remove.txt", b"unrelated key keeps progressing\n")
            status = cycle()
            assert status["incomplete_fragments"] > 0 or status.get("last_error"), status
            assert not live(0, "race/source.bin").exists(), "healer resurrected partial source removal"
            barrier.release()
            mutation.result(timeout=20)
            assert all(not live(index, "race/source.bin").exists() for index in range(2))
            assert all(live(index, "race/destination.bin").exists() for index in range(2))
            cycle()
            assert all(not live(index, "race/source.bin").exists() for index in range(2))
            return {"partial_removal_observed": True, "healer_deferred": True,
                    "independent_key_completed": True, "post_ack_resurrection": False}
        case("partial exact-holder removal excludes healing and preserves unrelated progress", remove_vs_heal)

        def repair_vs_remove():
            key = "repair/source.bin"
            upload(key)
            canonical = live(0, key).read_bytes()
            live(1, key).unlink()  # deliberately damage only this disposable fixture
            barrier.arm("repair_live", key)
            healing = pool.submit(cycle)
            assert barrier.entered.wait(15), "production healer did not reach repair publication barrier"
            rename(key, "repair/destination.bin", expected=(409, 503))
            assert live(0, key).read_bytes() == canonical, "removal crossed owned repair stage"
            upload("independent/during-repair.txt", b"another key during repair\n")
            barrier.release()
            healed = healing.result(timeout=20)
            assert healed["repaired_namespace_records"] >= 1, healed
            assert live(1, key).read_bytes() == canonical
            rename(key, "repair/destination.bin")
            assert all(not live(index, key).exists() for index in range(2))
            return {"missing_pointer_repaired": True, "concurrent_removal_deferred": True}
        case("owned fresh-read repair excludes source removal until joined completion", repair_vs_remove)

        def corrupt_pointer():
            key = "tokens/source.bin"
            damaged = bytearray(original)
            damaged[len(damaged) // 2] ^= 0x80
            live(1, key).write_bytes(damaged)
            status = cycle()
            assert status["repaired_namespace_records"] >= 1, status
            assert live(1, key).read_bytes() == original
        case("real checksum-invalid NCNX restored from verified replica", corrupt_pointer)

        def stale_grants():
            key, owner = "tokens/source.bin", secrets.token_hex(16)
            expired = lease(1, key, owner=owner, ttl_ms=200)
            old = expired["items"][0]["token"]
            time.sleep(0.4)
            peer_write(1, key, original, old, expected=(409,))
            lease(1, key, "renew", owner, old, expected=(409,))
            current = lease(1, key, owner=owner)
            new = current["items"][0]["token"]
            assert new != old
            peer_write(1, key, original, old, action="remove_live", expected=(409,))
            lease(1, key, "release", owner, old, expected=(409,))
            lease(1, key, "renew", owner, new)
            peer_write(1, "another/key.bin", original, new, expected=(409,))
            peer_write(1, key, original, new)
            lease(1, key, "release", owner, new)
            peer_write(1, key, original, new, action="remove_live", expected=(409,))
            successor = lease(1, key, owner=owner)
            assert successor["items"][0]["token"] not in (old, new)
            lease(1, key, "release", owner, successor["items"][0]["token"])
            assert live(1, key).read_bytes() == original
        case("expired released cross-key and delayed-release tokens cannot affect successor", stale_grants)

        def invalid_contexts_and_aliases():
            key, owner = "tokens/source.bin", secrets.token_hex(16)
            for change in (dict(cluster_id="other-cluster"), dict(pool_id="other-pool"),
                           dict(pool_generation=2), dict(binding_epoch=2),
                           dict(target_instance_id="replaced-instance"), dict(target_slot_id="node-a")):
                lease(1, key, expected=(409,), **change)
            for ttl in (0, 60001):
                lease(1, key, ttl_ms=ttl, expected=(400,))
            lease(1, key, items=[dict(logical_key=key, root_id="root-1", token="chosen-by-caller")],
                  expected=(400,))
            lease(1, key, items=[dict(logical_key=key, root_id="root-1"),
                                 dict(logical_key=f"/{key}//", root_id="root-1")], expected=(400,))
            lease(1, key, items=[dict(logical_key=f"oversized/{i}", root_id="root-1")
                                 for i in range(129)], expected=(400,))
            current = lease(1, key, owner=owner)
            grant = current["items"][0]["token"]
            lease(1, f"/{key}//", expected=(409,))
            lease(1, key, fragment_id=f"@f_{fragment}", expected=(409,))
            peer_write(1, key, original, grant, binding_epoch=2, expected=(409,))
            lease(1, key, "release", owner, grant)
            assert live(1, key).read_bytes() == original
        case("binding geometry aliases forged grants and oversized groups fail closed", invalid_contexts_and_aliases)

        def invalid_batch_token():
            keys = ["tokens/source.bin", "independent/during-repair.txt"]
            owner = secrets.token_hex(16)
            current = lease(1, keys[0], owner=owner,
                            items=[dict(logical_key=key, root_id="root-1") for key in keys])
            grants = {item["logical_key"]: item["token"] for item in current["items"]}
            contents = [live(1, key).read_bytes() for key in keys]
            request(bases[1], PEER_PATH + "/batch", "POST", dict(
                action="remove_live", pool_id="proof", pool_generation=1,
                target_slot_id="node-b", target_instance_id="node-b", binding_epoch=1,
                fragment_id=fragment, root_id="root-1", items=[dict(
                    logical_key=key, bytes=list(contents[index]),
                    lease_token=grants[key] if index == 0 else "invalid-second-item-token")
                    for index, key in enumerate(keys)]), peer=True, auth=False, expected=(409,))
            for key, content in zip(keys, contents):
                assert live(1, key).read_bytes() == content, "batch published before validating every grant"
            lease(1, keys[0], "release", owner,
                  items=[dict(logical_key=key, root_id="root-1", token=grants[key]) for key in keys])
            return {"validated_before_any_item_mutation": True, "keys_unchanged": len(keys)}
        case("grouped receiver validates every item token before any filesystem mutation", invalid_batch_token)

        def partial_acquisition_cleanup():
            key, owner = "tokens/source.bin", secrets.token_hex(16)
            held = lease(1, key, owner=owner, ttl_ms=30000)
            rename(key, "partial/destination.bin", expected=(409, 503))
            # The failed source stage may already have published its separate
            # destination stage. It must neither remove a source holder nor
            # strand the other holder's successfully acquired source grant.
            local_owner = secrets.token_hex(16)
            local = lease(0, key, owner=local_owner)
            lease(0, key, "release", local_owner, local["items"][0]["token"])
            lease(1, key, "release", owner, held["items"][0]["token"])
            assert all(live(index, key).read_bytes() == original for index in range(2))
            return {"partial_source_grant_released": True, "no_source_removed": True,
                    "compound_destination_may_exist": True}
        case("partial all-holder acquisition releases acquired grants before any stage write", partial_acquisition_cleanup)

        def restart_fence():
            key, owner = "tokens/source.bin", secrets.token_hex(16)
            before = lease(1, key, owner=owner, ttl_ms=60000)
            old = before["items"][0]["token"]
            stop(1)
            start(1)
            wait_up(1)
            peer_write(1, key, original, old, action="remove_live", expected=(409,))
            lease(1, key, "renew", owner, old, expected=(409,))
            after = lease(1, key, owner=owner)
            assert before["boot_id"] != after["boot_id"]
            assert old != after["items"][0]["token"]
            lease(1, key, "release", owner, after["items"][0]["token"])
            assert live(1, key).read_bytes() == original
        case("SIGKILL restart invalidates old incarnation tokens without namespace loss", restart_fence)

        def opposing_trash_recover():
            key = "opposing/item.bin"
            upload(key, b"opposing lifecycle namespace operations\n")
            canonical = live(0, key).read_bytes()
            barrier.arm("remove_live", key)
            deleting = pool.submit(api, bases[0], f"/noa/v1/objects/{key}", "DELETE")
            assert barrier.entered.wait(15), "trash removal did not reach controlled peer barrier"
            wait_for(lambda: not live(0, key).exists(), "first live holder trashed")
            trash_paths = list((live(0, key).parents[2] / ".trash").glob(f"*/{key}.ncnx"))
            assert len(trash_paths) == 1, trash_paths
            mutation_id = trash_paths[0].parents[1].name.split(".", 1)[1]
            request(bases[0], f"/noa/v1/objects/recover/{mutation_id}", "POST", {}, expected=(409, 503))
            assert not live(0, key).exists(), "recovery crossed in-progress owned trash move"
            assert trash_paths[0].read_bytes() == canonical
            barrier.release()
            deletion = deleting.result(timeout=20)
            assert deletion["trash_entry_id"] == mutation_id, deletion
            assert all(not live(index, key).exists() for index in range(2))

            barrier.arm("remove_trash", key)
            recovering = pool.submit(api, bases[0], f"/noa/v1/objects/recover/{mutation_id}", "POST", {})
            assert barrier.entered.wait(15), "recover removal did not reach controlled peer barrier"
            wait_for(lambda: not trash_paths[0].exists(), "first trash holder recovered")
            request(bases[0], f"/noa/v1/objects/{key}", "DELETE", expected=(409, 503))
            assert all(live(index, key).read_bytes() == canonical for index in range(2)), \
                "opposing trash removed newly recovered live pointers"
            barrier.release()
            recovering.result(timeout=20)
            assert all(live(index, key).read_bytes() == canonical for index in range(2))
            for index in range(2):
                assert not list((live(index, key).parents[2] / ".trash").glob(f"*/{key}.ncnx")), \
                    "acknowledged recovery retained an exact trash generation"
            assert request(bases[0], f"/noa/v1/objects/get/{key}")[1] == b"opposing lifecycle namespace operations\n"
            return {"opposing_recover_deferred_during_trash": True,
                    "opposing_trash_deferred_during_recover": True,
                    "acknowledged_recovery_retains_every_live_pointer": True}
        case("opposing trash and recover cannot jointly remove every namespace pointer", opposing_trash_recover)

        def bounded_concurrent_stress():
            stress_begin = time.monotonic()
            contents = {f"stress/objects/{index:03d}.bin": hashlib.sha256(
                f"n2-stage-stress-{index}".encode()).digest() * 512 for index in range(64)}
            retries, acknowledgements, healing_results, timings = [], [], [], []
            result_lock = threading.Lock()
            peak_rss_kib = {"node-a": 0, "node-b": 0}
            stop_memory = threading.Event()
            background_stops.append(stop_memory)
            report["stress"] = dict(acknowledgements=acknowledgements, retries=retries,
                                    healing_cycles=healing_results, request_timings=timings,
                                    max_engine_rss_kib=peak_rss_kib)

            def sample_memory():
                deadline = time.monotonic() + 300
                while not stop_memory.is_set() and time.monotonic() < deadline:
                    for index, process in list(processes.items()):
                        try:
                            status = Path(f"/proc/{process.pid}/status").read_text()
                        except OSError:
                            continue
                        for line in status.splitlines():
                            if line.startswith("VmRSS:"):
                                name = f"node-{'ab'[index]}"
                                peak_rss_kib[name] = max(peak_rss_kib[name], int(line.split()[1]))
                                break
                    stop_memory.wait(0.1)
            memory_thread = threading.Thread(target=sample_memory, daemon=True)
            memory_thread.start()

            def retry_request(path, method, data=None, success=(200, 201), label="operation"):
                deadline = time.monotonic() + 45
                attempts = 0
                while attempts < 24 and time.monotonic() < deadline:
                    attempts += 1
                    request_begin = time.monotonic()
                    status, body = request(bases[0], path, method, data,
                                           expected=success + (409, 503), timeout=20)
                    with result_lock:
                        timings.append(dict(operation=label, http_status=status,
                                            seconds=time.monotonic() - request_begin))
                    if status in success:
                        with result_lock:
                            acknowledgements.append({"operation": label, "path": path,
                                                     "http_status": status, "attempts": attempts})
                        return json.loads(body) if body else None
                    with result_lock:
                        retries.append({"operation": label, "path": path, "http_status": status,
                                        "error_response": body[:600].decode("utf-8", errors="replace")})
                    time.sleep(min(0.025 * (2 ** min(attempts - 1, 4)), 0.4))
                raise AssertionError(f"bounded retry exhausted: {label} {path}; final status {status}; {body[:300]!r}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as workers:
                futures = [workers.submit(retry_request, f"/noa/v1/upload/one-shot/{key}", "POST",
                                          content, (201,), "upload") for key, content in contents.items()]
                for future in futures:
                    future.result(timeout=100)
            upload_elapsed = time.monotonic() - stress_begin
            before_mutation = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                               for index in range(2)
                               for path in (root / f"node-{'ab'[index]}" / "disk" / f"@f_{fragment}" / ".objects").rglob("*")
                               if path.is_file()}
            assert len(before_mutation) > len(immutable_snapshot)
            (root / "stress-immutable-before.json").write_text(json.dumps(before_mutation, indent=2) + "\n")
            (root / "stress-expected-contents.json").write_text(json.dumps({
                key: hashlib.sha256(content).hexdigest() for key, content in contents.items()}, indent=2) + "\n")
            stop_healing = threading.Event()
            first_cycle_started = threading.Event()

            def heal_repeatedly():
                for _ in range(12):
                    if stop_healing.is_set():
                        break
                    cycle_begin = time.monotonic()
                    first_cycle_started.set()
                    result = cycle()
                    result.update(proof_started_monotonic=cycle_begin,
                                  proof_finished_monotonic=time.monotonic())
                    healing_results.append(result)
                    stop_healing.wait(0.025)

            healer = pool.submit(heal_repeatedly)
            assert first_cycle_started.wait(5)
            mutation_begin = time.monotonic()

            def mutate(index):
                source = f"stress/objects/{index:03d}.bin"
                destination = f"stress/renamed/{index:03d}.bin"
                retry_request("/noa/v1/objects/rename", "POST", dict(
                    source_fragment=fragment, destination_fragment=fragment,
                    source_key=source, destination_key=destination), (200,), "rename")
                deleted = retry_request(f"/noa/v1/objects/{destination}", "DELETE", success=(200,), label="trash")
                trash_id = deleted.get("trash_entry_id")
                assert trash_id, deleted
                retry_request(f"/noa/v1/objects/recover/{trash_id}", "POST", {}, (200,), "recover")

            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as workers:
                    futures = [workers.submit(mutate, index) for index in range(24)]
                    for future in futures:
                        future.result(timeout=180)
                mutation_end = time.monotonic()
            finally:
                stop_healing.set()
                healer.result(timeout=100)
                (root / "stress-acknowledgements.json").write_text(json.dumps(acknowledgements, indent=2) + "\n")
                (root / "stress-retries.json").write_text(json.dumps(retries, indent=2) + "\n")
                (root / "stress-healing-cycles.json").write_text(json.dumps(healing_results, indent=2) + "\n")
            mutation_elapsed = time.monotonic() - mutation_begin
            verification_begin = time.monotonic()

            expected = {(key.replace("stress/objects/", "stress/renamed/") if index < 24 else key): content
                        for index, (key, content) in enumerate(contents.items())}

            def listed(trash=False):
                cursor, records, pages = None, [], 0
                while True:
                    params = dict(prefix="stress/", limit="7")
                    if cursor is not None:
                        params["continuation_token"] = cursor
                    page = api(bases[0], ("/noa/v1/trash?" if trash else "/noa/v1/objects?") + urllib.parse.urlencode(params))
                    assert "objects" in page and "continuation_token" in page, page
                    # Rust's ListObjectsResult deliberately omits empty entries.
                    records.extend(entry["record"] for entry in page.get("entries", []))
                    pages += 1
                    following = page.get("continuation_token")
                    if following is None:
                        break
                    assert following != cursor and pages < 30, "pagination must advance and terminate"
                    cursor = following
                assert len(records) == len(set(records)), "duplicate keys across listing pages"
                return records, pages

            observed, pages = listed()
            assert observed == sorted(expected), (observed, sorted(expected))
            assert listed(trash=True)[0] == [], "acknowledged recovery left or resurrected trash pointers"
            for key, content in expected.items():
                for index in range(2):
                    assert request(bases[index], f"/noa/v1/objects/get/{key}")[1] == content, key
            for path, digest in before_mutation.items():
                assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, path
            assert len(acknowledgements) == 64 + 24 * 3, len(acknowledgements)
            assert healing_results, "stress must overlap at least one actual explicit healing cycle"
            assert any(entry["proof_started_monotonic"] < mutation_end
                       and entry["proof_finished_monotonic"] > mutation_begin
                       for entry in healing_results), "no measured healing/mutation overlap"
            final_cycle = cycle()
            assert final_cycle["incomplete_fragments"] == 0 and not final_cycle.get("last_error"), final_cycle
            assert listed()[0] == sorted(expected) and listed(trash=True)[0] == []
            stop_memory.set()
            memory_thread.join(timeout=1)
            latency_summary = {}
            for label in ("upload", "rename", "trash", "recover"):
                values = sorted(entry["seconds"] for entry in timings if entry["operation"] == label)
                assert values
                latency_summary[label] = dict(requests=len(values), median_ms=statistics.median(values) * 1000,
                    p95_ms=values[(len(values) * 95 + 99) // 100 - 1] * 1000, max_ms=values[-1] * 1000)
            report["stress"]["request_latency"] = latency_summary
            return {"uploads_acknowledged_and_verified": 64, "rename_trash_recover_workflows": 24,
                    "mutation_acknowledgements": 72, "retry_responses": len(retries),
                    "explicit_healing_cycles": len(healing_results), "live_listing_pages": pages,
                    "verified_public_gets_both_nodes": 128,
                    "unchanged_immutable_files": len(before_mutation),
                    "phase_elapsed_seconds": dict(upload=upload_elapsed, mutation=mutation_elapsed,
                                                  verification=time.monotonic() - verification_begin),
                    "max_engine_rss_kib": peak_rss_kib, "request_latency": latency_summary,
                    "scope": "finite concurrency correctness test, not a latency or scale guarantee"}
        case("bounded upload and namespace churn overlap healing without acknowledged-data loss", bounded_concurrent_stress)

        def immutable_unchanged():
            for path, digest in immutable_snapshot.items():
                assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, path
            content = request(bases[0], "/noa/v1/objects/get/tokens/source.bin")[1]
            assert content == b"NCNX key coordination fixture\n", content[:200]
            return {"unchanged_immutable_files": len(immutable_snapshot)}
        case("namespace-only operations preserve immutable metadata payloads and public GET", immutable_unchanged)
        report["status"] = "PASS"
    except BaseException as error:
        report.update(status="FAIL", error=repr(error))
        raise
    finally:
        for signal in background_stops:
            signal.set()
        barrier.release()
        pool.shutdown(wait=True, cancel_futures=True)
        for index in list(processes):
            stop(index)
        if proxy is not None:
            proxy.shutdown()
            proxy.server_close()
        for handle in logs:
            handle.close()
        (root / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        (root / "peer-timeline.json").write_text(json.dumps(barrier.timeline, indent=2) + "\n")
        print(f"{report['status']}: {len(results)} cases; evidence retained at {root}", flush=True)


if __name__ == "__main__":
    main()
