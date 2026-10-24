#!/usr/bin/env python3
"""Isolated real-process upload-ownership/targeted-cleanup proof.

Uses a new project-HDD fixture, two child engine processes, actual HTTP/native
peer transport, and deliberately paused streaming. No services, deployment data,
remote machines, formatting, or filesystem-wide cleanup are used.
"""

import argparse
import base64
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

HDD = Path("/mnt/PROJECT-DISK-1TB")
ENDPOINT = "/n2/peer/upload-ownership"


def ulid():
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    value = (int(time.time() * 1000) << 80) | secrets.randbits(80)
    result = ""
    for _ in range(26):
        result = alphabet[value & 31] + result
        value >>= 5
    return result


def pack(value):
    """Only the standard MessagePack types needed by the native peer frame."""
    if value is None:
        return b"\xc0"
    if isinstance(value, bool):
        return b"\xc3" if value else b"\xc2"
    if isinstance(value, int):
        if value < 128:
            return bytes([value])
        return b"\xcf" + struct.pack(">Q", value)
    if isinstance(value, str):
        data = value.encode()
        return (bytes([0xA0 | len(data)]) if len(data) < 32 else b"\xdb" + struct.pack(">I", len(data))) + data
    if isinstance(value, list):
        assert len(value) < 16
        return bytes([0x90 | len(value)]) + b"".join(pack(item) for item in value)
    raise AssertionError(f"unsupported native fixture value: {type(value)}")


def unpack(data):
    offset = 0

    def read(count):
        nonlocal offset
        result = data[offset:offset + count]
        assert len(result) == count
        offset += count
        return result

    def value():
        first = read(1)[0]
        if first < 128:
            return first
        if first >= 224:
            return first - 256
        if first in (0xC0, 0xC2, 0xC3):
            return {0xC0: None, 0xC2: False, 0xC3: True}[first]
        if 0xA0 <= first <= 0xBF:
            return read(first & 31).decode()
        if 0x90 <= first <= 0x9F:
            return [value() for _ in range(first & 15)]
        if 0x80 <= first <= 0x8F:
            return {value(): value() for _ in range(first & 15)}
        if first in (0xCC, 0xCD, 0xCE, 0xCF):
            return int.from_bytes(read({0xCC: 1, 0xCD: 2, 0xCE: 4, 0xCF: 8}[first]), "big")
        if first in (0xD9, 0xDA, 0xDB):
            size = int.from_bytes(read({0xD9: 1, 0xDA: 2, 0xDB: 4}[first]), "big")
            return read(size).decode()
        if first in (0xC4, 0xC5, 0xC6):
            size = int.from_bytes(read({0xC4: 1, 0xC5: 2, 0xC6: 4}[first]), "big")
            return read(size)
        if first in (0xDC, 0xDD):
            size = int.from_bytes(read(2 if first == 0xDC else 4), "big")
            return [value() for _ in range(size)]
        raise AssertionError(f"unsupported native frame tag: {first:#x}")

    result = value()
    assert offset == len(data), "native fixture decoder did not consume the complete frame"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    engine, out = args.engine.resolve(strict=True), args.out.resolve()
    assert engine.is_relative_to(HDD.resolve()) and out.is_relative_to(HDD.resolve())
    out.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="n2-upload-lifecycle-", dir=out))
    print(f"Evidence: {root}", flush=True)
    ports = []
    while len(ports) < 4:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in ports:
            ports.append(port)
    bases = [f"https://127.0.0.1:{ports[0]}", f"https://127.0.0.1:{ports[2]}"]
    context = ssl._create_unverified_context()  # disposable self-signed loopback fixture only
    secret, token, fragment = secrets.token_hex(32), "", ""
    cluster_id = "isolated-upload-lifecycle-proof"
    processes, logs, results, sockets = {}, [], [], []
    workers = concurrent.futures.ThreadPoolExecutor(max_workers=6)
    report = dict(status="RUNNING", scope="two real processes, one project HDD, loopback HTTP/native",
                  limitations=["not physical power-loss qualification", "not independent failure domains",
                               "not scale or latency qualification"], cases=results)

    def request(index, path, method="GET", data=None, expected=(200, 201, 202, 204), peer=False, auth=True):
        headers = {}
        if token and auth:
            headers["Authorization"] = f"Bearer {token}"
        if peer:
            headers["x-n2-cluster-secret"] = secret
        if isinstance(data, (dict, list)):
            headers["Content-Type"] = "application/json"
            data = json.dumps(data).encode()
        req = urllib.request.Request(bases[index] + path, data=data, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(req, context=context, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            assert response.status in expected, (path, response.status, body[:600])
            return response.status, body

    def api(index, path, method="GET", data=None, **kwargs):
        status, body = request(index, path, method, data, **kwargs)
        return json.loads(body) if body else dict(http_status=status)

    def wait_for(predicate, description, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.025)
        raise AssertionError(f"timed out: {description}")

    def start(index):
        handle = open(root / f"node-{index}.log", "ab")
        logs.append(handle)
        env = dict(os.environ, TMPDIR=str(root), N2_RUNTIME_WORKER_THREADS="4",
                   N2_RUNTIME_MAX_BLOCKING_THREADS="16", RUST_LOG="info")
        processes[index] = subprocess.Popen([str(engine)], cwd=root / f"node-{index}", env=env,
                                            stdout=handle, stderr=subprocess.STDOUT)

        def ready():
            assert processes[index].poll() is None, f"node {index} exited"
            try:
                request(index, "/noa/v1/ping", auth=False)
                return True
            except (AssertionError, OSError):
                return False
        wait_for(ready, f"node {index} readiness", timeout=45)

    def stop(index):
        process = processes.pop(index, None)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=10)

    def case(name, function):
        begin = time.monotonic()
        detail = function()
        results.append(dict(name=name, status="PASS", duration_seconds=time.monotonic() - begin,
                            details=detail))
        print(f"PASS {name}", flush=True)

    def ownership(index, identity, action="inspect", expected=(200,), **changes):
        payload = dict(action=action, target_slot_id=f"node-{index}", target_instance_id=f"node-{index}",
                       binding_epoch=1, identity=identity)
        payload.update(changes)
        return api(index, ENDPOINT, "POST", payload, expected=expected, peer=True, auth=False)

    def identity():
        return dict(cluster_id=cluster_id, pool_id="proof", pool_generation=1, fragment=fragment,
                    object_id=ulid(), version_id=ulid(), session_id=ulid(), data_shards=1,
                    parity_shards=1, created_at_unix=int(time.time()))

    def shard_path(index, owned, part=0):
        return "/n2/peer/shards/{fragment}/{object_id}/{version_id}/{part}/{index}?{query}".format(
            **owned, part=part, index=index, query=urllib.parse.urlencode(dict(
                pool_id="proof", pool_generation=1, slot_id=f"node-{index}", instance_id=f"node-{index}",
                binding_epoch=1, root_id=f"root-{index}", parity=str(index == 1).lower(),
                expected_len=4096, deferred_durability="false")))

    def payload_files(index, owned):
        disk = root / f"node-{index}" / "disk" / f"@f_{fragment}" / ".objects"
        return [path for directory in disk.rglob(owned["version_id"]) if directory.is_dir()
                for path in directory.rglob("*") if path.is_file()]

    def fence_file(index, owned):
        digest = hashlib.sha256(owned["version_id"].encode()).digest()
        return root / f"node-{index}" / "disk" / f"@f_{fragment}" / ".uploads/.fences" / \
            f"{digest[30]:02x}" / f"{digest[31]:02x}" / f"{owned['version_id']}.n2"

    def assert_terminal_fence(index, owned, state):
        observed = ownership(index, owned)["record"]
        assert observed is not None and observed["state"] == state, observed
        stored = json.loads(fence_file(index, owned).read_text())
        assert stored["record"]["identity"] == owned and stored["record"]["state"] == state, stored

    def open_both(owned):
        for index in range(2):
            ownership(index, owned, "open")

    def abort_both(owned):
        for index in range(2):
            ownership(index, owned, "abort")

    def cleanup_both(owned):
        # These are identities of real independently persisted votes, never
        # an unsigned assertion that a vote happened. Receivers must verify.
        for index in range(2):
            ownership(index, owned, "cleanup", abort_voters=["node-0", "node-1"])

    def ordinary_put(index, owned, expected=(201,)):
        return request(index, shard_path(index, owned), "PUT", b"\x01" * 4096,
                       peer=True, auth=False, expected=expected)

    def native_put(index, owned, expected_ok):
        payload = dict(secret=secret, pool_id="proof", pool_generation=1, slot_id=f"node-{index}",
                       instance_id=f"node-{index}", binding_epoch=1, root_id=f"root-{index}",
                       fragment=fragment, object_id=owned["object_id"], version_id=owned["version_id"],
                       part_index=0, shard_index=index, parity=index == 1, expected_len=4096,
                       offset=None, length=None, deferred_durability=False)
        control = pack(["lifecycle-proof-" + ulid(), "peer.shard.put", json.dumps(payload), None, 4096])
        with socket.create_connection(("127.0.0.1", ports[index * 2 + 1]), timeout=15) as sock:
            sock.sendall(struct.pack(">I", len(control)) + control + b"\x01" * 4096)
            sock.shutdown(socket.SHUT_WR)
            def read_exact(size):
                chunks = bytearray()
                while len(chunks) < size:
                    chunk = sock.recv(size - len(chunks))
                    assert chunk, "native response ended early"
                    chunks.extend(chunk)
                return bytes(chunks)
            size = struct.unpack(">I", read_exact(4))[0]
            assert 0 < size <= 8 * 1024 * 1024
            response = unpack(read_exact(size))
            assert isinstance(response, list) and response[1] is expected_ok, response
            return response

    def metadata_for(owned):
        shards = []
        for index in range(2):
            parity = index == 1
            locator = "n2-peer-v3|proof|1|node-{index}|root-{index}|{fragment}|{object_id}|{version_id}|0|{index}|{parity}".format(
                **owned, index=index, parity=str(parity).lower())
            shards.append(dict(shard_index=index, file_name=f"part_0_shard_{index}" + (".parity" if parity else ".shard"),
                               absolute_path=locator, size_bytes=4096,
                               checksum=hashlib.sha256(b"\x01" * 4096).hexdigest(), is_parity=parity))
        return dict(object_key=owned["object_id"], packed_ext_u64=0, fragment=fragment,
                    version_id=owned["version_id"], upload_session_id=owned["session_id"], owner_uid=fragment,
                    content_type="application/octet-stream", total_size_bytes=4096, created_at=owned["created_at_unix"],
                    pool_id="proof", pool_generation=1, membership_epoch=1, data_shards=1, parity_shards=1,
                    parts=[dict(part_index=0, size_bytes=4096, created_at=owned["created_at_unix"], shards=shards)],
                    is_delete_marker=False, stripe_size_bytes=4096)

    def publish_metadata(index, metadata, expected=(201,)):
        return request(index, "/n2/peer/objects/publish", "POST", dict(
            target_slot_id=f"node-{index}", target_instance_id=f"node-{index}", binding_epoch=1,
            metadata=metadata, sync_shards_before_publish=False), peer=True, auth=False, expected=expected)

    try:
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", str(root / "key.pem"), "-out", str(root / "cert.pem"),
                        "-days", "1", "-subj", "/CN=localhost"], check=True, capture_output=True)
        nodes = []
        for index in range(2):
            cwd = root / f"node-{index}"
            (cwd / "state").mkdir(parents=True)
            (cwd / "disk").mkdir()
            nodes.append(dict(slot_id=f"node-{index}", instance_id=f"node-{index}", binding_epoch=1,
                              endpoint=bases[index], native_endpoint=f"n2://127.0.0.1:{ports[index*2+1]}",
                              state="online", weight=1, joined_epoch=1,
                              storage_roots=[dict(id=f"root-{index}", path=str(cwd / "disk"))]))
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
node_id=node-{index}
pool_id=proof
pool_generation=1
node_endpoint={bases[index]}
native_endpoint=n2://127.0.0.1:{ports[index*2+1]}
membership_file={cwd}/state/n2_membership.json
object_data_shards=1
object_parity_shards=1
auto_rebalance=false
[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=2
retention_seconds=3600
[auto_healing]
enabled=false
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
                          pools=[dict(id="proof", generation=1, state="writable", slot_ids=["node-0", "node-1"],
                                      policy=dict(object_data_shards=1, object_parity_shards=1))],
                          slot_bindings=nodes, reserve_nodes=[], retired_instances=[])
        for index in range(2):
            (root / f"node-{index}" / "state/n2_membership.json").write_text(json.dumps(membership))
        for index in range(2):
            start(index)
        created = api(0, "/noa/v1/fragments", "POST", dict(name="upload-lifecycle", secret=secret, key=secret))
        token = api(0, "/noa/v1/auth/fragment", "POST", dict(fragment=created["fragment_hash"], secret=secret,
                    read=True, write=True, modify=True, delete=True, expires_in=3600))["access_token"]
        claims = token.split(".")[1]
        fragment = json.loads(base64.urlsafe_b64decode(claims + "=" * (-len(claims) % 4)))["fragment"]

        def slow_stream_vs_abort():
            owned = identity()
            open_both(owned)
            connection = http.client.HTTPSConnection("127.0.0.1", ports[2], timeout=20, context=context)
            sockets.append(connection)
            connection.putrequest("PUT", shard_path(1, owned))
            connection.putheader("x-n2-cluster-secret", secret)
            connection.putheader("Content-Length", "4096")
            connection.endheaders()
            connection.send(b"x" * 1024)
            wait_for(lambda: bool(payload_files(1, owned)), "real peer upload admitted its temporary file")
            aborting = workers.submit(ownership, 1, owned, "abort")
            time.sleep(0.15)
            assert not aborting.done(), "abort bypassed an admitted streaming shard writer"
            connection.send(b"x" * 3072)
            response = connection.getresponse()
            assert response.status == 201, (response.status, response.read())
            response.read()
            connection.close()
            aborting.result(timeout=15)
            ownership(0, owned, "abort")
            assert payload_files(1, owned), "abort vote alone cannot remove physical payload"
            cleanup_both(owned)
            assert all(not payload_files(index, owned) for index in range(2))
            for index in range(2):
                assert_terminal_fence(index, owned, "aborted")
                ordinary_put(index, owned, expected=(409,))
                native_put(index, owned, expected_ok=False)
                publish_metadata(index, metadata_for(owned), expected=(409,))
            ownership(1, owned, "open", expected=(409,))
            stop(1)
            start(1)
            ownership(1, owned, "open", expected=(409,))
            ordinary_put(1, owned, expected=(409,))
            native_put(1, owned, expected_ok=False)
            publish_metadata(1, metadata_for(owned), expected=(409,))
            return dict(stream_writer_drained_before_abort=True, physical_payload_reclaimed=True,
                        delayed_http_and_native_writes_rejected=True, permanent_abort_survived_restart=True)
        case("real streaming PUT drains before durable abort and stale writers remain fenced after restart", slow_stream_vs_abort)

        def insufficient_certificate():
            owned = identity()
            open_both(owned)
            ordinary_put(0, owned)
            ordinary_put(1, owned)
            ownership(0, owned, "abort")
            before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                      for index in range(2) for path in payload_files(index, owned)}
            ownership(0, owned, "cleanup", abort_voters=["node-0", "node-1"], expected=(409, 503))
            ownership(0, owned, "cleanup", abort_voters=["node-0", "node-0"], expected=(400, 409, 503))
            assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest
                       for path, digest in before.items())
            ownership(1, owned, "abort")
            cleanup_both(owned)
            assert all(not payload_files(index, owned) for index in range(2))
            return dict(unsigned_or_duplicate_votes_cannot_authorize_delete=True)
        case("cleanup verifies distinct durable abort votes rather than trusting caller claims", insufficient_certificate)

        def missing_enrolled_root():
            owned = identity()
            open_both(owned)
            ordinary_put(0, owned)
            ordinary_put(1, owned)
            mountpoint = root / "node-1/disk"
            detached = root / "node-1/disk-detached"
            mountpoint.rename(detached)
            mountpoint.mkdir()
            try:
                ownership(1, owned, "abort", expected=(409, 503))
                assert list(mountpoint.iterdir()) == [], "abort wrote onto an unenrolled empty mountpoint"
            finally:
                mountpoint.rmdir()
                detached.rename(mountpoint)
            assert ownership(1, owned)["record"]["state"] == "open"
            abort_both(owned)
            cleanup_both(owned)
            return dict(missing_root_cannot_vote_absence_or_create_fences=True, original_payload_retained_until_valid_abort=True)
        case("an empty mountpoint cannot impersonate an enrolled upload holder", missing_enrolled_root)

        def committed_protection():
            content = hashlib.sha256(b"committed ownership is not namespace reachability").digest() * 256
            uploaded = api(0, "/noa/v1/upload/one-shot/committed/source.bin", "POST", content)
            inventory = api(0, "/n2/peer/objects/inventory", "POST", dict(fragment=fragment), peer=True, auth=False)
            matching = [metadata for metadata in inventory if metadata["version_id"] == uploaded["version_id"]]
            assert len(matching) == 1, matching
            metadata = matching[0]
            owned = dict(cluster_id=cluster_id, pool_id=metadata["pool_id"],
                         pool_generation=metadata["pool_generation"], fragment=fragment,
                         object_id=metadata["object_key"], version_id=metadata["version_id"],
                         session_id=metadata["upload_session_id"], data_shards=metadata["data_shards"],
                         parity_shards=metadata["parity_shards"], created_at_unix=metadata["created_at"])
            before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                      for index in range(2) for path in payload_files(index, owned)}
            assert before
            # Exercise production verified GET -> repair -> native/HTTP peer
            # admission, not a mock peer that bypasses lifecycle guards.
            # Placement rotates node roles. Select the recorded DATA piece,
            # not a fixed node (which may contain unread parity for this ID).
            data_piece = next(piece for piece in metadata["parts"][0]["shards"] if not piece["is_parity"])
            targets = [path for index in range(2) for path in payload_files(index, owned)
                       if path.name == data_piece["file_name"]]
            assert len(targets) == 1, targets
            damaged = targets[0]
            original = damaged.read_bytes()
            damaged.write_bytes(bytes([original[0] ^ 1]) + original[1:])
            assert request(0, "/noa/v1/objects/get/committed/source.bin")[1] == content
            wait_for(lambda: damaged.is_file() and hashlib.sha256(damaged.read_bytes()).hexdigest() == before[str(damaged)],
                     "verified GET physically repaired bitrot through lifecycle fencing", timeout=30)
            # Isolated fixture-only simulation of a legacy committed version:
            # retain immutable NCDX, but remove this holder's new fence. Repair
            # must bootstrap from independently checked K metadata copies.
            data_holder = next(index for index in range(2) if damaged.is_relative_to(root / f"node-{index}"))
            fence_file(data_holder, owned).unlink()
            damaged.unlink()
            assert request(0, "/noa/v1/objects/get/committed/source.bin")[1] == content
            wait_for(lambda: damaged.is_file() and hashlib.sha256(damaged.read_bytes()).hexdigest() == before[str(damaged)],
                     "verified GET physically repaired missing piece through lifecycle fencing", timeout=30)
            assert_terminal_fence(data_holder, owned, "committed")
            for index in range(2):
                ownership(index, owned, "abort", expected=(409,))
                assert_terminal_fence(index, owned, "committed")
                ownership(index, owned, "cleanup", abort_voters=["node-0", "node-1"], expected=(409, 503))
                ordinary_put(index, owned, expected=(409,))
                native_put(index, owned, expected_ok=False)
                assert request(index, "/noa/v1/objects/get/committed/source.bin")[1] == content
            for index in range(2):
                physical = root / f"node-{index}" / "disk" / f"@f_{fragment}"
                pointer = physical / ".namespace/committed/source.bin.ncnx"
                assert pointer.is_file()
                pointer.unlink()  # fixture-only namespace loss cannot make committed NCDX an orphan
                ownership(index, owned, "abort", expected=(409,))
                ownership(index, owned, "cleanup", abort_voters=["node-0", "node-1"], expected=(409, 503))
            for path, digest in before.items():
                assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, path
            return dict(committed_version_protected_without_ncnx=True,
                        unchanged_immutable_files=len(before), ordinary_http_and_native_overwrite_rejected=True,
                        production_get_physically_repaired_bitrot_and_missing_piece=True,
                        legacy_committed_repair_bootstrapped_from_k_matching_metadata=True)
        case("committed NCDX protects immutable data even after every NCNX pointer disappears", committed_protection)

        def ncdx_boundary_before_namespace():
            owned = identity()
            open_both(owned)
            ordinary_put(0, owned)
            ordinary_put(1, owned)
            metadata = metadata_for(owned)
            for index in range(2):
                publish_metadata(index, metadata)
            for index in range(2):
                ownership(index, owned, "abort", expected=(409,))
                ownership(index, owned, "cleanup", abort_voters=["node-0", "node-1"], expected=(409, 503))
                matches = api(index, "/n2/peer/objects/resolve", "POST", dict(
                    fragment=fragment, object_id=owned["object_id"], version_id=owned["version_id"]),
                    peer=True, auth=False)
                assert len(matches) == 1 and matches[0]["version_id"] == owned["version_id"], matches
                assert payload_files(index, owned)
            return dict(ncdx_before_ncnx_is_protected=True, abort_without_namespace_refused=True)
        case("immutable NCDX publication commits ownership before logical namespace visibility", ncdx_boundary_before_namespace)

        def public_multipart_cancel():
            cancelled = []
            for durable in (False, True):
                prefix = "/noa/v1/upload/multipart" + ("/durable" if durable else "")
                family = "durable" if durable else "standard"
                initiated = api(0, prefix + f"/init/cancel/{family}.bin", "POST", dict(
                    total_size_bytes=8192, expected_parts=2, part_size_bytes=4096,
                    content_type="application/octet-stream"))
                version = initiated["version_id"]
                api(0, prefix + f"/{version}/0", "PUT", b"c" * 4096)
                candidates = list((root / "node-0/disk" / f"@f_{fragment}" / ".uploads").glob(
                    f"{version}/ownership.n2"))
                assert len(candidates) == 1, candidates
                owned = json.loads(candidates[0].read_text())["record"]["identity"]
                assert any(payload_files(index, owned) for index in range(2))
                request(0, prefix + f"/{version}?" + urllib.parse.urlencode(dict(object_key=f"cancel/{family}.bin")), "DELETE", expected=(200, 204))
                assert all(not payload_files(index, owned) for index in range(2))
                for index in range(2):
                    ownership(index, owned, "open", expected=(409,))
                    ordinary_put(index, owned, expected=(409,))
                    assert_terminal_fence(index, owned, "aborted")
                cancelled.append(family)
            return dict(public_cancelled_families=cancelled, exact_version_payload_removed=True,
                        ownership_fences_survive_session_cleanup=True)
        case("public standard and durable cancellation reclaim exact payload but preserve abort fences", public_multipart_cancel)

        def interrupted_one_shot_gc():
            uploads = root / "node-0/disk" / f"@f_{fragment}" / ".uploads"
            previous = {path.parent.name for path in uploads.glob("*/ownership.n2")}
            connection = http.client.HTTPSConnection("127.0.0.1", ports[0], timeout=20, context=context)
            sockets.append(connection)
            connection.putrequest("POST", "/noa/v1/upload/one-shot/abandoned/truncated.bin")
            connection.putheader("Authorization", f"Bearer {token}")
            connection.putheader("Content-Length", "8192")
            connection.endheaders()
            connection.send(b"incomplete body" * 70)
            wait_for(lambda: bool({path.parent.name for path in uploads.glob("*/ownership.n2")} - previous),
                     "one-shot registered durable ownership before streaming")
            paths = [path for path in uploads.glob("*/ownership.n2") if path.parent.name not in previous]
            assert len(paths) == 1, paths
            owned = json.loads(paths[0].read_text())["record"]["identity"]
            wait_for(lambda: any(payload_files(index, owned) for index in range(2)),
                     "interrupted one-shot created real temporary payload")
            connection.close()
            untouched = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                         for index in range(2)
                         for path in (root / f"node-{index}" / "disk" / f"@f_{fragment}" / ".objects").rglob("*")
                         if path.is_file() and owned["version_id"] not in path.parts}
            time.sleep(3)
            request(0, "/noa/v1/maintenance/gc/start?" + urllib.parse.urlencode(dict(fragment=fragment)), "POST", {})
            def reclaimed():
                states = [ownership(index, owned)["record"] for index in range(2)]
                return all(record is not None and record["state"] == "aborted" for record in states) \
                    and all(fence_file(index, owned).is_file() and not payload_files(index, owned) for index in range(2))
            wait_for(reclaimed, "explicit GC reclaimed the interrupted one-shot from ownership records", timeout=45)
            for path, digest in untouched.items():
                assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, path
            for index in range(2):
                assert_terminal_fence(index, owned, "aborted")
                ordinary_put(index, owned, expected=(409,))
            return dict(narrow_one_shot_orphan_reclaimed=True, permanent_abort_fences_retained=True,
                        unchanged_unrelated_files=len(untouched))
        case("explicit GC reclaims interrupted one-shot ownership without touching unrelated versions", interrupted_one_shot_gc)

        report["status"] = "PASS"
    except BaseException as error:
        report.update(status="FAIL", error=repr(error))
        raise
    finally:
        for connection in sockets:
            connection.close()
        workers.shutdown(wait=True, cancel_futures=True)
        for index in list(processes):
            stop(index)
        for handle in logs:
            handle.close()
        (root / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{report['status']}: {len(results)} cases; evidence retained at {root}", flush=True)


if __name__ == "__main__":
    main()
