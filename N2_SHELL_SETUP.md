# N2 Shell Distributed Commissioning

N2 Shell commissions a physical N2 cluster without turning any machine into
a permanent controller. The same `litiaina-n2-shell serve` process runs
temporarily on every physical node. One browser session becomes the temporary
commissioner and sends typed provisioning requests to the other Shell agents.

N2 Shell is not Bash, namespace authority, membership consensus, or part of
the object data path. When commissioning finishes, the Shell processes can be
stopped. The engines, static server pools, reserve set, and optional gateways
then operate from the identical committed membership.

## Physical Model

For a four-node EC `3 + 1` pool:

```text
operator browser
  |
  `-- temporary commissioner on node-001:50902
        |-- typed agent API -> node-001:50902 -> local disk/service only
        |-- typed agent API -> node-002:50902 -> local disk/service only
        |-- typed agent API -> node-003:50902 -> local disk/service only
        `-- typed agent API -> node-004:50902 -> local disk/service only

pool-001 / generation 1 / EC 3+1
  |-- node-001 -> its local N2DISK001
  |-- node-002 -> its local N2DISK002
  |-- node-003 -> its local N2DISK003
  `-- node-004 -> its local N2DISK004
```

Each agent is the sole authority for its host's `lsblk`, formatting, mounts,
local runtime files, and systemd services. The commissioner never assumes a
remote disk exists locally and never operates remote hosts through SSH.

The Shell's fan-out is bounded parallel control-plane work. It does not add a
request-path lock, coordinator, database, or user-facing bottleneck. A million
clients still communicate with N2 engines or gateways; they never traverse
the commissioning Shell.

## Safety and Commit Contract

The distributed installer enforces these rules:

- root, boot, EFI, swap, mounted, and read-only devices cannot be formatted;
- formatting confirmation is bound to physical agent ID, N2 label, and the
  currently observed disk serial;
- assignment records the filesystem UUID, hardware serial, stable device ID,
  and capacity, then revalidates UUID and serial before every commit;
- one physical machine fingerprint is one failure domain by default;
- each writable pool contains exactly `K + M` distinct nodes;
- a node belongs to exactly one active pool or the reserve set;
- every node owns at least one locally mounted XFS root;
- endpoint, pool-generation, node-role, disk-label, and gateway ownership
  collisions are rejected before prepare;
- every agent independently renders and stages its local files from the same
  canonical cluster specification;
- the specification, shared environment, certificate, and key are bound to
  one SHA-256 deployment identity;
- staged file content is hashed again and checked before local commit;
- prepare failure aborts all successful stages;
- an interrupted commit is safe to repeat with the identical hash;
- no engine or gateway starts during prepare or commit;
- gateways start only after every required node reports active;
- an agent can control only its committed node service and its own optional
  gateway service.

A distributed filesystem cannot make simultaneous local renames across hosts
one physical atomic instruction. N2 Shell therefore uses the safer deployment
boundary: all nodes prepare first, local commits are idempotent, and no data
service activates until every node acknowledges the same hash. If a
commissioner loses connectivity during commit, rerunning the identical commit
converges the remaining agents without exposing a partially running cluster.

## Install the Binaries on Every Physical Node

On each Debian node:

```bash
sudo apt update
sudo apt install xfsprogs util-linux fdisk

cargo build --release -p n2 -p n2_shell -p n2_gateway
sudo install -m 0755 target/release/litiaina-n2 /usr/local/bin/litiaina-n2
sudo install -m 0755 target/release/litiaina-n2-shell /usr/local/bin/litiaina-n2-shell
sudo install -m 0755 target/release/litiaina-n2-gateway /usr/local/bin/litiaina-n2-gateway
```

The `litiaina` operating-system user and group must exist on every node.

## Start One Temporary Agent per Node

Run this on every physical node:

```bash
sudo litiaina-n2-shell serve \
  --bind 0.0.0.0:50902 \
  --engine-binary /usr/local/bin/litiaina-n2 \
  --gateway-binary /usr/local/bin/litiaina-n2-gateway \
  --run-user litiaina \
  --run-group litiaina
```

Each node prints its own agent ID and random setup token. Record which token
belongs to which physical address. The setup port is temporary:

- use a trusted, isolated provisioning LAN;
- do not publish it through an Internet reverse proxy;
- use a TLS-protected local proxy if the provisioning network is not trusted;
- do not enable `PrivateTmp`, `PrivateMounts`, or mount-namespace isolation on
  the Shell service because its local mounts must enter the host namespace;
- stop every Shell agent after commissioning.

Open one agent, for example `http://10.0.0.11:50902`, and authenticate with
that node's setup token. This browser is now the temporary commissioner.

## Initialize and Pair the Physical Nodes

```text
cluster init cluster-001

node pair node-001 http://10.0.0.11:50902 <node-001-token> 10.0.0.11 50001 50002
node pair node-002 http://10.0.0.12:50902 <node-002-token> 10.0.0.12 50001 50002
node pair node-003 http://10.0.0.13:50902 <node-003-token> 10.0.0.13 50001 50002
node pair node-004 http://10.0.0.14:50902 <node-004-token> 10.0.0.14 50001 50002

node list
```

Pairing tokens are redacted from terminal output and excluded from browser
command history. A successful pair replaces the setup token with a random
agent credential stored only in the root-owned commissioner draft and a hash
stored by the remote agent.

Protect and back up the root-owned commissioner state at
`/var/lib/litiaina/n2-setup/n2_cluster_commissioner.json` after commit. N2 does
not need this file to serve data, but the credentials and last committed plan
are required to commission a later topology epoch without physically resetting
and pairing every agent again. Never place this state in a public repository.

The advertised host is the address used by N2 peers and clients. Never use
`0.0.0.0` as an advertised address.

## Inspect, Format, and Assign Remote Disks

Inventory is explicitly scoped to a physical node:

```text
lsblk node-001
lsblk node-002
lsblk node-003
lsblk node-004

disk inspect node-001 /dev/sdb
disk format node-001 /dev/sdb N2DISK001
disk format node-002 /dev/sdb N2DISK002
disk format node-003 /dev/sdb N2DISK003
disk format node-004 /dev/sdb N2DISK004
```

Use the actual device identity reported by each node. Every format command
prints a distinct confirmation. Successful formatting creates one GPT data
partition, one XFS filesystem, a UUID-based `/etc/fstab` entry, and a mount at
`/mnt/N2DISKxxx` on that physical machine.

Bind the initialized roots to their local nodes:

```text
disk assign node-001 N2DISK001
disk assign node-002 N2DISK002
disk assign node-003 N2DISK003
disk assign node-004 N2DISK004
```

A node may own multiple disks. Disk counts do not need to match between
nodes, but operators should balance usable capacity and performance within a
pool. N2 counts nodes—not individual disks—as EC failure domains.

## Define Pools, Reserves, TLS, and Gateways

```text
pool create pool-001 1 3 1
pool enroll pool-001 node-001 node-002 node-003 node-004

config tls /opt/litiaina/n2/cert.pem /opt/litiaina/n2/key.pem
gateway add gateway-001 node-001 0.0.0.0 50111
```

Additional complete server-pool generations are represented independently:

```text
pool create pool-002 2 4 2
pool enroll pool-002 node-101 node-102 node-103 node-104 node-105 node-106
```

An optional unassigned physical instance can enter the cluster reserve set:

```text
reserve enroll node-r01
```

Multiple gateways may be placed on different physical nodes for independent
front doors. The Shell rejects more than one gateway service on the same
physical agent.

## Validate, Prepare, and Commit

```text
cluster show
cluster validate
cluster commit
```

Validation contacts every agent in parallel and prints the canonical topology
hash, physical-node count, distinct machine-fingerprint count, disk count,
pool count, reserve count, and gateway count. Commit requires the exact hash
shown in its confirmation.

The committed artifacts are node-local:

```text
/etc/litiaina/n2/n2.env
/var/lib/litiaina/n2/nodes/<local-node-id>/n2_config.ini
/var/lib/litiaina/n2/nodes/<local-node-id>/state/n2_membership.json
/var/lib/litiaina/n2/nodes/<local-node-id>/certificates/
/etc/systemd/system/litiaina-n2@.service
```

Every physical node receives byte-identical membership. Node configuration
contains only that node's endpoint, ports, runtime, and local storage roots.

## Activate and Observe

```text
cluster start
cluster status
logs node-001 100
logs node-002 100
logs gateway-001 100
```

`cluster start` starts node services in parallel, waits for every required
node to report active, and only then starts gateways. If any node does not
activate, reachable nodes are stopped and gateways are never started.

After health is correct:

```text
cluster enable
```

Stop the temporary `litiaina-n2-shell serve` process on every physical node
and confirm port `50902` is closed. The engine and gateway services continue
under systemd without Shell availability.

## Controlled Topology Evolution

To add another complete pool, reserve node, storage root, or gateway later,
start the agents again and open a new topology epoch:

```text
cluster evolve 2
```

The last committed topology remains the engine authority while the new draft
is edited. Pair new physical agents, add their disks and roles, then run the
same `cluster validate` and `cluster commit` sequence. Agents require their
local engine service to be stopped during prepare; no partially committed
epoch can activate. If the draft is abandoned before commit:

```text
cluster discard-draft
```

This restores the commissioner's last committed plan. It does not rewrite or
roll back running engine data.

## Laboratory Simulation

The production default rejects multiple node identities carrying the same
machine fingerprint. A single-host process simulation must be explicit:

```bash
sudo litiaina-n2-shell serve --allow-shared-failure-domain ...
```

This flag proves protocol, membership, routing, and restart behavior. It does
not manufacture independent power, controller, kernel, network, or rack
failure domains and must not be used to describe a production topology.

## Future Site and Federation Boundary

The same hierarchy extends without changing the engine data path:

```text
local agent -> physical node -> server pool/reserve set -> cluster
cluster -> site policy -> federation
```

Site pairing and geographic replication will be separate typed Shell modules
after the single-cluster engine contract is qualified. They must not turn the
commissioner into permanent authority, mutate object bytes through Shell, or
weaken the current static-pool and exact-layout laws.
