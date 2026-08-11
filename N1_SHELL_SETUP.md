# N1 Shell Installation Guide

This guide provisions one fresh Debian Stable N1 server through the embedded
browser terminal in `litiaina-n1-shell`. It covers the four-disk N1 topology
used for the initial live proof: every data disk is both an index root and a
blob root, and the static erasure layout is `3 + 1`.

The Shell is an installation and operations control plane. It does not replace
the N1 engine, expose unrestricted Bash, or silently start storage services.
The N1 service remains disabled until the operator explicitly validates and
starts it.

## Safety Contract

`disk format` destroys existing partition and filesystem signatures on the
selected disk. The Shell protects the installation by enforcing all of these
rules:

- Debian root, boot, EFI, and swap devices are classified as `protected`.
- A protected, read-only, or mounted disk cannot enter the formatting path.
- Device names such as `/dev/sdb` are not treated as persistent identity. The
  Shell also displays the hardware serial and `/dev/disk/by-id` identity.
- Formatting requires a confirmation containing both the requested NDISK
  label and the currently observed device serial.
- Every configuration replacement is written to a new file, synchronized,
  and atomically renamed into place.
- Disk assignment and EC changes are rejected while N1 is running.
- `n1 start` always performs complete configuration and topology validation.

The Shell does not perform a full-disk overwrite, secure erase, firmware erase,
or destructive media test. It creates one GPT partition and one XFS filesystem
only on the explicitly confirmed disk.

## Before Starting the Browser Terminal

Install the Debian packages used by the provisioning path:

```bash
sudo apt update
sudo apt install xfsprogs util-linux fdisk
```

Build the N1 engine and Shell:

```bash
cargo build --release -p n1 -p n1_shell
```

Create the installation directory and place the engine plus existing TLS
certificate there. This packaging step happens before browser provisioning;
the browser terminal deliberately does not accept arbitrary file uploads.

```bash
sudo install -d -o administrator -g administrator /opt/litiaina/n1
sudo install -m 0755 target/release/litiaina-n1 /opt/litiaina/n1/litiaina-n1
sudo install -m 0644 certificates/cert.pem /opt/litiaina/n1/cert.pem
sudo install -m 0600 certificates/key.pem /opt/litiaina/n1/key.pem
```

For a server that previously ran N2, stop and disable every N2 engine node and
the N2 gateway before disk preparation. Confirm that no N2 process has any of
the four test disks mounted or open. N1 Shell never manipulates N2 services.

## Start N1 Shell

Run the Shell as root because disk formatting, persistent mounts, ownership,
and systemd installation require root privileges:

```bash
sudo target/release/litiaina-n1-shell serve \
  --bind 0.0.0.0:50901 \
  --install-dir /opt/litiaina/n1 \
  --engine-binary /opt/litiaina/n1/litiaina-n1 \
  --mount-root /mnt \
  --run-user administrator \
  --run-group administrator
```

The process prints a random setup token. Open the server in a browser:

```text
http://<server-ip>:50901
```

Enter the printed token. Plain HTTP is intentionally limited to a trusted
installation LAN. Do not expose this setup endpoint to the internet, and stop
the Shell after provisioning.

Verify every Debian command required by the installation path:

```text
administrator@n1:unconfigured$ prerequisites
```

Continue only when the result is `Installation prerequisites: PASS`.

## Inspect the Machine

List physical disks:

```text
administrator@n1:unconfigured$ lsblk
```

Inspect the Debian system disk and confirm `System disk: yes` plus
`Eligibility: protected`:

```text
administrator@n1:unconfigured$ disk inspect /dev/nvme0n1
```

Inspect each of the four data disks. Before formatting, they must be the disks
you intend to discard and must not be mounted:

```text
administrator@n1:unconfigured$ disk inspect /dev/sda
administrator@n1:unconfigured$ disk inspect /dev/sdb
administrator@n1:unconfigured$ disk inspect /dev/sdc
administrator@n1:unconfigured$ disk inspect /dev/sdd
```

Device names above are examples. Use the paths shown by `lsblk` on the actual
server. Match the size, model, serial, and stable ID—not the letter alone.

## Initialize the Disabled N1 Installation

Initialize machine-local configuration, generated secrets, the control root,
and the disabled systemd unit:

```text
administrator@n1:unconfigured$ n1 init
```

This command is restart-safe: an existing `n1.env` is preserved, and N1 is not
started or enabled.

## Format the Four Data Disks

Assign one stable NDISK label to each verified device:

```text
administrator@n1:incomplete$ disk format /dev/sda NDISK001
administrator@n1:incomplete$ disk format /dev/sdb NDISK002
administrator@n1:incomplete$ disk format /dev/sdc NDISK003
administrator@n1:incomplete$ disk format /dev/sdd NDISK004
```

Each command prints the exact confirmation required, for example:

```text
Type 'FORMAT NDISK001 WD-WCC7K...' to continue:
> FORMAT NDISK001 WD-WCC7K...
```

Do not reuse the sample serial. Type the value produced for that exact disk.
Successful formatting creates:

- one GPT data partition;
- one XFS filesystem;
- a filesystem UUID and `NDISKxxx` label;
- a persistent UUID-based `/etc/fstab` entry;
- `/mnt/NDISKxxx` with N1 service ownership.

Run `lsblk` again. All four disks should be mounted with their NDISK labels.

## Assign N1 Roles

For the four-disk single-server deployment, every disk holds both namespace
control/index data and immutable blob data:

```text
administrator@n1:incomplete$ disk assign NDISK001 both
administrator@n1:incomplete$ disk assign NDISK002 both
administrator@n1:incomplete$ disk assign NDISK003 both
administrator@n1:incomplete$ disk assign NDISK004 both
```

Assignment changes only `n1_config.ini`. It never moves or redistributes
objects.

## Configure N1

Set the static `3 + 1` EC layout:

```text
administrator@n1:incomplete$ config ec 3 1
```

Bind the N1 HTTPS and native listeners:

```text
administrator@n1:incomplete$ config endpoint 0.0.0.0 50001 50002
```

Point N1 at the packaged certificate and private key:

```text
administrator@n1:incomplete$ config tls /opt/litiaina/n1/cert.pem /opt/litiaina/n1/key.pem
```

Review the complete draft:

```text
administrator@n1:incomplete$ config show
```

Validate every prerequisite:

```text
administrator@n1:incomplete$ config validate
```

The expected terminal result includes:

```text
Validation: PASS
EC: 3 + 1
Index roots: 4
Blob roots: 4
Unique mounted disks: 4
```

Validation checks the engine binary, service identity, environment secrets,
TLS files, control root, XFS mounts, distinct physical disks, disk assignments,
and the number of blob roots required by EC.

## Retrieve Initial Access Keys

While connected through the one-time setup token, display the generated N1
access keys:

```text
administrator@n1:stopped$ secrets show
```

Copy the values into the appropriate protected client or Console
configuration. Do not leave them in screenshots, chat, or browser storage.

## Start and Enable N1

Start the engine only after validation passes:

```text
administrator@n1:stopped$ n1 start
administrator@n1:active$ n1 status
administrator@n1:active$ logs 100
```

Enable automatic startup only after the live health check is satisfactory:

```text
administrator@n1:active$ n1 enable
```

Stopping, changing the topology, validating, and restarting remains explicit:

```text
administrator@n1:active$ n1 stop
administrator@n1:stopped$ config validate
administrator@n1:stopped$ n1 start
```

## End the Setup Session

After N1 and its clients are verified:

1. Copy the required generated keys securely.
2. Close the browser tab.
3. Stop `litiaina-n1-shell serve` with `Ctrl+C` in its host terminal.
4. Confirm port `50901` is no longer listening.

The N1 engine continues independently under `litiaina-n1.service`. The setup
HTTP terminal is not required for storage operation.

## Failure Recovery

- If the connection drops, reconnect with the setup token. The Shell never
  repeats the previous command automatically.
- If formatting fails, use `lsblk` and `disk inspect` to observe the actual
  state. If XFS exists but is not mounted, fix the reported operating-system
  error and run `disk mount <NDISKxxx>`; do not needlessly reformat it. Repeat
  `disk format` only when no initialized N1 filesystem was created.
- If configuration validation fails, N1 remains stopped. Correct only the
  reported setting and run `config validate` again.
- If startup fails after validation, use `n1 status` and `logs 200`. Do not
  reformat disks as a response to an engine startup error.
- A configuration write interrupted before its atomic rename leaves the last
  committed configuration intact.

## Command Reference

The browser terminal supports `help` as its authoritative compact reference:

```text
help
clear
version
prerequisites
lsblk
disk inspect <device-or-label>
disk format <device> <NDISKxxx>
disk mount <NDISKxxx>
disk assign <NDISKxxx> <index|blob|both>
disk unassign <NDISKxxx> <index|blob|both>
config ec <data> <parity>
config endpoint <host> <https-port> <native-port>
config tls <certificate-path> <private-key-path>
config show
config validate
secrets show
n1 init
n1 start
n1 stop
n1 restart
n1 enable
n1 disable
n1 status
logs [count]
```
