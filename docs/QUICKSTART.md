# Run N1 locally

This example starts standalone N1 on loopback using a temporary 1+0 pool.
For redundant storage, TLS and optional named objects through M1, use the
[operator guide](../DOCUMENTATION.md).

Use `litiaina-n1` on your `PATH` from the
[Linux release](https://github.com/Litiaina/N1/releases/tag/v1.0.0-RC.2)
or run `cargo build --release --locked -p n1` and add `target/release`
to your `PATH`.

Run the following in a shell with OpenSSL available. Choose different ports
if 19001 or 19002 is already in use.

```sh
umask 077
demo="$(mktemp -d)"
mkdir "$demo/member"

for name in JWT_SECRET AUTH_KEY MASTER_KEY ADMIN_KEY; do
  printf '%s=%s\n' "$name" "$(openssl rand -hex 32)"
done > "$demo/n1.env"

cat > "$demo/n1_config.ini" <<EOF_INI
[server]
host=127.0.0.1
https_port=19001
native_port=19002
cert_path=
key_path=

[disk_config]
fs_storage_path=$demo/control

[storage_pool.A]
generation=1
roots=$demo/member
data_shards=1
parity_shards=0
EOF_INI

litiaina-n1 init --config "$demo/n1_config.ini"
printf 'Test directory: %s\n' "$demo"
litiaina-n1 serve --config "$demo/n1_config.ini"
```

In another terminal, check the running service:

```sh
curl -fsS http://127.0.0.1:19001/health
```

Use the native listener on port 19002 for uploads and reads. The
[N1 API guide](api/N1.md#native-rust-walkthrough) shows how to authenticate
with the generated `AUTH_KEY`, upload a payload, and read and delete it
using its LINK key.

Press Ctrl-C to stop N1. The test directory contains the pool, control
state and credentials. You can delete that directory when you finish.
