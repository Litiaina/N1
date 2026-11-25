# System releases and component versions

Litiaina publishes one system release with one Git tag. The release selects
a tested combination of N1, M1 and Console. N1 and M1 each report their own
binary version.

## RC.2 release versions

| System or component | Version | Source of truth |
| --- | --- | --- |
| Complete system | `1.0.0-RC.2` | [`deploy/system-release.json`](../deploy/system-release.json) |
| N1 object store | `1.0.0-RC.2` | [`object_engine/Cargo.toml`](../object_engine/Cargo.toml) |
| M1 metadata store | `1.0.0-RC.1` | [`metadata_engine/Cargo.toml`](../metadata_engine/Cargo.toml) |

M1 starts at RC.1 because it has no previous release. N1 and the complete
system retain their RC.2 numbering. Historical reports keep the versions
and hashes of the exact binaries they tested. The system download and
artifact manifest are listed in [GitHub Releases](https://github.com/Litiaina/N1/releases).

System RC.2 uses the Git tag `v1.0.0-RC.2`. The artifact manifest records
the component versions, source commits and binary SHA-256 checksums.

## Component versions

Each Cargo package declares its own version. There is no shared workspace
version. Each binary's `--version`, startup banner and existing API version
fields report its package version. N1's backup helper belongs to the `n1`
package and reports N1's version.

A change confined to M1 can advance M1 while the next system release reuses
the qualified N1 binary. A change confined to N1 can do the same with M1.
Qualify the affected interactions before publishing the new combination.

Shared libraries and operator tools also have explicit Cargo versions. They
are not additional storage products or proxies for the system release number.
M1 storage starts at `1.0.0-RC.1`; its package version and database format
version are distinct. Change library package versions according to their own
changes rather than bumping every crate for an unrelated component release.

Keep the existing `MAJOR.MINOR.PATCH-RC.N` spelling for current candidates.
Advance candidate numbers within the affected component's release series.
After stable releases, use major/minor/patch changes for compatibility breaks,
compatible features and compatible fixes respectively. Published versions and
tags identify immutable artifacts; do not replace their contents with a new
build carrying the same release identity.

## Complete system release

`deploy/system-release.json` records the intended system version, component
versions, the system tag and packaged server/helper binaries. It is packaging and
qualification metadata. The manifest regression checks that its component
versions and executable names match Cargo's actual package metadata.

Before publishing a complete system release:

1. Select and freeze the component versions in the manifest. Record any reused
   qualified binary without rebuilding it merely to match the system number.
2. Qualify that exact combination through standalone N1, M1 → N1 and the
   supported Console workflows, closing the required release gates.
3. Preserve a final artifact manifest with the system/component versions,
   component source commits, build/toolchain identity, executable SHA-256 values,
   supported API/storage contracts and selected Console source/build identity.
   Keep private infrastructure identifiers out of published metadata.
4. Verify each shipped executable's `--version` against its selected package
   version. Publish one system tag for the qualified combination on release
   authorization.

A subsequent system release can select a newer M1 and the same N1, or vice
versa. Its manifest must identify that tested pairing. Do not move an existing
system tag to change the component selection. Console belongs to its own
repository; the final system artifact manifest must identify the tested
Console build rather than assigning it N1's or M1's version implicitly.

## Protocol and disk-format compatibility

NOA v1/v2, native protocol identifiers, LINK encoding/placement versions and
M1's database/snapshot formats describe their own contracts. They do not follow
binary or system RC numbers. A package-version change alone does not alter a
key, directory layout, upload identity, pool marker or stored database record.

Record compatibility explicitly in release evidence. Preserve the existing
protocol and storage checks when changing those contracts; do not infer format
compatibility from equal application versions or reject a different component
version merely because it differs from the system version.
