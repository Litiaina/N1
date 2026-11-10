# Contribution policy

The N1 repository contains the N1 object store and M1 metadata store and is
open source under the [Apache License 2.0](LICENSE). The project does
not accept external code contributions, documentation contributions or pull
requests. Please do not submit a PR unless the maintainer changes this policy.

The license still permits use, modification and redistribution under its terms;
this policy concerns what is accepted into this repository, not those rights.

## Bug reports

Use the repository's issue tracker. Include:

- Affected system (N1, M1 or both), version/build, operating system and pool geometry.
- Minimal reproduction steps, expected behavior and actual behavior.
- Relevant sanitized logs and, for performance reports, sample counts,
  latency, workload size, client/network location and memory measurements.

Remove credentials, tokens, private keys, personal paths and private data.
Do not publish sensitive security details or secrets in a public issue.

Feature requests may be reported for discussion, but do not imply acceptance
or a commitment to implement them.

## Internal maintenance

Use **N1** for the object store, **M1** for the metadata store, **LINK** for
N1's key algorithm and storage architecture, and **M1 storage** for M1's own
transactional database. Source directories describe responsibilities: `object_engine/`,
`metadata_engine/` and `link/`. The architecture guide maps their modules.

Run `cargo fmt --all --check` before a checkpoint. Keep banners, JSON responses
and asynchronous control-flow macros on readable lines; `rustfmt` does not
expand every macro body. Preserve operator controls and established module
responsibilities. A naming change must update Cargo targets, imports, tools,
tests and operator documentation together.
