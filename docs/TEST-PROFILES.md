# Qualification hardware profiles and reporting

Public qualification reports identify hardware and deployment roles. They do
not identify privately operated machines, their owners or their networks.

| Profile | CPU and memory recorded in the reports | Storage and deployment |
| --- | --- | --- |
| Four-HDD reference system | Intel Core i5-11400, 6 cores / 12 threads, approximately 11 GiB RAM | Four independent HDD filesystems; immutable 3+1 or 2+2 fixtures; separate Kingston SNV2S500G NVMe for M1 in the named-service profile |
| SSD test system | Intel Core i5-12600K, 10 cores / 16 threads, approximately 31–32 GiB RAM | Intel SSDSC2KW240H6 SATA SSD; co-located N1 1+0/M1 measurements and M1's side of split-node tests, as specified by each report |
| Two-HDD test system | AMD Ryzen 5 PRO 4650U; memory and kernel details belong to each run's private manifest | Two ST1000LM049 HDDs over USB UAS; independent filesystems and 1+1 fixtures, with 1+0 controls where recorded |

These describe equipment and roles, not interchangeable benchmarks. A run's
actual filesystem, EC geometry, durability, cache conditions, software identity
and placement still govern what its result proves. Preserve the different
filesystems and transport failures recorded by earlier campaigns.

## Public and private evidence

Publish CPU/RAM, drive models and capacities, filesystem/kernel versions,
transport, topology, concurrency, durability, workload, sample counts,
measurements, failures and source/binary checksums. Describe competing load by
its measured resource use rather than identifying personal applications.

Keep hostnames, site labels, LAN addresses, SSH logins, personal paths, drive
serials, filesystem/boot IDs, credentials and raw process inventories in private
operator evidence. Private inventory must still distinguish physical devices
and failure domains; removing those identifiers from a report does not remove
the runtime's device-validation requirements.

Repository reports use generalized path and endpoint aliases. Paths under
`/srv/test-evidence`, private-evidence filenames and `*.example.invalid` names
are locators/placeholders, not actual server addresses or installation commands.
The operator retains the mapping to the original evidence outside this
repository. Store actual datasets and builds on the designated test media; a
generalized path does not prescribe which physical device backs it.

Privacy edits do not change numerical measurements, results, artifact hashes or
test counts. Reports are sanitized summaries, not assertions that raw logs have
been edited. Commit subjects describe the change, workload or topology and omit
private infrastructure identities.
