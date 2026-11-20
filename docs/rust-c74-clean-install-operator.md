# Clean installation and packaged operator commands on C74

The original Dispatcharr authors and contributors retain full application,
React and operator workflow credit. [Attribution](../ATTRIBUTION.md)

The exact C74 image passed all 11 clean-install checks. The native-only
Compose stack started with empty PostgreSQL and Redis. Disabled
initialization left the database empty. Explicit initialization created the
schema and its migration records. The existing React setup form created
the first admin, signed in and completed the initial API requests.
A restart preserved the user, schema and authenticated browser session.

The shipped operator executable passed its 17 safe command cases against
retained Original authority. Both scoped executions, evaluators and
cleanup commands exited zero with their process groups settled. Exact
Native help and stderr coverage and the full Django CLI remain open.

The coordinator recorded two passing required nodes, one retained Original
reference and 124 unbound nodes. Its actual exit was 2. The integrated
package matrix and performance remain open.
[Validation receipt](../scripts/rust-integration/milestone-batch113/c74-clean-install-safe17.json)

A separate run on the same exact image passed all 22 packaged
`createsuperuser` cases and 95 checks. The shipped UID 65532 CLI ran against
an owned copy containing 26 nonempty tables. The default database remained
unchanged and the fixture retired its containers, networks, and volumes.
The pinned Original comparator passed all 154 typed comparisons across the
same cases. Both nodes passed execution, evaluation, and cleanup. This
coordinator run recorded two passed nodes and 125 blocked nodes with actual
exit 2. Exact CLI byte parity remains open.
[Validation receipt](../scripts/rust-integration/milestone-batch113/c74-createsuperuser22-typed154.json)
