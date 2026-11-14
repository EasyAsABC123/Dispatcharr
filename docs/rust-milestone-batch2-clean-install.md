# Whole candidate: native install and React restart

The `e88cc73f…ece65` image built from `1dc16f8d` passes all11 assertions in the
existing complete fresh-install/React workflow. This is a new execution on the
combined worker/profile/plugin candidate; it does not inherit the old image's
result.

Official PostgreSQL17, Redis, Rust Main and data initialization run through
Docker Compose with no Django runtime service. The unchanged React frontend
creates the real first administrator, logs in, initializes and preserves tokens
across restart. All required browser API paths pass before and after restart.
Execution, cleanup and evaluation all exit0, with process groups settled, both
streams at EOF and exact project-container/network/volume absence.

[Workflow evidence](../scripts/rust-integration/milestone-batch2-clean-install/integration.json)
and the [dispatcher result](../scripts/rust-integration/milestone-batch2-clean-install/result.json)
bind the immutable image and source. The overall dispatcher exit2 remains
preserved because other required cells are blocked. Its19.2-second duration is
functional timing, not a paired performance result. Full milestones and new
resource gains remain unaccepted.

Original Dispatcharr authors and dependency contributors retain full application,
algorithm and license credit.
