# Retained Original Comskip task binding

Original Dispatcharr, Celery, Kombu, Comskip, FFmpeg and dependency authors
retain all application, algorithm and codec credit.

The M0 ledger assigns one partial Source contract to
`candidate-79333cd4d7e842b2d97b`. It identifies
`comskip_process_recording` and its Original shared-task decorator in
`apps/channels/tasks.py` at line 3374.

The [retained lifecycle](../../rust-integration/comskip-positive-lifecycle-source1/README.md)
provides six actual task outcomes. Manual MARK, MARK repeat, CUT and CUT
repeat publish own-root tasks. Natural MARK and CUT attach their Comskip
tasks to the originating recording task and retain its backend child link.
Four task results are `ok` and both repeat results are `already_processed`.
Each repeat preserves the whole recording row with zero Comskip events.
The five controllers exited 1, 1, 1, 1 and 0. All exact cleanup checks
passed. The raw journal retains unrelated manual background publications
and the natural MARK artwork backend remains unobserved.

```sh
python3 -B scripts/rust-integration/comskip-positive-lifecycle-source1/verify.py
python3 -B scripts/rust-contracts/retained_comskip_task_contracts.py
python3 -B scripts/rust-contracts/retained_comskip_task_contracts.py --binding > /tmp/dispatcharr-comskip-binding.json
cmp scripts/rust-contracts/retained-comskip-task/bindings.json /tmp/dispatcharr-comskip-binding.json
```

The first command verifies the existing business projections and corruption
controls. The second binds those artifacts to the declared M0 task identity
and prints a compact projection. The binding command derives the canonical
contract from these same published inputs. All commands run offline.
Native parity, complete task-domain coverage, whole milestones and
performance remain open.
