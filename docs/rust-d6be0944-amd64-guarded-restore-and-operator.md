# d6be AMD64 guarded restore and read-only operator

The ordinary d6be package passed the read-only operator canary. The probe verified signed ingress before and after the shipped CLI calls, checked invocation exit codes, and confirmed that application state and the full engine inventory matched the opening captures.

The guarded restore workload exited 0 and restored the exact engine inventory. Its original proof wrapper exited 1 when it loaded an evaluator module without the expected method. Two subsequent file-only replays loaded the sealed evaluator by exact path and reproduced the same successful proof. The original wrapper exits and proof inputs remain available.

The proof verified the partial ACL restore, explicitly invoked clean rollback, signed readback after Main restarted, 82 typed SQL columns, four broker and result joins, and two signed archive deletions. The [typed receipt](rust-d6be0944-amd64-guarded-restore-and-operator.json) records the exact scope. Automatic rollback and full M8 and M10 acceptance remain open.

Original Dispatcharr and all upstream contributors retain full authorship and license credit.
