# Engine caller cancellation on 63ee09fc

The physical test shared Core and Engine with a real held Redis
reply. Two Engine callers joined the pending refresh. Cancelling
one left the same physical refresh active. The second received
the result, and the reply gate and owned workload retired cleanly.

The exact source test used a management Compose role, with the
default database and cache scope unchanged. Normal HTTP caller
identity and full proxy-health acceptance remain open.

Original Dispatcharr and its upstream contributors retain full credit.
[Machine-readable waiter qualification](../scripts/rust-integration/m2-63ee09fc-engine-waiter-component.json)
