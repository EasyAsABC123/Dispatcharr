# M11 offline release decision for 0c5133b

The offline audit matched all 2,161 pinned application inputs against Git
blobs, including 497 React, license, and attribution inputs. It verified
the archived receipt for publication `f37bdfb21` and the exact local
warm rollback evidence. The audit rejected a substituted build manifest.

| Gate | Status at audit |
| --- | --- |
| Exact source and protected inputs | Passed offline |
| M9 integrated release acceptance | Open |
| M10 release platform and distribution | Open |
| M10 rollback | One bounded local rehearsal passed |
| M10 controlled rollout | Requires accepted release and platform evidence |
| M11 default switch and operator handoff | Requires the controlled rollout result |
| M11 fallback retirement | Requires monitored operation and the retention window |

The decision stays open. The existing [milestone plan](rust-complete-parity-plan.md)
defines the runtime, upgrade, rollout, and monitoring evidence needed to
move these gates. Original Dispatcharr, its React frontend, and its
dependencies retain their existing attribution and licenses.

[Machine-readable receipt](../scripts/rust-integration/m11-0c5133b-offline-release-decision.json)
