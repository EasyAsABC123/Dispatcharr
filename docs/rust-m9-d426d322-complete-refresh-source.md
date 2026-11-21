# M9 complete XMLTV refresh Original workflow on d426d322

The canonical coordinator independently verified `package/complete-refresh-original` while evaluating packaged Rust `d426d322b27a197aaaaa057ef73dfb53c516c452`. The pinned Original workflow completed both local XMLTV imports, all 40 checks and all six successful task results. It verified Current and Guide reads, the second import's replacement programme, and preservation of the default database. The Original node's execution, cleanup and evaluation each exited 0. With the previously verified 14 workflows, this adds one fresh node for **15 of 127 required workflows** on the exact package.

The Native test ran under the same reservation. The first import returned HTTP 202 and emitted refresh events, and both the Refresh parent and its programme-index child reached SUCCESS. The parent recorded a second child ID for Series. Its terminal result did not appear before the fixture deadline, so the complete Native workflow failed before the second import. The Native execution and evaluation each exited 1, and independent cleanup exited 0. Both owned project inventories were empty. Coordinator exit 2 and pair reducer exit 1 remain recorded. The cause of the missing Series result remains open.

The earlier wrapper exited 1 during dispatcher lease lookup before Compose. The corrected dispatcher exercised the ordinary workflow above. The Native EPG-only worker configuration, full M6 and comparative performance retain separate gates.

[Machine-readable receipt](../scripts/rust-integration/m9-d426d322-complete-refresh-source.json)

Original Dispatcharr, Django, Celery and dependency authors retain full credit.
