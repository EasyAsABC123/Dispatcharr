# BE82 M0 documentation wire comparison

The exact ordinary BE82 AMD64 package served all 484 cases in the pinned documentation matrix. The Native launch and exact inventory checks exited 0. The independent strict reducer exited 1.

All 84 successful OpenAPI schema GETs matched Original's status and body bytes. Their remaining differences were confined to header names. Across the full matrix, 296 selected cases differed only in header names and 28 differed in status and other wire fields. The reducer also recorded 156 HEAD transport diagnostics and four unsupported cache cases.

The 40 Swagger OPTIONS cases now have Native observations. Both backends returned 404 on eight and 406 on eight. Original returned 500 and Rust returned 405 on the other 24. The strict comparison uses Original's direct uWSGI listener and ordinary Native Main. Browser-facing ingress comparison and full M0 acceptance remain open.

The [typed evidence](rust-be828d4c-m0-schema-wire.json) records the exact package identity and comparison digest. Original Dispatcharr, Django REST Framework, drf-spectacular, and upstream contributors retain full authorship and license credit.
