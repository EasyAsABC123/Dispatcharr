# Original Core browser negotiation through stock nginx

The pinned Original all-in-one package answered 18 signed nginx requests on the User Agents list and Stream Profiles detail routes. A separate direct uWSGI probe ran two HEAD diagnostics. The test exercised administrator and standard sign-ins and verified unchanged state after the reads. Creation, readiness, probing, reduction and owned cleanup passed.

For administrators, navigation and the DRF AJAX Accept header returned HTML identical to the corresponding explicit HTML requests after CSRF masking. `?format=api` returned HTML with query-aware actions. An explicit JSON Accept header conflicting with that format returned JSON 406. Navigation with `?format=json` returned JSON 200. The standard user received HTML 200 on both routes with zero named form controls and one script CSRF mask. Administrator OPTIONS returned HTML 200 with route-specific metadata.

The nginx HEAD responses sent no body and declared 16,506 and 17,765 bytes respectively, one byte above the corresponding GET in each case. Direct uWSGI sent those declared byte counts as bodies. The Native comparison uses stock nginx as the reference ingress.

A previous retry stopped at its first HTML capture because the guest lacked a decoder import. The corrected fixture retained exact teardown records and captured all observations directly from the fresh stock process. The Native differential and formal M0 acceptance remain open.

Original Dispatcharr, Django REST Framework, React and their contributors retain full credit.

[Original observations and fixture exits](../scripts/rust-integration/m0-stock-core-browser-negotiation.json)
