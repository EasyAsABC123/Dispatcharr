# Stock public and direct ModelAdmin boundary

The original Dispatcharr and Django authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

One owned stock AIO Compose fixture made eight anonymous GETs across the public nginx listener and the direct Django listener. Both public ModelAdmin list paths reached Django and returned an empty 302 with the exact relative admin login and next path. The public admin login and deeper history paths returned nginx 301 redirects to the same-origin React login. The direct Django login returned HTML 200. Anonymous direct list and history requests returned the stock Django 302.

The run and independent proof returned 0 and verified exact cleanup. The [machine-readable receipt](../scripts/rust-integration/m0-stock-aio-modeladmin-boundary.json) identifies the exact Original package and response projections. Authenticated use through the public proxy remains open. The [separate diagnostic](rust-m0-e68fb95-modeladmin-readonly-diagnostic.md) records authenticated direct Original and the measured Native ingress behavior.
