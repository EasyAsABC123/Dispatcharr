# CoreSettings and ChannelGroup ModelAdmin reads on e68fb95

The original Dispatcharr and Django authors and contributors retain full credit. See [Attribution](../ATTRIBUTION.md).

The owned fixture requested the CoreSettings and ChannelGroup list and populated history views using a normal administrator session and an anonymous client. The stock Original Django upstream served the four authenticated pages as HTML 200 and redirected anonymous requests to its admin login with 302. The ordinary Native ingress returned 404 on the list paths and 301 on the history paths for both cases.

| View | Original administrator | Original anonymous | Native administrator | Native anonymous |
| --- | ---: | ---: | ---: | ---: |
| CoreSettings and ChannelGroup list | 200 | 302 | 404 | 404 |
| CoreSettings and ChannelGroup history | 200 | 302 | 301 | 301 |

The independent diagnostic returned 0 and verified exact retirement of two Original and four Native roles. Its [machine-readable proof](../scripts/rust-integration/m0-e68fb95-modeladmin-readonly-diagnostic.json) records the observed dedicated ModelAdmin gap. Stock front-proxy exposure and a pure Rust counterpart for the direct admin views remain separate work.
