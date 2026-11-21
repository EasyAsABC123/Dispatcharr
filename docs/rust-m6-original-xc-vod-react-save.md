# Stock Xtream VOD enable through the preserved React Save

An existing active Xtream account was submitted with the full 14-field JSON
body from the unchanged React Save. A local Node harness executed the pinned
`M3U.jsx` hydration and submit action, `M3uUtils.js` helpers, and the API
request serializer. Those source bytes match between pinned Original and
2025258f. The harness recorded fetch without rendering a browser. The owned
Stock HTTP client sent those exact serialized bytes to the account detail.

The initial Stock GET matched the types and values of the 13 other fields.
This account used null user-agent and server-group selections, inherited hash
key, and interval zero with empty cron. The blank password, Xtream expiry,
file, account ID, and `create_epg` were absent from the JSON body. All three
actual calls returned 200 for GET, PATCH, and GET. The owned account retained
its other properties, profiles and schedule identity. The update created the
two disabled typed Uncategorized links and published one VOD refresh root
to the default queue, preserving existing ready messages byte for byte.

The Source controller and strict reducer exited 0 and both Compose roles
retired. This run covered the Stock producer and did not start a worker or
Beat process. The [four-field Stock result](rust-m6-original-xc-vod-enable.md)
remains a separate receipt. Native full-form parity, full M6 and performance
remain open. [Redacted receipt](../scripts/rust-job-contracts/verified-original-xc-vod-react-save-1.json)

The original Dispatcharr and Celery authors and contributors retain full
credit. [Attribution](../ATTRIBUTION.md)
