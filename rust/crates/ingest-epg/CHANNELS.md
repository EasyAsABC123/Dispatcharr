# XMLTV channel pass

All application and algorithm credit belongs to the original Dispatcharr authors and contributors.

`ChannelImporter` is a bounded, app-unwired port of the XMLTV channel helper. It keeps the original initial identifier snapshot, separate batches of 500 creates and updates, shared mutable update objects, cache clearing after every 1,000 processed channel elements, and the original progress schedule. Blank IDs still count as processed. New duplicates keep their first inserted fields; existing duplicates usually keep the latest fields, but a pending update across a cache clear can keep the first update. These cases are captured from the original runtime rather than replaced with a generic upsert.

Creates and updates retain original autocommit boundaries. A later invalid TVG identifier can leave an earlier batch of 500 rows committed. Final stale cleanup is one transaction; it preserves NULL identifiers and base-mapped rows, while deleting override-only stale entries, their programmes, and clearing the override references. The helper recovers truncated XML; the file-fetch layer must preserve its separate extension-dependent footer check.

The worker supervises one blocking parser and one private SQL session per admitted job, with bounded frame count, frame bytes, catalogue keys and retained state. String allocation capacities are charged. Caller cancellation closes the queue after physical SQL completes and joins the actual parser before releasing job/lease guards. A false shutdown result keeps those guards alive. Public frame-credit counts describe enforced reservations, not process RSS.

Default limits are two physical jobs, 32 queued frames, 8 MiB of frame credits per job, 100,000 catalogue entries, 64 MiB of state per job, and a regular input file below 4 GiB. Configurable ceilings remain explicit. SQL argument/driver and native XML parser memory are additional bounded working space; no whole-service memory or performance result is claimed here.

The source corpus covers Unicode/codepoint limits, blank IDs/names/icons, namespaces, duplicate/cache-clear/batch boundaries, stale cascades, and different early versus final SQL error prefixes. Only newly allocated EPG row IDs normalize by TVG identity; existing IDs and channel/override references compare exactly. Field sets are compared independently of list order; no HTTP queryset ordering acceptance is implied.

Enabled logo auto-application, general SQL/IO error spelling, source deletion/reassignment races, download/extraction/path resolution, complete refresh leases/dispatch, child jobs/indexing, actual producer WebSocket transport, app wiring and resource measurements remain separate gates. The caller/path resolver must prepare cache-parent directories. Missing-file cases here verify model/error effects, not complete filesystem layout compatibility.
