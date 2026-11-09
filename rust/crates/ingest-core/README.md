# Streaming M3U parser component

Full credit for Dispatcharr and the parsing behavior belongs to its original
authors and contributors; see [ATTRIBUTION.md](../../../../ATTRIBUTION.md).
The behavioral reference is `apps/m3u/tasks.py` and `apps/m3u/utils.py` at the
recorded upstream baseline. Directive casing, quote handling, duplicate keys,
display-name fallback, ignored directives and permissive URL-prefix recognition
are preserved literally.

The parser retains one pending entry and yields each completed entry to its
caller. Default bounds are 256 KiB per input line, 1 MiB per assembled entry,
4,096 attributes and 256 VLC options. Exceeding a bound returns an explicit error,
discards the pending entry and prevents later output from a failed parser. The
caller must enforce a streaming line-reader bound before allocating a raw line.

This starts M6 component work. Provider fetching, compression/encoding,
database batches, hashes/filtering, groups, EPG matching, scheduler/Redis job
coordination, progress events and recovery are still separate migration gates.
The component does not yet reproduce the source logger warning side effects.
It is not wired into the application or a replacement for its import jobs.
