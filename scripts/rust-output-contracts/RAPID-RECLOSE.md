# Original VOD reconnect timer compatibility

All application credit belongs to the original Dispatcharr authors and
contributors. The timer contract below now has scoped native replay evidence.

The retained original fixture opens and polls one VOD response, closes it,
reconnects the same session within the one-second grace interval, polls and
closes that response immediately. Both original delayed-cleanup timers then see
no active stream. Each publishes `vod_stopped` and a statistics update, even if
the other timer already removed the Redis hash. The captured original sequence
contains one start and two stops, with matching start/stop/stop audit rows.
Image20 suppresses the earlier timer through an epoch check and publishes one
stop. Image21 separates event publication from fenced physical cleanup and
passes the [paired frozen-runtime replay](verified-rapid-reclose-image21-1.json):
one start, two stops, three statistics events and start/stop/stop audit rows.
Two native tests also check retained request admission. Ordinary six-case
lifecycle and packaged HTTP/WebSocket regressions pass separately. The rapid
replay uses an in-memory native event sink; packaged rapid WebSocket transport
and the expanded malformed/delayed Redis reply family remain open.

The implementation separates the event decision from physical cleanup:

1. After each timer's grace interval, read the current global session-ID active
   count using original integer semantics. A missing/invalid/error reply means
   inactive in the original helper. Check the current global ID rather than an
   old local session object, because another worker may own its replacement.
2. If inactive and this response was eligible to announce a stop, publish that
   timer's original stop event/audit/stats before physical cleanup. Keep event
   work outside pool/state locks to avoid the existing statistics lock cycle.
3. Keep epoch/token/identity checks for physical metadata deletion and provider
   lease release. A historical event must never delete a replacement owner's
   metadata or decrement another active session's provider allocation.
4. Retain bounded physical-work admission through delayed Redis and event work,
   and include every timer in graceful-shutdown accounting.

Required proof is a fresh paired original/native replay of rapid reclose,
ordinary disconnect, unpolled response, active reconnect, normal EOF and provider
failure. Add a second active session sharing the provider and a replacement
owner of the same session ID, and inspect actual counters/metadata throughout.
Test Unicode/oversized/malformed Redis integer fields and paused replies,
bounded timer admission and shutdown. Repeat packaged HTTP/WebSocket regressions
on the next immutable image. Image20's ongoing resource matrix must remain tied
to image20; this later change cannot inherit its measurements automatically.
