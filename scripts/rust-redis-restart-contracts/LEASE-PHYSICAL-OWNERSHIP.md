# Lease mutation physical ownership

Original Dispatcharr authors retain application and algorithm credit; redis-rs authors retain client credit.

Baseline169 preserves an actual failure on the accepted coordination runtime. A bounded RESP proxy forwards each selected script to real Redis, observes its successful reply and then withholds or drops only that reply. All five held-reply cases released command admission at the observer deadline: heartbeat, provider clear, provider reserve, explicit release and ordinary `Lease::Drop` release. The four lost-reply cases applied the successful mutation once. This is separate from accepted152: canceled initial acquisition already retains its `AcquireDelivery` permit through direct fenced cleanup.

The scoped fix supervises each admitted lease mutation in an owned task. Its true command permit, immutable token/keys, connection owner and per-lease physical counter survive caller timeout or cancellation until the actual script future returns a reply or disconnect. Public observer deadlines and error behavior remain unchanged. Scripts, provider/credential counters and ownership predicates remain unchanged. An ambiguous mutation is never replayed.

`Lease::settlement()` returns an observer for that lease's admitted physical operations. `is_settled()` and `wait()` report physical completion, not remote success. A caller must prevent new operations separately before using settlement as a transition boundary. Admission waiters are not yet physical operations. This API does not serialize ownership migration or provide a Redis/PostgreSQL transaction.

The positive gate holds successful replies for more than2.2seconds, verifies both the lease receipt and sole command admission remain occupied, cancels four caller futures, and checks actual profile/credential effects and eventual cleanup. An independent regression replays provider fencing and canceled acquisition against the exact same frozen runtime. Acceptance is recorded only in the corresponding completed proof, not by this design text.

This fix does not change active VOD recovery policy. A logical heartbeat failure can still mark a lease lost while its actual reply is pending; recovery must settle and classify the immutable old identity before replacing it. The image34 active-stream EOF/reopen503 mismatch166 remains open. The background reaper's separate observer wrapper, unrelated cached owners and realtime membership recovery are outside this scoped lease-mutation change.
