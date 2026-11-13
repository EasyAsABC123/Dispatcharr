# Core authentication and error-response ownership

Core settings now uses the bounded retained REST reader for subject and UI-network checks. It preserves invalid raw-header rejection and the authentication → network → role order. The bounded subject is retired before allocating settings metadata; all work shares the original admitted request permit.

Admitted error responses retain that same permit in their body bytes, including byte clones and observer-timeout responses. Four retained error bodies refuse a fifth request before authentication; dropping the final clone makes the slot reusable. Empty successful 204 responses retire without a synthetic body owner.

The plugin-inclusive composition has 854 Rust inputs and five coupled inputs. Formatting, whole-workspace compilation, strict all-target checks and default tests pass: 768 passed, 738 ignored. Eleven added controls cover raw-header rejection, permission ordering, subject lifetime, error-body clones, capacity refusal/reuse and bounded output. The [Root review](../scripts/rust-contracts/core-rest-owner-Root-current854-review.json) binds the exact source and command exits.

The [fresh compiled-component replay](rust-core-bounded-rest-component.md) now passes 32 Source-derived HTTP comparisons and six ownership/capacity cases on this exact `46b91d70` composition. Main route assembly and later-source application acceptance remain separate gates. Earlier historical Core32, Core7 and REST2 executions remain separate from this new replay. Duplicate-key HTML compatibility is still a separate correction; no new API performance gain or full M5 acceptance is claimed. All application and algorithm credit belongs to the original Dispatcharr authors.
