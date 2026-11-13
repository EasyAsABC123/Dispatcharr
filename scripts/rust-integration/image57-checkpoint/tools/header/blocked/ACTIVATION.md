# Reviewed activation boundary, still blocked

The 41 preparation payloads from image53-worker-header-main-prep-1 are copied byte for byte, including its historical blocked entry and local proofs. The new launch_once.py is the only intended future entry. It implements real dispatch to the frozen adapter rather than leaving a permanent placeholder. EXECUTION_BLOCKED remains True and no cue is issued by this packet.

Future exact argv, only after Root reviews a flag-only execution copy and writes a fresh cue:

    source artifacts/rust/tooling/environment.sh
    PYTHONDONTWRITEBYTECODE=1 python3 artifacts/rust/image53-worker-header-main-activation-1/launch_once.py --repo /home/justin/github/Dispatcharr

Root should substitute the reviewed execution-copy path if the flag is activated in a separate directory. No business tool, policy, case order or workload changes are authorized by activation. The canonical selection hashes normalize only the single launch_once.py EXECUTION_BLOCKED=True → False line. tools/run.py remains blocked on its own CLI; the launcher calls its exact implementation adapter only after validation and claim.

The launcher checks explicit functional-only authority, nonempty 32-hex Root nonce, exact Original revision/full archive hash, exact case list, boot, finite UTC/epoch issue and expiry with at most 1,800 seconds lifetime, Root's fresh preflight hash and boot, exact policy/plan/binding/selection, all sealed payloads, candidate manifest plus complete 791-input archive/runtime proof, and absent output. A symlink output is rejected. The Root preflight must be within 300 seconds before cue issue, and a second expiry check occurs before claiming.

Before dispatch it creates the cue-adjacent .claimed.json exclusively with O_EXCL/O_NOFOLLOW, writes cue SHA, canonical selection, boot, Root nonce, own PID/starttime, action, output and no-retry declaration, fsyncs the file and parent directory. A failed dispatch still consumes the once claim. Once output exists, cue and claim are retained alongside partial results. Root nonce identifies this authorization; the adapter independently generates and records its own private fixture nonce for all owned PID/starttime, origin and SQL-related scopes. No environment value authorizes dispatch.

The 28 launcher tests use temporary synthetic repositories and a mocked candidate verifier, never application/Docker actions. They cover empty/missing/bad nonce, time/UTC/boot, all core input bindings, corruption, absent/duplicate output, flag-only normalization, hardblock, and persisted once claim after synthetic failure. The first test run had one test-fixture mkdir error; activation-controls.log is retained, and corrected test infrastructure passes in activation-controls-final.log. It did not expose or fix a production launcher failure.

The inherited 32 phase/identity controls remain qualified at their original executed hashes. No new Source, Main, Docker, origin/decoder, Cargo or resource run has occurred. This packet does not establish image 53 Main acceptance, invalid-HEAD Source parity, error-body parity, full M4 or performance acceptance.
