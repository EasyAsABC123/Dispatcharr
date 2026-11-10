# Index failure payload component

Original Dispatcharr authors and contributors retain full application and
algorithm credit and AGPL licensing. Celery/Kombu/redis-py authors retain their
framework credit and original licenses.

The initial quiet-window draft is now the test-only
`rust/crates/job-transport/src/index_failure.rs` component. The retained Source136
dataset is `datasets/source-index-failure-136.json`; it contains synthetic fixture
observations rather than production account data. The exact reviewed closure is
[verified-index-failure-payload-writer-2.json](verified-index-failure-payload-writer-2.json).

Fourteen library tests and strict all-target Clippy pass. Thirty-six actual
Original-reader checks cover metadata, reconstructed exception class/module/typed
arguments,3600second backend expiry, pubsub bytes and namespace separation,
unchanged existing SUCCESS bytes/TTL/no-event guard, and replaceable FAILURE.
Two actual Redis tests cover delayed replies, canceled callers, failed then actual
physical drain, and wrong-type backend preservation. Eleven normal worker resource
tests plus three ETA tests repeat against the same complete frozen input tree
because the existing SUCCESS writer now delegates to the common metadata helper.

The codec accepts only the observed `builtins.IsADirectoryError` with integer
errno21/string arguments, `django.db.utils.ProgrammingError`, and cached-client
`redis.exceptions.ConnectionError`/`AuthenticationError` with one string argument.
It imports no Python module. It bounds total bytes, argument strings and traceback
text, requires exact task identity and a UTC timestamp, and rejects unknown
fields, parent shapes, children, modules and argument types. The encoding preserves
decoded metadata values, not Original JSON whitespace.

Traceback text in this fixture is an opaque supplied Original136 observation
replay. It is not a generated or executed native Python stack. Native traceback
format/content parity requires its own policy and gates. The writer is test-only;
there is no native classifier, worker error attachment, application route or202.
No native `Error::Execution` is converted into a fabricated Python exception.

The first fixture's failure is retained: it changed an already-observed terminal
FAILURE to rawSET SUCCESS, so Original's result consumer returned its cached old
failure. The corrected fixture uses independent pre-existing SUCCESS task IDs and
subscriptions; native runtime bytes are unchanged.

Next coordinate the baseline agent's detailed IndexService report before adding
native raised-result construction. Keep copyable worker control errors separate
from task terminal results. Admission, shutdown, cancellation, CAS ownership loss,
capacity and unresolved physical replies remain operational states. Connector or
startup errors are not the captured cached-client errors. Source138's actual
initialization AttributeError and busy child-publication kombu OperationalError
remain unsupported by this codec until separately added and gated.

Before a worker writer becomes reachable, prove native stage identity, commit and
physical release state, typed arguments and actual backend interoperability.
Preserve existing SUCCESS bytes/TTL even when a repeated execution raises.
Parented/ETA failure metadata, SQL-plus-release double-failure ordering and
result-backend outage/restart behavior remain separate gates. Partial XML,
lossy UTF8 and empty input must retain their actual Source index writes and normal
SUCCESS/null outcomes, rather than a blanket parser failure mapping.

The later [terminal result checkpoint](INDEX-TERMINAL-RESULTS.md) supersedes the
roadmap above for typed native terminal construction, observed Directory/P0001
classification and both worker result paths. This earlier codec remains test-only
and relays supplied observations; general failures and application attachment
remain separate gates.
