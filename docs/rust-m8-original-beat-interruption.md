# Original backup scheduler interruption

The pinned Original Dispatcharr application `1241b3f7` ran the normal daily backup scheduler using Celery 5.6.3 and django-celery-beat 2.9.0. Each arm created an administrator-owned backup schedule and an authentic older PostgreSQL archive in a fresh disposable database and Redis namespace. The same copied database, web, worker, and Redis generations stayed live across one exact Beat interruption and ordinary uninstrumented restart.

| Placement | Accepted before restart | Accepted after restart | Saved count after the fresh stop | Successful worker results |
|---|---:|---:|---:|---:|
| Before Redis publication | 0 | 1 | 1 | 1 |
| After Redis acceptance before SQL sync | 1 | 1 | 1 | 2 |

The separately observed SQL row still held count zero at both instrumented boundaries. Placements were tied to the actual Beat generation and the recorded process exit was −9 with no graceful signal sent. Redis publication identities matched real worker results through the 25-second restart observation. Each retained archive state was independently checked. The unrelated copied SQL stayed equal, the final disable returned 200, and each project retired all of its owned process and container generations.

Three earlier physical controller failures remain in the receipt. The traced process's private marker was initially unreadable across the rootless bind. After the observer was fixed, both arms reached placement and the controller recorded an additional graceful signal after its abrupt signal. The final fixtures use one exclusive signal selection before the wait and physical teardown.

The current Rust shared Core scheduler's durable SQL tick precedes its Redis publish. These Original observations supply comparison evidence for that interrupted ordering. Native interruption, cross-system atomicity, exactly-once execution, performance and full migration acceptance are separate scopes.

Original Dispatcharr, Django, Celery, Redis and PostgreSQL contributors retain full credit. [Machine-readable receipt](../scripts/rust-integration/m8-original-beat-interruption.json)
