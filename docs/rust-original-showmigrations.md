# Original migration inspection command

The pinned Original package ran two real Django 6.0.9 commands against one
fully migrated disposable PostgreSQL copy. Each command exited 0.

| Command | Applied | Pending |
| --- | ---: | ---: |
| `showmigrations core --list --database default --no-color` | 32 | 0 |
| `showmigrations --plan --database default --no-color` | 184 | 0 |

The verifier matched each row and applied marker to the Original migration
graph and PostgreSQL recorder. It checked dependency order and agreement
between the two core inventories. The exact migration recorder and full
default database manifest stayed unchanged. Both invocations ran Original
startup hooks that changed developer notifications and periodic task state
inside the copy.

The isolated Compose fixture retired both the Original web and Origin
containers and their exact host process generations. Its labeled container,
network and volume inventories were empty. The equivalent Native operator
command remains open. Original Dispatcharr and
Django authors retain full credit.

[Redacted Original receipt](../scripts/rust-operator-contracts/showmigrations/verified-original-1.json)

A second independent disposable copy used the ordinary Original migration plan
and reversed only `core.0032_limit_default_interleave_delta`. Both startup and
inspection effects stayed inside that copy. The list showed 31 applied and
one pending migration. The plan showed 183 applied and one pending. The only
stdout difference from the fully applied reference was that migration's
pending marker. Both inspection commands preserved the complete recorder,
and the default database stayed unchanged. Controller exit and exact retirement
were successful.

[Redacted Original pending receipt](../scripts/rust-operator-contracts/showmigrations/verified-original-pending-1.json)
