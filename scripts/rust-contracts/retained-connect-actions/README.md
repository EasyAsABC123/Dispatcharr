# Retained Original delivery logs and subscription replacement

Original Dispatcharr and dependency authors retain all application and algorithm credit.

Delivery log GET observations cover pagination, type and integration filters,
and denial by role. The actual collection path is literal. Query values taken
from the owned Source fixture retain symbolic forms. Response digests use the
captured JSON projection with relative pagination links.

For subscription replacement, the Original request log corroborates thirteen
literal PUT paths and statuses. Twelve cases preserved database and sequence
state. The deliberate stored-duplicate 500 removed one stale log and its
subscription and committed earlier upserts. Four successful cases have
literal route templates and captured SQL comparisons with generated IDs
retained only as fixture symbols. Both outer exit codes were 0 and independent
exact cleanup passed. The subscription PUT fixture measured Original.

```sh
git show c82825f14:docs/rust-contract-register.json > /tmp/dispatcharr-actions-before.json
python3 scripts/rust-contracts/retained-connect-actions/reproduce.py \
  --private-root . --register /tmp/dispatcharr-actions-before.json \
  --output /tmp/dispatcharr-actions-binding.json
cmp scripts/rust-contracts/retained-connect-actions/bindings.json /tmp/dispatcharr-actions-binding.json
```

The published projection contains no private response or request payloads.
