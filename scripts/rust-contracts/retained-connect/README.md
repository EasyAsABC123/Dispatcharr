# Retained Original Connect collection observations

Original Dispatcharr and dependency authors retain all application and algorithm credit.

The eight collection GET responses cover empty and seeded contents, role denial, inactive users, API keys and invalid bearer precedence. The sixteen collection POST responses cover thirteen rejected requests with unchanged rows and sequence, and three creations with one new integration each. Those creations advanced the sequence and left subscriptions and deliveries empty. The owned webhook count stayed zero.

Both runners exited 1 after a separate TS streaming 404 body mismatch and passed independent exact cleanup. The GET comparison passed within its scope. The POST ran only against Original. These observations support partial associations for two declared operations.

From the repository root, reproduce the binding with the retained private reports and earlier register:

```sh
git show 6e5f04ddd:docs/rust-contract-register.json > /tmp/dispatcharr-connect-before.json
python3 scripts/rust-contracts/retained-connect/reproduce.py \
  --private-root . --register /tmp/dispatcharr-connect-before.json \
  --output /tmp/dispatcharr-connect-binding.json
cmp scripts/rust-contracts/retained-connect/bindings.json /tmp/dispatcharr-connect-binding.json
```

The binding contains response digests and scoped SQL comparisons. Raw payloads and credentials remain in their private fixtures.
