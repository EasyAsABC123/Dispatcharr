# Retained Original Connect detail updates and deletes

Original Dispatcharr and dependency authors retain all application and algorithm credit.

The Original fixture reports 18 PUT requests and nine DELETE requests at the
ordinary Connect integration detail route. The Original server warnings
corroborate 14 literal PUT paths and seven literal DELETE paths. Four PUT
successes and two DELETE successes retain their generated IDs as fixture
symbols. PUT response digests use a sorted JSON projection. DELETE response
digests use the actual HTTP bytes.

Rejected cases preserved integrations, subscriptions, delivery logs and their
sequences. Successful PUT changed only the targeted integration. The stock
DELETE removed one integration, two subscriptions and three delivery logs.
The childless DELETE removed one integration. Both DELETEs returned HTTP 204
with empty bodies and preserved unrelated rows and sequences. Both fixtures
recorded zero calls at the owned receiver, exited 0 and passed independent
exact cleanup. These are scoped Original observations. Native detail
coverage, milestone acceptance and performance remain open.

To reproduce the public projection from the retained private fixtures:

```sh
git show 52b9f59f1:docs/rust-contract-register.json > /tmp/dispatcharr-connect-detail-before.json
python3 scripts/rust-contracts/retained-connect-detail/reproduce.py \
  --private-root . --register /tmp/dispatcharr-connect-detail-before.json \
  --output /tmp/dispatcharr-connect-detail-binding.json
cmp scripts/rust-contracts/retained-connect-detail/bindings.json /tmp/dispatcharr-connect-detail-binding.json
```
