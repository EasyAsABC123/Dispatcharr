# Retained Original DVR and Xtream playback evidence

Original Dispatcharr and dependency authors retain application and algorithm credit.

The DVR HLS and authorization reports preserve their observed passing probes.
Their outer controller exited 1 and verified exact cleanup. The manual Comskip
responses belong to three Original controllers that each exited 1. The retained
manual business qualification is narrower than worker-graph acceptance. The XC
wire evidence retains 36 normalized Original responses from a separately paired
capture. Fixture roles and supplied credentials do not establish that an
authentication attempt succeeded. The long and short XC paths omit actual
usernames and passwords. These responses leave positive XC media, broader
Native compatibility and full performance acceptance open.

To regenerate the evidence from the preceding exact register and retained XC
capture and DVR driver custody, run from the repository root:

```sh
git show 60f56bdf9:docs/rust-contract-register.json > /tmp/dispatcharr-source373.json
python3 scripts/rust-contracts/retained-dvr-xc/reproduce.py \
  --repo . --private-root . --register /tmp/dispatcharr-source373.json \
  --output /tmp/dispatcharr-source378.json
cmp scripts/rust-contracts/retained-dvr-xc/bindings.json /tmp/dispatcharr-source378.json
```

The public register verifies the hash-bound projection without private capture
access. The reproduction starts no service and measures no performance.
