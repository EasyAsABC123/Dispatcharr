# Abrupt fMP4 owner loss on Original and Rust

A fresh owned stock Original run used a dedicated uWSGI lead and let the application serving the attached peer survive. Its paired evidence references the earlier exact Rust `f960875` run. Both began with two live fMP4 viewers sharing one provider slot and upstream connection. Each retained a decodable 74-frame lead prefix and 75-frame peer prefix. The harness killed the exact lead, observed exit 137 without OOM, sampled seven times through 75 seconds, and verified exact cleanup in each scope.

The attached peer eventually reached EOF in both arms. The lead ended in body EOF on Original and a transport error on Rust. At 75 seconds, Original still held the original remux lease and one provider slot. Rust had removed the remux lease and freed the slot. Original still counted two viewer records and Rust counted one. The one upstream connection had closed in both arms.

These are recorded differences in the bounded abrupt scenario. The harness started zero fresh stream requests after loss. Fresh-client reconnect and same-socket takeover remain unverified. The [machine-readable comparison](../scripts/rust-integration/m3-f960875-original-abrupt-fmp4-comparison.json) binds both outcomes to the raw proof. No performance estimate was taken.
