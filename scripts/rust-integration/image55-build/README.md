# Image55: combined-source build checkpoint

Original Dispatcharr contributors and dependency authors retain all application,
algorithm and licensing credit. React and the Original license remain byte exact.

The immutable `aca6d900ad13700ab86b2898134a61e4dba57bc1` application build passed,
producing image `sha256:120df2f930a7f4d2738a04fbd793c16b230b7a80114f4d6b03f670244940a014`.
Its 1,291 inputs comprise 794 Rust inputs, 495 Original React files, LICENSE and
ATTRIBUTION.md. The normalized source archive SHA256 is
`8addffe734f0326b99f9faeb7fac0af670129a482ecb591df2a63638446c74af`.
The source archive is reconstructible from Git and is omitted from this packet.

The actual builder and host wait exited zero. The builder verified FFmpeg and
retained the runtime package list. A separate isolated, read-only, unprivileged
probe verified the installed license/attribution and every React asset against
image54's actual asset inventory; they matched exactly. Its owned container is gone.

Check the retained build/input/identity/cleanup evidence offline:

    python3 scripts/rust-integration/image55-build/validate.py --repo .
    python3 scripts/rust-integration/image55-build/test_packet.py

The optional Git check reconstructs all application bytes and the normalized tar in
memory, including a direct comparison with Original React/LICENSE blobs. These
commands run no Docker, application, compiler or network request. The build used the
unchanged `scripts/rust-integration/build.py` from the exact committed revision;
the complete source/private Git recipe is in the sibling sdkaca6 packet.

This is a build checkpoint. Package/Main replay, new workload resource measurements,
whole-graph acceptance, soak and cutover/rollback are open. The selected functional
checkpoint remains image54; image53's measured gains do not transfer to this image.
