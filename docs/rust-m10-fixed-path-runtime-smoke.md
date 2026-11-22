# M10 fixed-path private runtime and smoke verification

Original Dispatcharr, React, FFmpeg, and dependency authors retain full credit.

The private production-path BuildKit pilot built exact child revision `424880a9ba0a375eab8f985b1a279252c4d0a54b`. Its warm build took 342.785 seconds with the stock copy-and-touch policy and 148.099 seconds with the content-aware policy, a 56.8% reduction for this one build pair. The cold comparison and warm comparison matched both Main and operator binaries and executable modes. The build results were published in `d06ee9220c708e4899ff3b5dd66e02abac17983a`.

Independent non-root, read-only, no-network checks compared 40,209 selected paths across the independent ordinary image and both pilot images. The selected bytes, modes, ownership, file types and link destinations matched, including React, media tools, the guarded restore helper, license and attribution. The audit verified FFmpeg and the package inventory and retired each owned probe.

The content-aware child passed the isolated clean-install and shared MPEG-TS smoke checks. The clean-install child had already exited 0 when the first wrapper stopped on a project name check. The second wrapper preserved that child result and ran the fresh short playback with two readers, one provider reservation, one upstream connection and real audio/video decoding. It stopped after the child exited 0 because it expected the wrong receipt filename. A separate verifier consumed both actual child receipts, confirmed exact project absence, and exited 0.

The timings apply to this private build experiment. The runtime evidence covers only the selected package paths and the two listed smoke scenarios. [Machine-readable evidence](../scripts/rust-integration/m10-fixed-path-runtime-smoke.json)
