# Finite-number JSON compatibility

Original Dispatcharr and dependency authors retain full credit and licensing.

The shared finite-number formatter now uses the existing pinned serde_json shortest round-to-even digits and preserves Python's decimal/scientific thresholds, exponent spelling and signed zero. The previous generic formatter chose different digits in 259 captured cases. All 9,368 finite values in the extended Original corpus now match for repr, repr length, compact JSON and UTF8 size, with exact bit identity. This is a finite helper contract, not complete HTTP, ORM or nonfinite-number acceptance.

The original 9,338-case corpus remains historical. Its values are all retained in the separately sorted extension, including 30 additional boundary cases. Fifteen corruption controls validate typed comparison behavior. The same-file scanner consumer replay passes 5,135 checks and four physical controls; all 512 mtime vectors now have zero generic-format differences. Historical records reporting nine differences are preserved.

The private767 source passed strict workspace checks and 476 defaults. A combined771 source containing the separately qualified scanner recovery work also passed strict checks, 486 defaults and the full finite corpus. A subsequent combined-tree lock-reacquisition control failed and is being investigated; that combined scanner extension is not promoted by this formatter commit. The formatter has not yet received a new packaged-image or performance claim. Image50 remains the latest accepted selected package checkpoint, with its separately documented four-second Main parity gap.

See `scripts/rust-job-contracts/json-float/` for exact source/reference/proof hashes, peer logs, strict comparisons, corruption tests and reproduction instructions. No frontend or license content changes are part of this fix.
