# Observed M3U and XMLTV output on Rust revision 9cf6407

The portable packet contains 19 M3U and 13 XMLTV comparisons per backend, all under the anonymous role. It records the exact request target, method, response status and entity digest for each measured case. The named M3U cases include exact observed headers and body shapes. The M3U GET block carries the verified selected-wire equality result from the paired observer. The XMLTV block records the shared headers, channel IDs, programme counts and bodyless HEAD results.

From a checkout, run `python3 -I -B verify.py` in this directory. The verifier uses this packet and the published M7/M8 summary from the adjacent directory. In a standalone copy, pass `--published-summary PATH` pointing to the original tracked summary file. It validates the exact application and image, both physical exit and cleanup records, the request rosters, and the paired response comparisons without Docker. [Published M7/M8 evidence](../../../docs/rust-m7-m8-9cf6407-ordinary-compose.md)

The first M3U observer exit remains 1. Its 15 Original GETs passed before the second callback reopened a single-use snapshot. Rust never started in that first attempt. Independent cleanup verified that the two started generations retired. The corrected paired attempt completed on the same image.

The published register assigns the M3U and XMLTV routes to resolver ordinals 362 and 363 with unresolved methods. The packet records the methods observed on those routes. Wider route verification and M7 acceptance remain open.

Original Dispatcharr and dependency contributors retain full credit.
