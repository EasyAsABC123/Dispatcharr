# AMD64 signed catalogue and guide inventory replay

The ordinary Linux AMD64 package for application 0596767d65b889424d529670cc7301b02564071b passed two isolated Docker Compose replays covering 50 distinct GET contracts and 134 HTTP observations. The current candidate used fresh stock fixture data in each scope and compared its responses with the retained, independently checked Original responses from 1241b3f7. The exact ordinary candidate manifest is 31b96706c8c5581f46a725ea41356e7bdb0f4b8696de1b65c7f7d47aebbc8ea7. [Machine-readable evidence](../scripts/rust-integration/m0-0596767d-amd64-signed-read-cohorts.json)

| Owned scope | Observations | Results |
| --- | --- | --- |
| Catalogue, 34 GET contracts | 68 GET, 17 HEAD, 17 OPTIONS | All returned HTTP 200 and matched the selected representation and header checks. Raw bodies matched for 56 GET and all 17 OPTIONS. Ten designated OPTIONS bodies also matched their independently retained stock metadata fixture. Every Rust HEAD had an empty body. |
| Channel, stream and logo inventory, 16 GET contracts | 32 GET | All returned HTTP 200 and matched typed JSON, selected headers and the owned SQL data. The provider URL remained absent. Raw bodies matched for 26 cases, including the 184-byte Guide summary and 31-byte Stream Groups response for both signed roles. |

The twelve catalogue GET and six inventory logo GET raw-body differences repeated the prior 381a scope's same cases, byte counts and matching typed JSON. Selected headers, including recorded Content-Length, matched. The retained Original HEAD comparator was a direct uWSGI listener that returned entity bytes in all 17 cases. These results establish the recorded typed GET parity, the specified raw body checks and the candidate's bodyless HEAD behavior.

Both candidate cohorts retained their scoped SQL and broker state and retired four owned container roles and their process generations. Each Compose teardown completed with zero remaining owned containers, networks or volumes. The retained Original receipts also recorded exact cleanup. Every preflight, execution, cleanup and proof exited zero, including the independent combined reducer. Formal M0, performance and release approval remain open. Original Dispatcharr authors and contributors retain full application and protocol credit.
