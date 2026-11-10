# Generic null/nonobject request contracts — post image21

Original Dispatcharr authors and contributors retain full application credit and AGPL licensing. The original serializers and views are authoritative, including their failure behavior.

The actual source fixture captures141 real HTTP cases across UserAgent, StreamProfile, OutputProfile, Logo, users, groups, channel groups/profiles/channels/streams, notifications and owner `users/me`. It checks POST/PUT/PATCH JSONnull, list/bool/float/string bodies, role denials and missing-object precedence. Sixteen protected model/through tables retain identical fingerprints before and after invalid requests.

The [before/after summary](../rust-integration/verified-serializer-null-source-native-after-1.json) records44 native response mismatches before correction and zero afterward. All141 source/native cases pass, with zero protected-table mutations and owned Compose cleanup. Exact source/tool/executable/report hashes are retained. This is a frozen host-component proof, not an image21 claim or a performance measurement.

Generic ModelSerializer null input returns400 `non_field_errors: ["No data provided"]`. Other nonobject inputs preserve their type-specific error. Users/groups now resolve a missing object before validating its body. Authentication and permissions still precede these checks.

Owner `users/me` differs because the original calls `.keys()` before its serializer. Seven authenticated null/nonobject cases produce the exact145-byte default Django HTML500; inactive/anonymous requests still return401. Native code preserves this literal outcome without performing a model write. The change does not pretend the original accepts or repairs those bodies.

Administrative notification invalid input and missing-object responses now follow the source before reaching the existing write guard. Valid administrative notification production/scheduling remains501; no event or job success is synthesized.

Run the guarded source discovery with `run_serializer_null_contracts.py --output <empty-owned-directory>` and the actual native comparison with `run_serializer_null_native.py --output <empty-owned-directory>`. Raw ignored fixtures contain temporary JWTs; committed summaries contain only safe categories and hashes. Full1024 and packaged regression must run against the next canonical image before these post21 changes are accepted there. Malformed JSON/MIME and custom action-route bodies remain separate families.
