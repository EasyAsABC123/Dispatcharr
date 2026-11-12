"""Corruption controls for captured finite-helper evidence; no service claims."""
import copy
import json
import unittest
import verify


def row():
    compact = '{"float":0.0,"label":"é𝄞"}'
    return {"bits": "0000000000000000", "repr": "0.0", "repr_length": 3,
            "compact": compact, "compact_size": len(compact.encode())}


class EvidenceControls(unittest.TestCase):
    def setUp(self):
        self.reference = {"passed": True, "rows": [row()]}
        self.actual = [row()]

    def test_valid_typed_fields(self):
        self.assertEqual(verify.compare(self.reference, self.actual)["strict_typed_field_comparisons"], 5)

    def rejected(self, reason):
        with self.assertRaisesRegex(ValueError, reason):
            verify.compare(self.reference, self.actual)

    def test_failed_source(self):
        self.reference["passed"] = 1
        self.rejected("SourceCaptureFailed")

    def test_empty_corpus(self):
        self.reference["rows"] = []
        self.actual = []
        self.rejected("CaseCount")

    def test_missing_case(self):
        self.actual = []
        self.rejected("CaseCount")

    def test_field_shape(self):
        self.actual[0]["unqualified"] = True
        self.rejected("FieldSet")

    def test_boolean_not_integer(self):
        self.actual[0]["repr_length"] = True
        self.rejected("FieldType")

    def test_canonical_bits(self):
        self.actual[0]["bits"] = "0X00000000000000"
        self.rejected("CanonicalBits")

    def test_nonfinite(self):
        self.actual[0]["bits"] = "7ff0000000000000"
        self.rejected("FiniteOnly")

    def test_repr_length(self):
        self.actual[0]["repr_length"] += 1
        self.rejected("ReprLength")

    def test_unicode_bytes_not_characters(self):
        self.actual[0]["compact_size"] = len(self.actual[0]["compact"])
        self.rejected("UTF8Size")

    def test_duplicate_source(self):
        self.reference["rows"] *= 2
        self.actual *= 2
        self.rejected("DuplicateSourceBits")

    def test_identity_order(self):
        self.actual[0]["bits"] = "8000000000000000"
        self.rejected("CaseIdentityOrder")

    def test_same_length_wrong_digits_fail(self):
        self.actual[0]["repr"] = "1.0"
        result = verify.compare(self.reference, self.actual)
        self.assertFalse(result["passed"])
        self.assertEqual(result["differences"][0]["different_fields"], ["repr"])

    def test_same_size_wrong_payload_fails(self):
        self.actual[0]["compact"] = self.actual[0]["compact"].replace("0.0", "1.0")
        self.assertFalse(verify.compare(self.reference, self.actual)["passed"])

    def test_unique_peer_payload_and_test_pass(self):
        raw = ("FINITE_FLOAT_PEER_JSON=" + json.dumps(self.actual) +
               "\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n").encode()
        self.assertEqual(verify.peer_rows(raw), self.actual)
        with self.assertRaisesRegex(ValueError, "UniquePeerPayloadRequired"):
            verify.peer_rows(raw + raw)
        with self.assertRaisesRegex(ValueError, "ExactlyOnePeerTestRequired"):
            verify.peer_rows(raw.replace(b"1 passed", b"0 passed"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
