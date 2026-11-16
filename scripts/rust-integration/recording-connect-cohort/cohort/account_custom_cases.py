"""Same-fixture ordinary account custom-property PATCH/GET conformance.
No startup/bootstrap/provider/media work on import.
"""
import hashlib
import json
import uuid
from pathlib import Path


def run(f):
    out = f.output / 'account-custom-scalar-capture'
    out.mkdir(mode=0o755)
    out.chmod(0o755)
    private = f.output / 'private'
    private.mkdir(mode=0o700, exist_ok=True)
    private.chmod(0o700)
    data = json.loads((Path(__file__).parent / 'source-account-custom-scalars.json').read_text())
    report = {'status': 'running', 'cells': [], 'full_M5_passed': False, 'performance_verified': False,
              'scope': 'admin-owned auxiliary User ordinary custom_properties PATCH/GET; no playback provider-ID admission claim'}
    ident = None

    def save():
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')

    def request(method, path, body=None, actor='admin'):
        # Keep actual responses (potential XC test password/API key projections)
        # PRIVATE; public cells contain hashes/comparison outcomes only.
        before = set(f.output.glob(f.case + '-api-*.json'))
        result = f.api(method, path, body, actor)
        records = set(f.output.glob(f.case + '-api-*.json')) - before
        if len(records) != 1:
            raise RuntimeError('ActualSerialFixtureAPIReceiptRequired')
        original = records.pop()
        target = private / original.name
        original.chmod(0o600)
        original.rename(target)
        return result, {'method': method, 'status': result['status'], 'receipt_sha256': hashlib.sha256(target.read_bytes()).hexdigest(), 'private_receipt': str(target.relative_to(f.output))}

    def properties():
        result, receipt = request('GET', '/api/accounts/users/' + str(ident) + '/')
        if result['status'] != 200:
            raise AssertionError('ActualAuxiliaryUserReadRequired')
        return result['body']['custom_properties'], receipt

    save()
    try:
        result, receipt = request('POST', '/api/accounts/users/', {'username': 'owned-scalars-' + uuid.uuid4().hex, 'password': uuid.uuid4().hex, 'user_level': 0, 'custom_properties': {'owned_preserved_marker': 'retained'}})
        report['create'] = receipt
        save()
        if result['status'] != 201:
            raise AssertionError('ActualAuxiliaryUserAPICreateRequired')
        ident = result['body']['id']
        for record in data['records']:
            row = {'name': record['name'], 'status': 'running', 'actual_Source_validator_accepted': record['accepted']}
            report['cells'].append(row)
            save()
            try:
                before, before_receipt = properties()
                result, receipt = request('PATCH', '/api/accounts/users/' + str(ident) + '/', {'custom_properties': record['input']})
                row['PATCH'] = receipt
                row['before_read'] = before_receipt
                actual, after_receipt = properties()
                row['after_read'] = after_receipt
                expected = dict(before)
                if record['accepted']:
                    for key, value in record['actual_output'].items():
                        if value is None:
                            expected.pop(key, None)
                        else:
                            expected[key] = value
                    # Source28 actual PATCH retains -0.0 in-memory, while the
                    # subsequent JSONB-backed GET returns +0.0. Preserve the
                    # negative-zero request; only this exact stored scalar has
                    # a separately captured persistence expectation.
                    if record['name'] == 'xc-negative-zero-float':
                        value = expected.get('xc_password')
                        if type(value) is not float or json.dumps(value) != '-0.0':
                            raise AssertionError('ExactNegativeZeroSourceVectorRequired')
                        expected['xc_password'] = 0.0
                        row['persistence_authority'] = 'Source28 PATCH -0.0 then GET 0.0; original failed criterion preserved'
                    row['actual_stored_properties_serialization'] = json.dumps(actual, sort_keys=True, separators=(',', ':'))
                    row['expected_stored_properties_serialization'] = json.dumps(expected, sort_keys=True, separators=(',', ':'))
                    row['actual_type_preserving_stored_properties_match'] = json.dumps(actual, sort_keys=True, separators=(',', ':')) == json.dumps(expected, sort_keys=True, separators=(',', ':'))
                    row['actual_Patch_body_properties_match'] = result['body'].get('custom_properties') == actual
                    if result['status'] != 200 or not row['actual_type_preserving_stored_properties_match'] or not row['actual_Patch_body_properties_match']:
                        raise AssertionError('ActualSourceCompatibleCustomScalarPersistence')
                else:
                    expected_error = {'custom_properties': [record['actual_validation_message']]}
                    row['actual_exact_validation_error_match'] = result['body'] == expected_error
                    row['actual_invalid_input_preserves_properties'] = json.dumps(actual, sort_keys=True, separators=(',', ':')) == json.dumps(before, sort_keys=True, separators=(',', ':'))
                    if result['status'] != 400 or not row['actual_exact_validation_error_match'] or not row['actual_invalid_input_preserves_properties']:
                        raise AssertionError('ActualInvalidCustomScalar400NoMutation')
                row['actual_non_target_marker_preserved'] = actual.get('owned_preserved_marker') == 'retained'
                if not row['actual_non_target_marker_preserved']:
                    raise AssertionError('OrdinaryCustomPropertyMergeMustPreserveOtherKeys')
                row['status'] = 'passed'
            except Exception as error:
                row.update(status='failed', error_type=type(error).__name__, error_label=str(error)[:160])
            save()
    except BaseException as error:
        report['primary_failure'] = {'type': type(error).__name__, 'label': str(error)[:160]}
        save()
    finally:
        if ident is not None:
            try:
                result, receipt = request('DELETE', '/api/accounts/users/' + str(ident) + '/')
                report['actual_User_cleanup'] = receipt
                report['actual_owned_User_absent'] = f.sql("from apps.accounts.models import User;print(json.dumps(not User.objects.filter(pk=" + str(ident) + ").exists()))")
                if result['status'] != 204 or not report['actual_owned_User_absent']:
                    raise AssertionError('ActualOwnedUserAPIDeletionRequired')
            except Exception as error:
                report['cleanup_failure'] = type(error).__name__
        report['status'] = 'passed' if len(report['cells']) == len(data['records']) and all(row['status'] == 'passed' for row in report['cells']) and report.get('actual_owned_User_absent') and not report.get('primary_failure') and not report.get('cleanup_failure') else 'failed'
        save()
    return report
