"""Socket-free batch guards for demonstrated provenance/scheduling failure modes."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
import batch as B


def write(path, value):
    path.write_text(json.dumps(value))
    return B.digest(path)


def candidate(root):
    mapping = {'rust/crates/store/src/lib.rs': 'a' * 64, 'rust/Cargo.toml': 'b' * 64, 'coupled-test-path': 'c' * 64}
    map_file = root / 'map.json'
    map_hash = write(map_file, mapping)
    archive = root / 'source.tar'; archive.write_bytes(b'synthetic guard fixture only, not an app archive')
    image = 'sha256:' + 'd' * 64
    git = 'e' * 40
    build = {'passed': True, 'exit_code': 0, 'git_revision': git, 'image_id': image, 'source_archive_sha256': B.digest(archive), 'source_sha256': {**{k: v for k, v in mapping.items() if k.startswith('rust/')}, 'LICENSE': '8486a10c4393cee1c25392769ddd3b2d6c242d6ec7928e1414efff7dfb2f07ef', 'ATTRIBUTION.md': 'f' * 64, 'frontend/src/App.jsx': '1' * 64}}
    build_file = root / 'build.json'
    build_hash = write(build_file, build)
    return {'git': git, 'source_map_file': str(map_file), 'source_map_sha256': map_hash, 'build_manifest_file': str(build_file), 'build_manifest_sha256': build_hash, 'image_id': image, 'source_archive_file': str(archive), 'source_archive_sha256': B.digest(archive)}, build


class BatchTests(unittest.TestCase):
    def test_shared_registry_and_no_oldMain_identity(self):
        matrix, nodes, external = B.load()
        self.assertEqual(len([x for x in external if x.startswith('package/')]), 28)
        self.assertEqual(matrix['dispatch']['global_ordinary_max'], 3)
        self.assertTrue(matrix['dispatch']['Root_global_scheduler_only'])
        self.assertFalse(any('Main55' in x for x in nodes))

    def test_matching_binding_structure_is_ready_not_executed(self):
        with tempfile.TemporaryDirectory() as tmp:
            value, build = candidate(Path(tmp))
            self.assertTrue(B.candidate_ready(value))

    def test_old_image_or_different_source_cannot_be_current(self):
        with tempfile.TemporaryDirectory() as tmp:
            value, build = candidate(Path(tmp))
            build['source_sha256']['rust/crates/store/src/lib.rs'] = '2' * 64
            value['build_manifest_sha256'] = write(Path(value['build_manifest_file']), build)
            self.assertFalse(B.candidate_ready(value))

    def test_old_image_git_cannot_be_current(self):
        with tempfile.TemporaryDirectory() as tmp:
            value, build = candidate(Path(tmp))
            value['git'] = '3' * 40
            self.assertFalse(B.candidate_ready(value))

    def test_missing_archive_cannot_be_ready(self):
        with tempfile.TemporaryDirectory() as tmp:
            value, build = candidate(Path(tmp))
            Path(value['source_archive_file']).unlink()
            self.assertFalse(B.candidate_ready(value))

    def test_unexecuted_missing_capabilities_stay_blocked(self):
        matrix, nodes, external = B.load()
        results = B.state(matrix, {'candidate': {}, 'nodes': {}})
        self.assertTrue(all(v['status'] == 'blocked' and v['execution_performed'] is False for v in results.values()))
        self.assertTrue(all(v['actual_calls'] is None for v in results.values()))

    def test_failed_actual_not_relabelled_by_qualified_observation(self):
        matrix, nodes, external = B.load()
        node = 'tools/M8-transport-materials'
        r = {'execution': {'actual_wait_exit': 1, 'actual_poll_exit': 1, 'stdout_EOF': True, 'stderr_EOF': True, 'physical_settled': True}, 'qualification': {'evaluator_exit': 0, 'semantic_passed': True}, 'cleanup': {'independent_exit': 0, 'exact_owned_absence': True, 'PID_namespace_checked': True}, 'actual_calls': 0, 'evidence': []}
        result = B.state(matrix, {'candidate': {}, 'nodes': {}}, {node: r})
        self.assertEqual(result[node]['status'], 'failed')
        self.assertEqual(result[node]['actual_exit'], 1)
        self.assertEqual(result['source/plugin-TLS-DNS-GPG16']['status'], 'blocked')


if __name__ == '__main__': unittest.main()
