"""Phase-local completed-decode evidence reuse; Original/FFmpeg credit retained."""
import copy
import hashlib
import json
import os
from pathlib import Path
import stat


def custody(path, root):
    path, root = Path(path), Path(root).resolve()
    if path.is_symlink() or path.resolve() != path.absolute() or path.suffix != '.ts':
        raise ValueError('OwnedRegularTSRequired')
    path.relative_to(root)
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('OwnedRegularTSRequired')
        digest = hashlib.sha256()
        for block in iter(lambda: stream.read(65536), b''):
            digest.update(block)
        after = os.fstat(stream.fileno())
    fields = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if fields(before) != fields(after) or fields(after) != fields(path.stat()):
        raise ValueError('MediaChangedDuringCustody')
    return {'path': str(path), 'sha256': digest.hexdigest(), 'bytes': after.st_size,
            'generation': list(fields(after))}


def settled(receipt):
    return (type(receipt.get('actual_exit')) is int and receipt['actual_exit'] == 0
            and receipt.get('failure') is None
            and isinstance(receipt.get('command_receipt'), dict)
            and type(receipt['command_receipt'].get('actual_exit')) is int
            and receipt['command_receipt']['actual_exit'] == 0
            and receipt['command_receipt'].get('failure') is None)


def physical_artifacts(receipt):
    command = receipt['command_receipt']
    for stream in ('stdout', 'stderr'):
        digest = hashlib.sha256()
        size = 0
        with Path(command[stream]).open('rb') as source:
            for block in iter(lambda: source.read(65536), b''):
                digest.update(block)
                size += len(block)
        if (digest.hexdigest() != receipt[stream + '_sha256']
                or size != command[stream + '_bytes']
                or (stream == 'stderr' and size != receipt['stderr_bytes'])):
            raise ValueError('PhysicalCommandArtifactChanged:' + stream)


def outcome(record):
    return record['physical_authority'] if record.get('kind') == 'reused_decode' else record


class DecodeMemo:
    def __init__(self, root, identity, max_entries, max_bytes=8 * 1024 * 1024):
        if not identity.get('image_id') or not identity.get('ffmpeg_sha256') or not identity.get('argv_environment'):
            raise ValueError('ActualDecoderIdentityRequired')
        self.root = Path(root).resolve()
        self.identity = copy.deepcopy(identity)
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.bytes = 0
        self.entries = {}

    def decode(self, path, argv, invoke):
        before = custody(path, self.root)
        # Only the single owned input path is substituted. All other argv bytes
        # (including executable, stream maps, codecs and execution prefix) bind.
        local = '/evidence/' + str(Path(path).relative_to(self.root))
        if argv.count(local) != 1 or argv[argv.index(local) - 1] != '-i':
            raise ValueError('ExactSingleOwnedInputRequired')
        options = list(argv)
        options[options.index(local)] = '<verified-owned-ts-content>'
        key = json.dumps([before['sha256'], before['bytes'], self.identity, options], sort_keys=True)
        cached = self.entries.get(key)
        if cached:
            receipt, text, receipt_path, receipt_sha = cached
            physical_artifacts(receipt)
            # Evidence files must still be exact; never accept a stale in-memory
            # authority after its on-disk physical proof/transcript was altered.
            proof = Path(receipt_path)
            if hashlib.sha256(proof.read_bytes()).hexdigest() != receipt_sha:
                raise ValueError('PhysicalDecodeReceiptChanged')
            if hashlib.sha256(Path(receipt['stdout_path']).read_bytes()).hexdigest() != receipt['stdout_sha256']:
                raise ValueError('PhysicalDecodeTranscriptChanged')
            if custody(path, self.root) != before:
                raise ValueError('MediaChangedDuringReuse')
            return {'kind': 'reused_decode', 'media_sha256': before['sha256'],
                    'media_bytes': before['bytes'], 'input_custody': before,
                    'authority_receipt_path': receipt_path, 'authority_receipt_sha256': receipt_sha,
                    'physical_authority': copy.deepcopy(receipt),
                    'decoder_identity': copy.deepcopy(self.identity),
                    'new_physical_command_executed': False}, text
        receipt, text = invoke()
        after = custody(path, self.root)
        if before != after or receipt.get('media_sha256') != before['sha256'] or receipt.get('media_bytes') != before['bytes']:
            raise ValueError('MediaChangedDuringPhysicalDecode')
        # Failure/incomplete outputs are returned honestly but cannot seed reuse.
        receipt_path = Path(path).with_suffix(Path(path).suffix + '.decode.json')
        size = len(text.encode('utf8'))
        if settled(receipt) and receipt['command_receipt'].get('argv') == argv and receipt_path.is_file() and len(self.entries) < self.max_entries and self.bytes + size <= self.max_bytes:
            if hashlib.sha256(Path(receipt['stdout_path']).read_bytes()).hexdigest() != receipt['stdout_sha256'] or hashlib.sha256(text.encode('utf8')).hexdigest() != receipt['stdout_sha256']:
                raise ValueError('PhysicalDecodeTranscriptChanged')
            physical_artifacts(receipt)
            persisted = json.loads(receipt_path.read_text())
            if persisted != receipt:
                raise ValueError('PhysicalDecodeReceiptMismatch')
            self.entries[key] = (copy.deepcopy(receipt), text, str(receipt_path), hashlib.sha256(receipt_path.read_bytes()).hexdigest())
            self.bytes += size
        return receipt, text
