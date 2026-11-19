"""Independently bind the fixed Candidate97 reducer to Candidate102 inputs."""
from hashlib import sha256
import importlib.util
from pathlib import Path

root=Path(__file__).resolve().parent.parent
path=root/'candidate97-regex-preview/verify_attempt.py'
assert sha256(path.read_bytes()).hexdigest()=='0287f1792e89b886c884af093244f0dede8790da63df5364f618da2383bd6931'
spec=importlib.util.spec_from_file_location('fixed_regex_reducer',path)
reducer=importlib.util.module_from_spec(spec)
spec.loader.exec_module(reducer)
reducer.REVISION='7b23e5fa27ba618e476c1b6cde465234cf30b639'
reducer.ARCHIVE='1cd638e3113cfde84a383acc59e33bcc395a5ca302b8d31ca17feafc993a684f'
reducer.OPTIMIZED_IMPORTS={'rust/crates/store/src/stream_regex_preview.rs': '856f442fa23f78c678ad8b1f148498ccc3c5c89d5f70f40001b06733b8b99d26', 'rust/runtime/regex_bridge/dispatcharr_native/stream_preview.py': '5a498e3e25b09b881a7d58fa259867476313853a9fcf5c86a59b96564ee045f5', 'rust/runtime/regex_bridge/dispatcharr_native/regex_only.py': '2ad155570d709fdb6812f83ea87e04a1ab78d86f6cd28d922b05e57447fbbfd6'}
reducer.main()
