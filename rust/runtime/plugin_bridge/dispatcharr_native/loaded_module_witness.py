"""Internal observation of the exact source bytes compiled by the action host."""
import hashlib
import itertools
import os
import re
import sys
import threading
import weakref

from .wire import CompatibilityUnavailable

_MAX_BYTES = 1024 * 1024
_MAX_OBSERVED_MODULES = 256
_records = weakref.WeakKeyDictionary()
_sequence = itertools.count(1)
_lock = threading.Lock()


def instrument(loader_module):
    """Wrap the Source loader's existing source read and compilation in this host."""
    original = loader_module._PluginSourceLoader

    class ObservedSource(original):
        def get_code(self, fullname):
            source = self.get_data(self.path)
            code = self.source_to_code(source, self.path)
            if os.path.basename(self.path) == "plugin.py" and len(source) <= _MAX_BYTES:
                module = sys.modules.get(fullname)
                if module is not None and module.__loader__ is self:
                    path = os.path.abspath(self.path)
                    digest = hashlib.sha256(source).hexdigest()
                    with _lock:
                        if len(_records) < _MAX_OBSERVED_MODULES:
                            _records[module] = (path, digest, next(_sequence))
            return code

    loader_module._PluginSourceLoader = ObservedSource


def observe(manager, key):
    """Read an already-loaded module without discovery, ordinary calls or file writes."""
    if not isinstance(key, str) or not re.fullmatch(r"[a-z0-9_]{1,128}", key):
        raise CompatibilityUnavailable("Unsupported installed module identity")
    with manager._lock:
        plugin = manager.get_plugin(key)
        if not plugin or not plugin.loaded or not plugin.trusted or not plugin.instance or not plugin.module:
            raise CompatibilityUnavailable("Trusted module was not already loaded")
        module = plugin.module
        with _lock:
            record = _records.get(module)
        if (record is None or sys.modules.get(module.__name__) is not module
                or not isinstance(plugin.path, str)
                or type(plugin.instance).__module__ != module.__name__
                or record[0] != os.path.join(plugin.path, "plugin.py")
                or manager._registered_task_names(key)):
            raise CompatibilityUnavailable("Actual taskless module identity unavailable")
        metadata = [plugin.name, plugin.version, plugin.description]
        if any(not isinstance(item, str) or len(item) > 65536 for item in metadata):
            raise CompatibilityUnavailable("Unsupported actual plugin metadata")
    path, digest, sequence = record
    return {"actual_pid": os.getpid(), "path": path, "sha256": digest,
            "load_sequence": sequence, "metadata": metadata}
