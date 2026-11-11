"""UNEXECUTED bounded process collection. Never starts on import.

Overflow/timeout/abort invalidates the attempt. Stopping the entire exact-owned
fixture is intentional on collection failure: killing docker-exec's host client
alone does not acknowledge termination of its container child.
"""
import subprocess
import threading
import time
from pathlib import Path


class CollectionFailure(RuntimeError):
    pass


class Collected:
    def __init__(self, argv, environment, limit, stop_owned, stdout_path=None,
                 stderr_path=None, body=None):
        self.limit, self.stop_owned = limit, stop_owned
        self.lock = threading.Lock()
        self.stop_lock = threading.Lock()
        self.used = 0
        self.invalid = None
        self.stopped = False
        self.physically_settled = False
        self.output = {'stdout': bytearray(), 'stderr': bytearray()}
        self.files = {}
        for kind, path in [('stdout', stdout_path), ('stderr', stderr_path)]:
            if path is not None:
                self.files[kind] = Path(path).open('xb')
        # Body writers are bounded by the caller before start; no secret argv log.
        if body is not None and len(body) > 1024*1024:
            self._close_files()
            raise ValueError('CommandInputCapacity')
        try:
            self.process = subprocess.Popen(argv, env=environment,
                stdin=subprocess.PIPE if body is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except BaseException:
            self._close_files()
            raise
        self.threads = []
        for kind in ['stdout', 'stderr']:
            thread = threading.Thread(target=self._read,
                args=(kind, getattr(self.process, kind)), daemon=True)
            self.threads.append(thread)
            thread.start()
        self.writer = None
        if body is not None:
            self.writer = threading.Thread(target=self._write, args=(body,), daemon=True)
            self.writer.start()

    def _write(self, body):
        try:
            self.process.stdin.write(body)
            self.process.stdin.close()
        except (BrokenPipeError, OSError):
            pass

    def _close_files(self):
        for stream in self.files.values():
            stream.close()

    def _read(self, kind, pipe):
        try:
            while True:
                data = pipe.read1(65536)
                if not data:
                    break
                with self.lock:
                    keep = min(len(data), max(0, self.limit-self.used))
                    if keep:
                        if kind in self.files:
                            self.files[kind].write(data[:keep])
                        else:
                            self.output[kind].extend(data[:keep])
                        self.used += keep
                    if keep != len(data) and self.invalid is None:
                        self.invalid = 'CollectionCapacity'
                # The live reader itself initiates owned stop on overflow, even
                # if a long-lived job is not yet being waited by the controller.
                if keep != len(data):
                    self.abort()
                # Continue draining after the cap; never retain overflow bytes.
        except BaseException as error:
            with self.lock:
                self.invalid = 'CollectionRead'+type(error).__name__
        finally:
            pipe.close()

    def abort(self, reason='CollectionAborted'):
        with self.lock:
            self.invalid = self.invalid or reason
        with self.stop_lock:
            if not self.stopped:
                try:
                    # Must acknowledge container stop before host CLI teardown.
                    self.stop_owned()
                except BaseException:
                    self.invalid = 'OwnedStopUnacknowledged'
                    raise CollectionFailure(self.invalid)
                self.stopped = True
        if self.process.poll() is None:
            self.process.terminate()

    def wait(self, timeout):
        deadline = time.monotonic()+timeout
        try:
            while self.process.poll() is None:
                if self.invalid is not None:
                    self.abort()
                    break
                if time.monotonic() >= deadline:
                    self.abort('CollectionDeadline')
                    break
                time.sleep(.01)
            # A child may exit just before overflow is detected by a pipe reader.
            join_deadline = time.monotonic()+10
            try:
                self.process.wait(timeout=max(.01, join_deadline-time.monotonic()))
            except subprocess.TimeoutExpired:
                self.abort('CollectionJoinDeadline')
                self.process.kill()
                self.process.wait(timeout=5)
            for thread in self.threads + ([self.writer] if self.writer else []):
                thread.join(max(0, join_deadline-time.monotonic()))
            if any(t.is_alive() for t in self.threads) or (self.writer and self.writer.is_alive()):
                self.abort('CollectionPipesNotSettled')
                # Preserve ownership/failure; no successful drain claim.
                raise CollectionFailure('CollectionPipesNotSettled')
            if self.invalid is not None:
                self.abort()
                raise CollectionFailure(self.invalid)
            self.physically_settled = True
            if self.process.returncode:
                raise CollectionFailure('CommandExit'+str(self.process.returncode))
            return bytes(self.output['stdout'])
        except BaseException:
            if not self.physically_settled:
                self.abort()
            raise
        finally:
            # No writer may still own these streams when we close them.
            if not any(t.is_alive() for t in self.threads):
                self._close_files()

    def poll(self):
        return self.process.poll()
