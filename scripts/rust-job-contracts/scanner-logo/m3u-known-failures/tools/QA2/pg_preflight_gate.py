"""Unexecuted bounded protocol gate for the existing owned PG relay.
No sockets/processes/import-time execution. Never log credentials or Bind values.
The future fixture plugs frontend()/backend() into its exact owned relay and
holds ONLY the returned True Ready, after recording it, until Root-reviewed
owned-trigger installation. This helper itself grants no execution authority.
"""
import hashlib
import threading

class Gate:
    def __init__(self, target_sql_sha256):
        if type(target_sql_sha256) is not str or len(target_sql_sha256) != 64:
            raise ValueError('ExactReviewedSQLHash')
        self.target = target_sql_sha256
        self.statements = set()
        self.portals = set()
        self.active = False
        self.rows = 0
        self.false_row = False
        self.complete = False
        self.armed = True
        self.held = False
        self.events = []
        self.lock = threading.RLock()
    def record(self, kind):
        if len(self.events) >= 128:
            raise ValueError('GateEventCapacity')
        self.events.append(kind)
    @staticmethod
    def name(value):
        if len(value) > 64 or b'\0' in value:
            raise ValueError('ProtocolNameCapacity')
        return value
    def frontend(self, kind, body):
        if type(body) is not bytes or len(body) > 2097152:
            raise ValueError('FrontendFrameCapacity')
        with self.lock:
            if kind == b'P':
                name, sql, _ = body.split(b'\0', 2)
                self.name(name)
                self.statements.discard(name)
                if hashlib.sha256(sql).hexdigest() == self.target:
                    self.statements.add(name)
                    self.record('matched_Parse')
            elif kind == b'B':
                portal, statement, _ = body.split(b'\0', 2)
                self.name(portal); self.name(statement)
                self.portals.discard(portal)
                if statement in self.statements:
                    self.portals.add(portal)
            elif kind == b'E':
                portal, rest = body.split(b'\0', 1)
                self.name(portal)
                if len(rest) != 4:
                    raise ValueError('ExecuteShape')
                self.active = portal in self.portals
                self.rows, self.false_row, self.complete = 0, False, False
                if self.active:
                    self.record('matched_Execute')
            if len(self.statements) > 4 or len(self.portals) > 4:
                raise ValueError('OwnedTargetNameCapacity')
    def backend(self, kind, body):
        if type(body) is not bytes or len(body) > 2097152:
            raise ValueError('BackendFrameCapacity')
        with self.lock:
            if self.active and kind == b'E':
                raise ValueError('UnexpectedPreflightErrorResponse')
            if self.active and kind == b'D':
                self.rows += 1
                # Exact one false bool; PostgreSQL text or binary bool format.
                self.false_row = body in (b'\x00\x01\x00\x00\x00\x01f', b'\x00\x01\x00\x00\x00\x01\x00')
                self.record('matched_false_DataRow' if self.false_row else 'unexpected_DataRow')
            if self.active and kind == b'C':
                self.complete = body == b'SELECT 1\0'
                self.record('matched_CommandComplete' if self.complete else 'unexpected_CommandComplete')
            if kind == b'Z':
                hold = self.armed and self.active and self.rows == 1 and self.false_row and self.complete and body == b'I'
                self.active = False
                if hold:
                    self.armed = False
                    self.held = True
                    self.record('hold_matching_success_Ready')
                return hold
            return False
