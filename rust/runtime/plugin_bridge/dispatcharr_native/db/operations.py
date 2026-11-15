"""Django PostgreSQL SQL operations; actual pinned psycopg client adaptation."""
from django.db.backends.postgresql.operations import DatabaseOperations as PostgreSQLOperations
class DatabaseOperations(PostgreSQLOperations):
 def compose_sql(self,sql,params):
  # Django's normal helper constructs a real ClientCursor against libpq. The
  # owned DBAPI exposes the SAME PostgresClientQuery/Transformer algorithm on
  # the actual native session without opening a libpq connection.
  with self.connection.cursor()as cursor:return cursor.mogrify(sql,params)
