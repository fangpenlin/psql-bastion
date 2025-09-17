import logging
import riffq
import psycopg
import pyarrow as pa
from psycopg.types import TypeInfo

logger = logging.getLogger(__name__)

PG_TO_ARROW = {
    'int4': pa.int32(),
    'text': pa.string(),
    'float8': pa.float64(),
    'bool': pa.bool_(),
    'timestamp': pa.timestamp('ns')
}
DB_URL = 'postgres://infisical:infisical@localhost:5432/infisical'

class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        logger.info("Received auth req, user=%s, password=%s, host=%s, db=%s", user, password, host, database)
        # TODO: check credentials
        # callback(user == "user" and password == "secret")
        callback(True)

    def handle_connect(self, ip, port, callback=callable):
        logger.info("Connected, ip=%s, port=%s", ip, port)

        self.outgoing_conn = psycopg.connect(DB_URL)
        # allow every incoming connection
        callback(True)

    def handle_disconnect(self, ip, port, callback=callable):
        logger.info("Disconnect, ip=%s, port=%s", ip, port)
        # invoked when client disconnects
        callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        logger.info("SQL sql=%s", sql)
        with self.outgoing_conn.cursor() as cursor:
            cursor.execute(sql)
            schema = pa.schema([
                (desc[0], PG_TO_ARROW.get(TypeInfo.fetch(self.outgoing_conn, desc.type_code).name, pa.string()))
                for desc in cursor.description
            ])
            # TODO: well... if this is huge, ideally we want to stream the result back to the client
            rows = cursor.fetchall()
            data_columns = list(zip(*rows))
            # Convert data to PyArrow arrays
            arrays = [
                pa.array(data_columns[i], type=field.type)
                for i, field in enumerate(schema)
            ]
            record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
            self.send_reader(record_batch, callback)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

def main():
    server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
    server.start(tls=False)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()