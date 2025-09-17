import logging
import typing

import click
import psycopg
import pyarrow as pa
import riffq

logger = logging.getLogger(__name__)

PG_TO_ARROW = {
    "int2": pa.int16(),
    "int4": pa.int32(),
    "int8": pa.int64(),
    "float4": pa.float32(),
    "float8": pa.float64(),
    "text": pa.string(),
    "varchar": pa.string(),
    "char": pa.string(),
    "bool": pa.bool_(),
    "timestamp": pa.timestamp("ns"),
    "date": pa.date32(),
}
DB_URL = "postgres://infisical:infisical@localhost:5432/infisical"


def fetch_type_info_by_oid(conn, oid):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.typname
            FROM pg_type t
            WHERE t.oid = %s
        """,
            (oid,),
        )
        return cur.fetchone()[0]


class Connection(riffq.BaseConnection):
    def handle_auth(
        self,
        user: str,
        password: str,
        host: str,
        database: str = None,
        callback: typing.Callable = callable,
    ):
        logger.info(
            "Received auth req, user=%s, password=%s, host=%s, db=%s",
            user,
            password,
            host,
            database,
        )
        # TODO: check credentials
        # callback(user == "user" and password == "secret")
        callback(True)

    def handle_connect(self, ip: str, port: int, callback: typing.Callable = callable):
        logger.info("Connected, ip=%s, port=%s", ip, port)

        self.outgoing_conn = psycopg.connect(DB_URL)
        # allow every incoming connection
        callback(True)

    def handle_disconnect(
        self, ip: str, port: int, callback: typing.Callable = callable
    ):
        logger.info("Disconnect, ip=%s, port=%s", ip, port)
        # invoked when client disconnects
        callback(True)

    def _handle_query(self, sql: str, callback: typing.Callable, **kwargs):
        logger.info("SQL sql=%r", sql)
        try:
            with self.outgoing_conn.cursor() as cursor:
                cursor.execute(sql)
                schema = pa.schema(
                    [
                        # TODO: this fetch_type_info_by_oid is very slow, maybe we should cache it or query all at once
                        #       instead?
                        (
                            desc[0],
                            PG_TO_ARROW.get(
                                fetch_type_info_by_oid(
                                    self.outgoing_conn, desc.type_code
                                ),
                                pa.string(),
                            ),
                        )
                        for desc in cursor.description
                    ]
                )
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
        except Exception as exc:
            logger.error("Failed to execute SQL", exc_info=True)
            batch = self.arrow_batch(
                # TODO: well, this is from the example. revealing details exc info may not be a good idea from the
                #       perspective of security
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                ["error", "message"],
            )
            self.send_reader(batch, callback)

    def handle_query(self, sql: str, callback: typing.Callable = callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)


@click.command()
@click.argument("DB_URL")
@click.option(
    "--host", default="localhost", help="The interface to listen connection from"
)
@click.option(
    "--port", default=5433, help="Port number to listen for incoming connections."
)
def main(db_url: str, host: str, port: int):
    server = riffq.RiffqServer(f"{host}:{port}", connection_cls=Connection)
    server.start(tls=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
