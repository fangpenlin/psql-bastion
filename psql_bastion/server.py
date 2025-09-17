import datetime
import functools
import json
import logging
import pathlib
import typing
import uuid
from concurrent.futures import ThreadPoolExecutor

import click
import psycopg
import pyarrow as pa
import pytz
import riffq


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
    # TODO: should this be native uuid type instead? We have some difficulty to convert it to uuid value
    "uuid": pa.string(),
    # TODO: should this be pa.timestamp("ns") instead? We have some difficulty to convert it to datetime value
    "timestamp": pa.timestamp("ns"),
    # TODO: should this be pa.timestamp("ns", tz="UTC") instead? We have some difficulty to convert it to datetime value
    "timestamptz": pa.timestamp("ns", tz="UTC"),
    "date": pa.date32(),
}

logger = logging.getLogger(__name__)


def types_to_json(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


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


def to_pa_types(rows: list) -> typing.Generator[tuple, None, None]:
    for row in rows:
        values = []
        for value in row:
            if isinstance(value, uuid.UUID):
                value = str(value)
            elif isinstance(value, datetime.datetime):
                if value.tzinfo is not None:
                    value = value.astimezone(pytz.UTC)
                else:
                    value = value
            values.append(value)
        yield tuple(values)


class Observer:
    def init(self, conn_id: typing.Any):
        raise NotImplementedError()

    def connected(self, ip: str, port: int):
        raise NotImplementedError()

    def auth(self, user: str, password: str, host: str, database: str = None):
        raise NotImplementedError()

    def query(self, sql: str):
        raise NotImplementedError()

    def query_response(self, columns: list[dict], response: list[tuple]):
        raise NotImplementedError()

    def query_error(self, error: str):
        raise NotImplementedError()

    def disconnected(self, ip: str, port: int):
        raise NotImplementedError()


class AuditTrail(Observer):
    def __init__(self, folder: pathlib.Path):
        self.folder = folder

    def init(self, conn_id: typing.Any):
        self.file = (self.folder / f"{conn_id}.jsonl").open("wt")

    def connected(self, ip: str, port: int):
        self._emit_event(dict(event="CONNECTED", ip=ip, port=port))

    def auth(self, user: str, password: str, host: str, database: str = None):
        self._emit_event(
            dict(event="AUTH", user=user, password=password, host=host, db=database)
        )

    def query(self, sql: str):
        self._emit_event(dict(event="QUERY", sql=sql))

    def query_response(self, columns: list[dict], rows: list[tuple]):
        self._emit_event(dict(event="QUERY_RESPONSE", columns=columns, rows=rows))

    def query_error(self, error: str):
        self._emit_event(dict(event="QUERY_ERROR", error=error))

    def disconnected(self, ip: str, port: int):
        self._emit_event(dict(event="DISCONNECTED", ip=ip, port=port))

    def _emit_event(self, payload: dict):
        self.file.write(json.dumps(payload, default=types_to_json) + "\n")
        self.file.flush()


class Connection(riffq.BaseConnection):
    def __init__(
        self,
        dest_url: str,
        observer: Observer | None,
        conn_id: typing.Any,
        executor: ThreadPoolExecutor,
    ):
        super().__init__(conn_id, executor)
        self.observer = observer
        self.dest_url = dest_url
        if self.observer is not None:
            self.observer.init(conn_id)

    def handle_auth(
        self,
        user: str,
        password: str,
        host: str,
        database: str = None,
        callback: typing.Callable = callable,
    ):
        logger.info(
            "Received auth req, user=%s, host=%s, db=%s",
            user,
            host,
            database,
        )
        if self.observer is not None:
            self.observer.auth(
                user=user, password=password, host=host, database=database
            )
        # TODO: check credentials
        # callback(user == "user" and password == "secret")
        callback(True)

    def handle_connect(self, ip: str, port: int, callback: typing.Callable = callable):
        logger.info("Connected, ip=%s, port=%s", ip, port)
        if self.observer is not None:
            self.observer.connected(ip, port)

        self.outgoing_conn = psycopg.connect(self.dest_url)
        # TODO: run some simple test to ensure the connection is good?
        # allow every incoming connection
        callback(True)

    def handle_disconnect(
        self, ip: str, port: int, callback: typing.Callable = callable
    ):
        logger.info("Disconnect, ip=%s, port=%s", ip, port)
        if self.observer is not None:
            self.observer.disconnected(ip, port)
        # invoked when client disconnects
        callback(True)

    def _handle_query(self, sql: str, callback: typing.Callable, **kwargs):
        logger.info("SQL sql=%r", sql)
        if self.observer is not None:
            self.observer.query(sql=sql)
        try:
            with self.outgoing_conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [
                    # TODO: this fetch_type_info_by_oid is slow, maybe we should cache it or query all at once
                    #       instead?
                    dict(
                        name=desc[0],
                        type=fetch_type_info_by_oid(self.outgoing_conn, desc.type_code),
                    )
                    for desc in cursor.description
                ]
                schema = pa.schema(
                    [
                        (
                            column["name"],
                            PG_TO_ARROW.get(
                                column["type"],
                                pa.string(),
                            ),
                        )
                        for column in columns
                    ]
                )
                # TODO: well... if this is huge, ideally we want to stream the result back to the client
                rows = cursor.fetchall()
                data_columns = list(zip(*to_pa_types(rows)))
                if self.observer is not None:
                    self.observer.query_response(columns=columns, rows=list(rows))
                # Convert data to PyArrow arrays
                arrays = [
                    pa.array(data_columns[i], type=field.type)
                    for i, field in enumerate(schema)
                ]
                record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
                self.send_reader(record_batch, callback)
        except Exception as exc:
            logger.error("Failed to execute SQL", exc_info=True)
            self.observer.query_error(error=str(exc))
            batch = self.arrow_batch(
                # TODO: well, this is from the example. revealing details exc info may not be a good idea from the
                #       perspective of security
                [pa.array(["ERROR"]), pa.array([str(exc)])],
                ["error", "message"],
            )
            self.send_reader(batch, callback)

    def handle_query(self, sql: str, callback: typing.Callable = callable, **kwargs):
        # TODO: be careful with race condition for the audit trail writer
        self.executor.submit(self._handle_query, sql, callback, **kwargs)


@click.command()
@click.argument("DB_URL")
@click.option(
    "--host",
    default="localhost",
    type=str,
    help="The interface to listen connection from",
)
@click.option(
    "--port",
    default=5433,
    type=int,
    help="Port number to listen for incoming connections.",
)
@click.option(
    "--audit-trail",
    type=click.Path(dir_okay=True, file_okay=False, writable=True),
    help="Write audit trial files to the given folder",
)
def main(db_url: str, host: str, port: int, audit_trail: str | None):
    if audit_trail is not None:
        audit_trail = AuditTrail(folder=pathlib.Path(audit_trail))

    server = riffq.RiffqServer(
        f"{host}:{port}",
        connection_cls=functools.partial(Connection, db_url, audit_trail),
    )
    server.start(tls=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
