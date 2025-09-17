import logging
import riffq

logger = logging.getLogger(__name__)

class Connection(riffq.BaseConnection):
    def handle_auth(self, user, password, host, database=None, callback=callable):
        logger.info("Received auth req, user=%s, password=%s, host=%s, db=%s", user, password, host, database)
        # simple username/password check
        callback(user == "user" and password == "secret")

    def handle_connect(self, ip, port, callback=callable):
        logger.info("Connected, ip=%s, port=%s", ip, port)
        # allow every incoming connection
        callback(True)

    def handle_disconnect(self, ip, port, callback=callable):
        logger.info("Disconnect, ip=%s, port=%s", ip, port)
        # invoked when client disconnects
        callback(True)

    def _handle_query(self, sql, callback, **kwargs):
        logger.info("SQL sql=%s", sql)

    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

def main():
    server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
    server.start(tls=False)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()