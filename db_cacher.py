import threading
import time
from collections import namedtuple

from sqlalchemy.engine.base import Engine


class IndividualTTLCache:
    _connections: dict
    _maxsize: int
    _default_ttl: int

    _pool_connection = namedtuple("db_pool", "db_pool life_time")

    def __init__(self, maxsize: int = 10, default_ttl: int = 900):
        self._maxsize = maxsize
        self._default_ttl = default_ttl
        self._connections = {}

        self.cleanup_thread = threading.Thread(target=self.cleanup_expired_items)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()

    def get(self, database_name: str | int) -> Engine:
        """Retrieves a QueuePool from the cache associated with the specified database_name.

        Args:
            database_name (str | int): The database_name to retrieve the QueuePool from the cache.

        Returns:
            Engine: The QueuePool associated with the database_name.
        """
        database_poll = self._connections.get(database_name)

        return database_poll.db_pool if database_poll else None

    def __setitem__(self, database_name: str | int, queue_pool: Engine):
        """Stores a QueuePool in the cache associated with the specified database_name.

        Args:
            database_name (str | int): The database_name to get the TTL for.
            queue_pool (Engine): The QueuePool associated with the database_name
        """
        self._connections[database_name] = self._pool_connection(queue_pool, time.time())

    def __delitem__(self, database_name: str | int):
        """Removes item from cache

        Args:
            database_name (str | int): The database_name to get the TTL for.
        """
        if database_name in self._connections:
            del self._connections[database_name]

    def cleanup_expired_items(self):
        """Checks for connections that have expired and removes them"""
        while True:
            current_time = time.time()

            keys_to_delete = [
                key for key, database_poll in self._connections.items()
                if database_poll.life_time < current_time
            ]

            for key in keys_to_delete:
                self.__delitem__(key)

            time.sleep(60)
