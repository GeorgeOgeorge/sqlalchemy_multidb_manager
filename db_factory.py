import logging

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from .db_cacher import IndividualTTLCache


class DatabaseManager:

    def __init__(
        self,
        main_db: str,
        db_url: str,
        main_db_poolsize: int | None = 15,
        main_db_recycle: int | None = 900,
        regular_db_poolsize: int | None = 5,
        regular_db_recycle: int | None = 300,
    ):
        """Initialize a DatabaseManager instance

        Args:
            main_db (str): The name of the main database.
            db_url (str): Database connection Url.
            main_db_poolsize (int | None, optional): The pool size for the main database. Defaults to 16.
            main_db_recycle (int | None, optional): The recycle time for the main database pool. Defaults to 900.
            regular_db_poolsize (int | None, optional): The pool size for regular databases. Defaults to 5.
            regular_db_recycle (int | None, optional): The recycle time for regular database pools. Defaults to 300.
        """
        self.db_cacher = IndividualTTLCache()
        self.main_db = main_db
        self.db_url = db_url
        self._main_db_poolsize = main_db_poolsize or 15
        self._main_db_recycle = main_db_recycle or 900
        self._regular_db_poolsize = regular_db_poolsize or int(main_db_poolsize/3),
        self._regular_db_recycle = regular_db_recycle or int(main_db_recycle/3),

    async def create_session(self, db_name: str) -> Session:
        """ Creates and returns a new SQLAlchemy session for a specified database.
            This function utilizes a cache mechanism to reuse existing database connection pools for improved efficiency

        Args:
            db_name (str): The name of the database to connect to.

        Returns:
            Session: A SQLAlchemy session connected to the specified database.

        Note:
            The function checks if a database connection pool for the specified `db_name` exists in the cache.
            If a cached pool is found, it reuses the connection pool to create a session.
            If not, it creates a new connection pool for the database, adds it to the cache.
            Then creates a session using the new or existing pool.

        """
        cached_db_pool = self.db_cacher.get(db_name)
        if cached_db_pool:
            return await self._create_pool_connection(cached_db_pool)

        db_pool = create_engine(
            url=f"{self.db_url}/{db_name}",
            pool_size=self._main_db_poolsize if db_name == self.main_db else self._regular_db_poolsize[0],
            pool_recycle=self._main_db_recycle if db_name == self.main_db else self._regular_db_recycle[0],
            max_overflow=10,
            pool_pre_ping=True,
            hide_parameters=True,
        )

        self.db_cacher[db_name] = db_pool

        return await self._create_pool_connection(db_pool)

    async def _create_pool_connection(self, db_pool: Engine) -> Session:
        """Creates and returns a new SQLAlchemy session from a database pool.

        Args:
            db_pool (Engine): The database pool from which to create the connection.

        Raises:
            SQLAlchemyError: If an SQLAlchemy-related error occurs while creating the connection.
            Exception: If a generic error occurs while creating the connection.

        Returns:
            Session: A newly created SQLAlchemy session.
        """
        try:
            engine = db_pool.connect()
            session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            new_session = session_maker()
        except SQLAlchemyError as connection_error:
            logging.error(f"error while creating database connection from pool: {connection_error}")
            raise connection_error
        except Exception as generic_error:
            logging.error(f"error raise while creating database connection from pool: {generic_error}")
            raise generic_error
        else:
            return new_session
