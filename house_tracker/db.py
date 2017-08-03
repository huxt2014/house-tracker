
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


engine = None
session_factory = None


def Session(config):
    global engine, session_factory
    if not engine:
        engine = create_engine(config.db_url,
                               connect_args={"charset": "utf8"})
    if not session_factory:
        session_factory = sessionmaker(bind=engine)

    return session_factory()
