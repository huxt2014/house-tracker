
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


engine = None
Session = None


def init(config, debug=False):
    global engine, Session
    engine = create_engine(config.db_url,
                           echo=debug,
                           connect_args={"charset": "utf8"})
    Session = sessionmaker(bind=engine)
