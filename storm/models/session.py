from sqlalchemy.orm import sessionmaker

from .engine import engine

Session = sessionmaker(bind=engine)


def get_session():
    return Session()
