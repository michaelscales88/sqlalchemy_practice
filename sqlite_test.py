import json
import logging
import sqlalchemy as sqla
from sqlalchemy import orm
from sqlalchemy.ext import mutable, declarative


class JsonEncodedDict(sqla.TypeDecorator):
    """Enables JSON storage by encoding and decoding on the fly."""
    impl = sqla.String

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        return json.loads(value)

mutable.MutableDict.associate_with(JsonEncodedDict)


Base = declarative.declarative_base()


class FlexibleStorage(Base):

    __tablename__ = 'flexible_storage'
    id = sqla.Column(sqla.Integer, primary_key=True)
    data = sqla.Column(JsonEncodedDict)

    def __init__(self, content):
        self.content = content


def test():
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    # We set query echoing to True for demonstration purposes
    engine = sqla.create_engine('sqlite://', echo=True)
    Base.metadata.bind = engine
    Base.metadata.create_all()
    session = orm.sessionmaker(bind=engine)()

    bob = FlexibleStorage({'name': 'Bobby'})
    session.add(bob)
    session.commit()
    print(bob)

    bob.surname = 'Selbat'
    bob.age = 5
    session.commit()
    print(bob)
    del bob.surname
    session.flush()
    print(bob)
    bob.age = 19
    session.commit()
    print(bob)
    Base.metadata.reflect()


if __name__ == '__main__':
    test()
