from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, String, exc,
    JSON, TypeDecorator, inspect
)
from sqlalchemy.ext import mutable, declarative, hybrid
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.sql import case
from json import loads, dumps


def run(stmt):
    rs = stmt.execute()
    for row in rs:
        print(row)


class JsonEncodedDict(TypeDecorator):
    """Enables JSON storage by encoding and decoding on the fly."""
    impl = String

    def process_bind_param(self, value, dialect):
        return dumps(value)

    def process_result_value(self, value, dialect):
        return loads(value)

# mutable.MutableDict.associate_with(JsonEncodedDict)   # Toggle this to make the JSON data mutable

Base = declarative.declarative_base()


class FlexibleStorage(Base):

    __tablename__ = 'flexible_storage'

    id = Column(Integer, primary_key=True)
    data = Column(JsonEncodedDict)

    def __repr__(self):
        return "<flexible_storage (id={id}, data={data})>".format(id=self.id, data=self.data)


def main():
    # create engine
    db = create_engine('sqlite:///tutorial.db', echo=False)
    Base.metadata.bind = db
    Base.metadata.create_all()      # This has to be created before making the session

    # create a configured "Session" class
    Session = sessionmaker(bind=db)
    session = Session()

    for x in range(10):
        session.add(
            FlexibleStorage(
                data={'key{}'.format(x + 2): 'Value{}'.format(x - 1),
                      'key2{}'.format(x + 3): 'value{}'.format(x + 5)}
            )
        )
    session.commit()

    for row in session.query(FlexibleStorage).all():
        print(row)

    # Tested below doesn't work without enabling making the record mutable
    for row in session.query(FlexibleStorage).filter(FlexibleStorage.id == 1):
        print(row)
        row.data['key23'] = 'something else'
    session.commit()

    for row in session.query(FlexibleStorage).filter(FlexibleStorage.id == 1):
        print(row)

if __name__ == '__main__':
    main()
