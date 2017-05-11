from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event
import datetime

Base= declarative_base()

def validate_int(value):
    if isinstance(value, basestring):
        value = int(value)
    else:
        assert isinstance(value, int)
    return value

def validate_string(value):
    assert isinstance(value, basestring)
    return value

def validate_datetime(value):
    assert isinstance(value, datetime.datetime)
    return value

validators = {
    Integer:validate_int,
    String:validate_string,
    DateTime:validate_datetime,
}

# this event is called whenever an attribute
# on a class is instrumented
@event.listens_for(Base, 'attribute_instrument')
def configure_listener(class_, key, inst):
    if not hasattr(inst.property, 'columns'):
        return
    # this event is called whenever a "set"
    # occurs on that instrumented attribute
    @event.listens_for(inst, "set", retval=True)
    def set_(instance, value, oldvalue, initiator):
        validator = validators.get(inst.property.columns[0].type.__class__)
        if validator:
            return validator(value)
        else:
            return value


class MyObject(Base):
    __tablename__ = 'mytable'

    id = Column(Integer, primary_key=True)
    svalue = Column(String)
    ivalue = Column(Integer)
    dvalue = Column(DateTime)


m = MyObject()
m.svalue = "ASdf"

m.ivalue = "45"

m.dvalue = "not a date"