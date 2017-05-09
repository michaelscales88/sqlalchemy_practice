from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, String, exc,
    and_, not_, or_, func, select
)


def run(stmt):
    rs = stmt.execute()
    for row in rs:
        print(row)


db = create_engine('sqlite:///tutorial.db', echo=False)

metadata = MetaData(db, reflect=True)

try:
    users = Table('users', metadata,
                  Column('user_id', Integer, primary_key=True),
                  Column('name', String(40)),
                  Column('age', Integer),
                  Column('password', String)
                  )
    users.create()
except exc.InvalidRequestError:
    users = Table('users', metadata, autoload=True)

# i = users.insert()
# i.execute(name='Mary', age=30, password='secret')
# i.execute({'name': 'John', 'age': 42},
#           {'name': 'Susan', 'age': 57},
#           {'name': 'Carl', 'age': 33})

# s = users.select()
# rs = s.execute()

# row = rs.fetchone()
# print('Id:', row[0])
# print('Name:', row['name'])
# print('Age:', row.age)
# print('Password:', row[users.c.password])

# for row in rs:
#     print('Id:', row[0])
#     print('Name:', row['name'])
#     print('Age:', row.age)
#     print('Password:', row[users.c.password])


# Most WHERE clauses can be constructed via normal comparisons
s = users.select(users.c.name == 'John')
run(s)

s = users.select(users.c.age < 40)
run(s)

# Python keywords like "and", "or", and "not" can't be overloaded, so
# SQLAlchemy uses functions instead
s = users.select(and_(users.c.age < 40, users.c.name != 'Mary'))
run(s)
s = users.select(or_(users.c.age < 40, users.c.name != 'Mary'))
run(s)
s = users.select(not_(users.c.name == 'Susan'))
run(s)

# Or you could use &, | and ~ -- but watch out for priority!
s = users.select((users.c.age < 40) & (users.c.name != 'Mary'))
run(s)
s = users.select((users.c.age < 40) | (users.c.name != 'Mary'))
run(s)
s = users.select(~(users.c.name == 'Susan'))
run(s)

# There's other functions too, such as "like", "startswith", "endswith"
s = users.select(users.c.name.startswith('M'))
run(s)
s = users.select(users.c.name.like('%a%'))
run(s)
s = users.select(users.c.name.endswith('n'))
run(s)

# The "in" and "between" operations are also available
s = users.select(users.c.age.between(30, 39))
run(s)
# Extra underscore after "in" to avoid conflict with Python keyword
s = users.select(users.c.name.in_('Mary', 'Susan'))
run(s)

# If you want to call an SQL function, use "func"
s = users.select(func.substr(users.c.name, 2, 1) == 'a')
run(s)

# You don't have to call select() on a table; it's got a bare form
s = select([users], users.c.name != 'Carl')
run(s)
s = select([users.c.name, users.c.age], users.c.name != 'Carl')
run(s)

# This can be handy for things like count()
s = select([func.count(users.c.user_id)])
run(s)
# Here's how to do count(*)
s = select([func.count("*")], from_obj=[users])
run(s)
