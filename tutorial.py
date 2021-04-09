
from datetime import datetime

import sqlalchemy as db


def get_engine():
    engine = db.create_engine('postgresql://postgres:1234@localhost:5432/dvdrental')
    return engine


def defening_table(engine):
    meta = db.MetaData()
    addresses = db.Table('address', meta, autoload=True, autoload_with=engine)
    example = db.Table('example', meta,
                       db.Column('customer_id', db.INTEGER, primary_key=True),
                       db.Column('store_id', db.SMALLINT),
                       db.Column('first_name', db.VARCHAR),
                       db.Column('last_name', db.VARCHAR),
                       db.Column('email', db.VARCHAR),
                       db.Column('address_id', db.SMALLINT, db.ForeignKey(addresses.c.address_id)),
                       db.Column('activebool', db.BOOLEAN),
                       db.Column('create_date', db.DATE),
                       db.Column('last_update', db.TIMESTAMP),
                       db.Column('active', db.INTEGER))
    meta.create_all(engine)
    return example


def connect_session(engine):
    dbSession = engine.connect()
    print("\nSession: ", dbSession)
    return dbSession


def insert_query(sess, example):
    ins = example.insert().values(
        customer_id=600,
        store_id=2,
        first_name='john',
        last_name='green',
        email='johngreen@mail.com',
        address_id=200,
        activebool=True,
        create_date=datetime.now(),
        last_update=datetime.now(),
        active=1)
    sess.execute(ins)


def update_query(sess, example):
    upt = db.update(example) \
        .where(example.c.customer_id == 600) \
        .values(email='jen@gmail.com', last_update=datetime.now())

    sess.execute(upt)


def delete_query(sess, example):
    dl = db.delete(example).where(example.c.first_name.like('jo%'))
    sess.execute(dl)


def select_query(sess, engine):
    meta = db.MetaData()
    addresses = db.Table('address', meta, autoload=True, autoload_with=engine)

    s = addresses.select()
    print("Select result: ", len(sess.execute(s).fetchall()))


def and_or(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)

    and_sql = db.select([customer]).where(
        db.and_(customer.c.customer_id >= 50, customer.c.address_id < 100, db.not_(customer.c.first_name == 'Karl')))
    print("And:", len(sess.execute(and_sql).fetchall()))

    or_sql = db.select([customer]).where(db.or_(customer.c.customer_id >= 50, customer.c.address_id < 100))
    print("Or:", len(sess.execute(or_sql).fetchall()))


def in_notin(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    notin_sql = db.select([customer]).where(customer.c.first_name.notin_(["Sarah", "John"]))
    print("notin:", len(sess.execute(notin_sql).fetchall()))

    in_sql = db.select([customer]).where(customer.c.first_name.in_(["Sarah", "John"]))
    print("in:", len(sess.execute(in_sql).fetchall()))


def is_null(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    is_sql = db.select([customer]).where(customer.c.email == None)
    print("is_null:", len(sess.execute(is_sql).fetchall()))

    is_not_null_sql = db.select([customer]).where(customer.c.email != None)
    print("is_not_null:", len(sess.execute(is_not_null_sql).fetchall()))


def between_(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    between_sql = db.select([customer]).where(customer.c.address_id.between(10, 20))
    print("between:", len(sess.execute(between_sql).fetchall()))

    not_between_sql = db.select([customer]).where(db.not_(customer.c.address_id.between(10, 20)))
    print("not_between:", len(sess.execute(not_between_sql).fetchall()))


def like_ileke(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    like_sql = db.select([customer]).where(customer.c.email.like("sa%"))
    result = sess.execute(like_sql).fetchall()
    print("like:", len(result))
    for i in result:
        print(i)

    ilike_sql = db.select([customer]).where(customer.c.email.ilike("Sa%"))
    print("ilike:", len(sess.execute(ilike_sql).fetchall()))


def distinct(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    distinct_sql = db.select([customer.c.first_name, customer.c.last_name]).where(
        customer.c.customer_id < 10).distinct()
    result = sess.execute(distinct_sql).fetchall()
    print("like:", len(result))
    for i in result:
        print(i)


def cast_(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    cast_sql = db.select([customer.c.customer_id.cast(db.Integer)])
    print("cast:", len(sess.execute(cast_sql).fetchall()))


def orderby_asc(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    asc_sql = db.select([customer]).where(customer.c.address_id < 25).order_by(db.asc(customer.c.first_name))
    result = sess.execute(asc_sql).fetchall()
    print("orderby_asc:", len(result))
    for i in result:
        print(i)


def orderby_desc(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    desc_sql = db.select([customer]).where(customer.c.address_id < 25).order_by(db.desc(customer.c.first_name))
    result = sess.execute(desc_sql).fetchall()
    print("orderby_asc:", len(result))
    for i in result:
        print(i)


def group_by(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    c = [db.func.count("*").label('count'), customer.c.customer_id]
    group_by_sql = db.select(c).group_by(customer.c.customer_id)
    print(group_by_sql)
    result = sess.execute(group_by_sql).fetchall()
    print("orderby_asc:", len(result))
    for i in result[0:20]:
        print(i)


def join(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    addresses = db.Table('address', db.MetaData(), autoload=True, autoload_with=engine)

    join_sql = db.select([customer.c.first_name, addresses.c.address]).select_from(
        customer.join(addresses, customer.c.address_id == addresses.c.address_id))
    result = sess.execute(join_sql).fetchall()

    print("join:", len(result))
    for i in result[0:20]:
        print(i)


def union(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    union_sql = db.union(
        db.select([customer.c.customer_id, customer.c.first_name]).where(customer.c.first_name.like("%red%")),
        db.select([customer.c.customer_id, customer.c.first_name]).where(customer.c.first_name.like("%die%"))
    ).order_by(db.desc("customer_id"))
    result = sess.execute(union_sql).fetchall()
    print("union:", len(result))
    for i in result[0:20]:
        print(i)


def union_all(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    union_sql = db.union_all(
        db.select([customer.c.customer_id, customer.c.first_name]).where(customer.c.first_name.like("%red%")),
        db.select([customer.c.customer_id, customer.c.first_name]).where(customer.c.first_name.like("%die%"))
    ).order_by(db.desc("customer_id"))
    result = sess.execute(union_sql).fetchall()
    print("union_all:", len(result))
    for i in result[0:20]:
        print(i)


def exists_(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    exists_sql = db.select([db.exists(db.select([customer.c.customer_id]).where(customer.c.address_id == 35))])
    print(exists_sql.compile(compile_kwargs={"literal_binds": True}))
    print("exists_sql:", sess.execute(exists_sql))


def drop_table(engine, example):
    example.drop(engine)
    print("Table was drop....")


def result_proxy(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    result = sess.execute(db.select([customer]))

    print("\nfetchone:", result.fetchone())
    print("\nfetchmany:", result.fetchmany(500))
    print("\nfirst:", result.first())
    print("\nkeys:", result.keys())
    print("\nrowcount:", result.rowcount)

    result = sess.execute(db.select([customer]))
    print("\nfetchall:", len(result.fetchall()))


def convert_to_sql(sess, engine):
    customer = db.Table('customer', db.MetaData(), autoload=True, autoload_with=engine)
    sql = db.select([customer.c.customer_id]).where(customer.c.customer_id == 14)
    print('\nSQLAlchemy:', sql)
    print('\nSQLAlchemy:', sql.compile(compile_kwargs={"literal_binds": True}))


if __name__ == '__main__':
    engine = get_engine()
    before_api_process = datetime.now()

    print("\nEngine: ", engine)
    session = connect_session(engine)

    example = defening_table(engine)

    # insert_query(session, example)
    # update_query(session, example)
    # delete_query(session, example)
    # select_query(session, engine)
    # and_or(session, engine)
    # in_notin(session, engine)
    # is_null(session,engine)
    # between_(session, engine)
    # like_ileke(session, engine)
    # distinct(session, engine)
    # cast_(session,engine)
    # orderby_asc(session,engine)
    # orderby_desc(session, engine)
    # group_by(session, engine)
    # join(session, engine)
    # union(session, engine)
    # union_all(session, engine)
    # exists_(session, engine)
    # drop_table(engine, example)
    # result_proxy(session, engine)
    # convert_to_sql(session, engine)
    engine.dispose()

    print('\nAlchemy', datetime.now() - before_api_process)

'''
Sources
    Sample Datebase: https://www.postgresqltutorial.com/postgresql-sample-database/
    Code: github.com/omertortumlu/SqlAlchemy-Core
    https://docs.sqlalchemy.org/en/13/core/tutorial.html
    https://www.tutorialspoint.com/sqlalchemy/index.htm
    https://overiq.com/sqlalchemy-101/
    
    Book: Essential SQLAlchemy: Mapping Python to Databases Authors: Jason Myers, Rick Copeland
'''
