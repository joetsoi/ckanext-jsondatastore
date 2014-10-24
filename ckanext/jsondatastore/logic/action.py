import sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB

from ckan.plugins.core import get_plugin


def jsondatastore_create(context, data_dict):
    session = context['session']
    jsondatastore_plugin = get_plugin('jsondatastore')
    session.configure(bind=jsondatastore_plugin.write_engine)

    metadata = sqlalchemy.MetaData(bind=jsondatastore_plugin.write_engine)
    sqlalchemy.schema.Table(
        data_dict['resource_id'],
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('data', JSONB),
        sqlalchemy.Column('metadata', JSONB),
    )
    metadata.create_all()
    return True


def jsondatastore_delete(context, data_dict):
    pass


def jsondatastore_insert(context, data_dict):
    session = context['session']
    jsondatastore_plugin = get_plugin('jsondatastore')
    session.configure(bind=jsondatastore_plugin.write_engine)

    table = sqlalchemy.schema.Table(
        data_dict['resource_id'],
        sqlalchemy.MetaData(bind=jsondatastore_plugin.write_engine),
        autoload=True,
        autoload_with=jsondatastore_plugin.write_engine
    )

    connection = session.connection(bind=jsondatastore_plugin.write_engine)
    try:
        trans = connection.begin()
        connection.execute(table.insert(), data=data_dict['data'])
        trans.commit()
    except:
        trans.rollback()
    return True


def jsondatastore_search(context, data_dict):
    session = context['session']
    jsondatastore_plugin = get_plugin('jsondatastore')
    session.configure(bind=jsondatastore_plugin.read_engine)

    table = sqlalchemy.schema.Table(
        data_dict['resource_id'],
        sqlalchemy.MetaData(bind=jsondatastore_plugin.write_engine),
        autoload=True,
        autoload_with=jsondatastore_plugin.write_engine
    )

    connection = session.connection(bind=jsondatastore_plugin.write_engine)
    s = table.select().where(table.c.data.has_key('key1'))
    res = [i for i in connection.execute(s)]

    return res
