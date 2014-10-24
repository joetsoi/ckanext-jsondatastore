import sqlalchemy
import sqlalchemy.engine.url as sa_url

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.jsondatastore.logic.action import (
    jsondatastore_create,
    jsondatastore_delete,
    jsondatastore_insert,
    jsondatastore_search,
)


class JsonDatastoreError(Exception):
    pass


class JsondatastorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)

    def update_config(self, config_):
        #toolkit.add_template_directory(config_, 'templates')
        #toolkit.add_public_directory(config_, 'public')
        #toolkit.add_resource('fanstatic', 'jsondatastore')

        if not 'ckanext.jsondatastore.write.url' in config_:
            error_message = 'jsonckan.datastore.write_url not found in config'
            raise JsonDatastoreError(error_message)

        ckan_db = config_['sqlalchemy.url']
        self.write_url = config_['ckanext.jsondatastore.write.url']

        if self.write_url == ckan_db:
            raise JsonDatastoreError('The json datastore url cannot be the '
                                     'same as the main ckan database')


        self.write_engine = sqlalchemy.engine_from_config(
            config_,
            prefix='ckanext.jsondatastore.write.',
            client_encoding='utf8',
        ) 

        self.read_engine = sqlalchemy.engine_from_config(
            config_,
            prefix='ckanext.jsondatastore.read.',
            client_encoding='utf8',
        ) 

    def get_actions(self):
        return {
            'jsondatastore_create': jsondatastore_create,
            'jsondatastore_delete': jsondatastore_delete,
            'jsondatastore_insert': jsondatastore_insert,
            'jsondatastore_search': jsondatastore_search,
        }
