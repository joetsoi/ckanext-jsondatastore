"""Tests for plugin.py."""
from ckan.new_tests import helpers
import ckanext.jsondatastore.plugin as plugin

class TestJsonDatastore(object):
    def setup(self):
        helpers.reset_db()

    def test_plugin(self):
        helpers.call_action('jsondatastore_create', resource_id='test')
        helpers.call_action('jsondatastore_insert', resource_id='test',
                            data={"key1": "value1", "key2": "value2"})

        helpers.call_action('jsondatastore_search', resource_id='test')
