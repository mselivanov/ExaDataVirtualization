import unittest

from src import exa_impala_adapter


class ExaImpalaAdapterTest(unittest.TestCase):
    def test_check_request(self):
        # Check exception on unknown request type
        self.assertRaises(ValueError, exa_impala_adapter.check_request, {u'schemaMetadataInfo': {u'name': u'EXA_PROXY',
                                                                                                 u'properties': {
                                                                                                     u'META_CONNECTION': u'SYS_CONNECTION',
                                                                                                     u'TABLE_SCHEMA': u'SANDBOX',
                                                                                                     u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'}},
                                                                         u'type': u'mockVirtualSchema'})

        # Check exception on metadata section absence
        self.assertRaises(ValueError, exa_impala_adapter.check_request, {u'schemaInfo': {u'name': u'EXA_PROXY',
                                                                                         u'properties': {
                                                                                             u'META_CONNECTION': u'SYS_CONNECTION',
                                                                                             u'TABLE_SCHEMA': u'SANDBOX',
                                                                                             u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'}},
                                                                         u'type': u'mockVirtualSchema'})
        # Check exception on schema name absence
        self.assertRaises(ValueError, exa_impala_adapter.check_request, {u'schemaInfo': {u'properties': {
            u'META_CONNECTION': u'SYS_CONNECTION',
            u'TABLE_SCHEMA': u'SANDBOX',
            u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'}},
            u'type': u'mockVirtualSchema'})

        # Check exception on meta connection absence
        self.assertRaises(ValueError, exa_impala_adapter.check_request, {u'schemaMetadataInfo': {u'name': u'EXA_PROXY',
                                                                                                 u'properties': {
                                                                                                     u'TABLE_SCHEMA': u'SANDBOX',
                                                                                                     u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'}},
                                                                         u'type': u'createVirtualSchema'})

        # Check exception on table schema absence
        self.assertRaises(ValueError, exa_impala_adapter.check_request, {u'schemaMetadataInfo': {u'name': u'EXA_PROXY',
                                                                                                 u'properties': {
                                                                                                     u'META_CONNECTION': u'SYS_CONNECTION',
                                                                                                     u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'}},
                                                                         u'type': u'createVirtualSchema'})
        # Check exception on websockets path absence
        self.assertRaises(ValueError, exa_impala_adapter.check_request, {u'schemaMetadataInfo': {u'name': u'EXA_PROXY',
                                                                                                 u'properties': {
                                                                                                     u'META_CONNECTION': u'SYS_CONNECTION',
                                                                                                     u'TABLE_SCHEMA': u'SANDBOX',
                                                                                                     u'type': u'createVirtualSchema'}}})
