import unittest

from src.exa_impala_adapter_meta import AdapterRequestFactory


class ExaImpalaAdapterMetaTest(unittest.TestCase):
    def test_check_request_base(self):
        self.assertRaises(ValueError, AdapterRequestFactory.check_request_base, {u'notype': None})
        try:
            AdapterRequestFactory.check_request_base({u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                u'META_CONNECTION': u'SYS_CONNECTION', u'TABLE_SCHEMA': u'SANDBOX',
                u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'}}, u'type': u'createVirtualSchema'})
        except Exception as e:
            self.fail("check_request_base threw and exception: {0}".format(e.message))

    def test_check_required_attributes(self):
        # Check missing inner attribute
        self.assertRaises(ValueError, AdapterRequestFactory.check_required_attributes,
                          {u'schemaMetadataInfo': {u'name': None, u'properties': {
                              u'META_CONNECTION': None, u'TABLE_SCHEMA': None,
                              u'WEBSOCKETS_EGG_PATH': None}},
                           u'type': None}, {
                              u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                                  u'META_CONNECTION': u'SYS_CONNECTION', u'TABLE_SCHEMA': u'SANDBOX'
                              }},
                              u'type': u'createVirtualSchema'})

        # Check missing outer attribute
        self.assertRaises(ValueError, AdapterRequestFactory.check_required_attributes,
                          {u'schemaMetadataInfo': {u'name': None, u'properties': {
                              u'META_CONNECTION': None, u'TABLE_SCHEMA': None,
                              u'WEBSOCKETS_EGG_PATH': None}},
                           u'type': None}, {
                              u'schemaMetadataInfo': {u'name': u'EXA_PROXY'},
                              u'type': u'createVirtualSchema'})

        # Check valid request
        try:
            AdapterRequestFactory.check_required_attributes(
                {u'schemaMetadataInfo': {u'name': None, u'properties': {
                    u'META_CONNECTION': None, u'TABLE_SCHEMA': None,
                    u'WEBSOCKETS_EGG_PATH': None}},
                 u'type': None}, {
                    u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                        u'META_CONNECTION': u'SYS_CONNECTION', u'TABLE_SCHEMA': u'SANDBOX',
                        u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'
                    }},
                    u'type': u'createVirtualSchema'})

        except Exception as e:
            self.fail("check_required_attributes threw and exception: {0}".format(e.message))

    def test_check_create_virtual_schema_request(self):
        # Missing websockets attribute
        self.assertRaises(ValueError, AdapterRequestFactory.check_create_virtual_schema_request,
                          {
                              u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                                  u'META_CONNECTION': u'SYS_CONNECTION', u'TABLE_SCHEMA': u'SANDBOX'
                              }},
                              u'type': u'createVirtualSchema'})

        # Missing META_CONNECTION attribute
        self.assertRaises(ValueError, AdapterRequestFactory.check_create_virtual_schema_request,
                          {
                              u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                                  u'TABLE_SCHEMA': u'SANDBOX',
                                  u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'
                              }},
                              u'type': u'createVirtualSchema'})

        # Missing TABLE_SCHEMA attribute
        self.assertRaises(ValueError, AdapterRequestFactory.check_create_virtual_schema_request,
                          {
                              u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                                  u'META_CONNECTION': u'SYS_CONNECTION',
                                  u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'
                              }},
                              u'type': u'createVirtualSchema'})

        try:
            # Valid request
            AdapterRequestFactory.check_create_virtual_schema_request({
                u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                    u'META_CONNECTION': u'SYS_CONNECTION', u'TABLE_SCHEMA': u'SANDBOX',
                    u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'
                }},
                u'type': u'createVirtualSchema'})
        except Exception as e:
            self.fail("check_create_virtual_schema_request threw and exception: {0}".format(e.message))

    def test_create_virtual_schema_request(self):
        request = AdapterRequestFactory.create_virtual_schema_request({
            u'schemaMetadataInfo': {u'name': u'EXA_PROXY', u'properties': {
                u'META_CONNECTION': u'SYS_CONNECTION', u'TABLE_SCHEMA': u'SANDBOX',
                u'WEBSOCKETS_EGG_PATH': u'/buckets/bfsdefault/py/ws'
            }},
            u'type': u'createVirtualSchema'})
        self.assertEqual(request.name, u"EXA_PROXY")
        self.assertEqual(request.meta_connection, u"SYS_CONNECTION")
        self.assertEqual(request.table_schema, u"SANDBOX")
        self.assertEqual(request.websockets_lib_path, u"/buckets/bfsdefault/py/ws")
