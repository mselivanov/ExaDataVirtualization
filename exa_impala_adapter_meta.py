from enum import Enum


class AdapterRequestType(Enum):
    CREATE_VIRTUAL_SCHEMA = 1,
    DROP_VIRTUAL_SCHEMA = 2,
    SET_PROPERTIES = 3,
    REFRESH = 4,
    GET_CAPABILITIES = 5,
    PUSHDOWN = 6


class AdapterRequest(object):
    def __init__(self, request_type):
        self._request_type = request_type

    @property
    def request_type(self):
        return self._request_type

    @request_type.setter
    def request_type(self, value):
        self._request_type = value


class CreateVirtualSchemaAdapterRequest(AdapterRequest):
    def __init__(self, name, meta_connection, table_schema, websockets_lib_path):
        super(CreateVirtualSchemaAdapterRequest, self).__init__(AdapterRequestType.CREATE_VIRTUAL_SCHEMA)
        self._name = name
        self._meta_connection = meta_connection
        self._table_schema = table_schema
        self._websockets_lib_path = websockets_lib_path

    @property
    def name(self):
        return self._name

    @property
    def meta_connection(self):
        return self._meta_connection

    @property
    def table_schema(self):
        return self._table_schema

    @property
    def websockets_lib_path(self):
        return self._websockets_lib_path


class AdapterRequestFactory(object):
    REQUEST_TYPE_MAPPING = {"createVirtualSchema": AdapterRequestType.CREATE_VIRTUAL_SCHEMA,
                            "dropVirtualSchema": AdapterRequestType.DROP_VIRTUAL_SCHEMA,
                            "refresh": AdapterRequestType.REFRESH,
                            "pushdown": AdapterRequestType.PUSHDOWN,
                            "getCapabilities": AdapterRequestType.GET_CAPABILITIES,
                            "setProperties": AdapterRequestType.SET_PROPERTIES}

    CREATE_VIRTUAL_SCHEMA_REQUIRED_ATTRIBUTES = {"schemaMetadataInfo": {"name": None,
                                                                        "properties": {"META_CONNECTION": None,
                                                                                       "TABLE_SCHEMA": None,
                                                                                       "WEBSOCKETS_EGG_PATH": None}}}

    @classmethod
    def check_request_base(cls, request_dict):
        """
        Function checks base validity of request object.
        :param request_dict: json request represented as python dictionary
        :return:
        """
        if "type" not in request_dict:
            raise ValueError("Missing request 'type' key.")
        if request_dict["type"] not in cls.REQUEST_TYPE_MAPPING:
            raise ValueError("'{0}' is not supported".format(request_dict["type"]))

    @classmethod
    def check_required_attributes(cls, required_attrs_dict, request_dict):
        for required_attribute in required_attrs_dict.keys():
            if required_attribute not in request_dict:
                raise ValueError("Attribute '{0}' is not found in request.")
            if required_attrs_dict[required_attribute] is not None:
                AdapterRequestFactory.check_required_attributes(required_attrs_dict[required_attribute],
                                                                request_dict[required_attribute])

    @classmethod
    def check_create_virtual_schema_request(cls, request_dict):
        AdapterRequestFactory.check_required_attributes(AdapterRequestFactory.CREATE_VIRTUAL_SCHEMA_REQUIRED_ATTRIBUTES,
                                                        request_dict)

    @classmethod
    def get_adapter_request_type(cls, request_dict):
        return AdapterRequestFactory.REQUEST_TYPE_MAPPING[request_dict["type"]]

    @classmethod
    def create_virtual_schema_request(cls, request_dict):
        schema_metadata = request_dict["schemaMetadataInfo"]
        schema_properties = schema_metadata["properties"]
        return CreateVirtualSchemaAdapterRequest(schema_metadata["name"], schema_properties["META_CONNECTION"],
                                                 schema_properties["TABLE_SCHEMA"],
                                                 schema_properties["WEBSOCKETS_EGG_PATH"])

    @classmethod
    def create_from_json_dict(cls, request_dict):
        AdapterRequestFactory.check_request_base(request_dict)
        request_type = AdapterRequestFactory.get_adapter_request_type(request_dict)
        if request_type == AdapterRequestType.CREATE_VIRTUAL_SCHEMA:
            AdapterRequestFactory.check_create_virtual_schema_request(request_dict)
        elif request_type == AdapterRequestType.GET_CAPABILITIES:
            raise NotImplementedError
        elif request_type == AdapterRequestType.SET_PROPERTIES:
            raise NotImplementedError
        elif request_type == AdapterRequestType.PUSHDOWN:
            raise NotImplementedError
        elif request_type == AdapterRequestType.REFRESH:
            raise NotImplementedError
        elif request_type == AdapterRequestType.DROP_VIRTUAL_SCHEMA:
            raise NotImplementedError
        else:
            raise ValueError("Can't handle '{0}' request type".format(request_type))
