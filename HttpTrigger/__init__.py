import logging

import azure.functions as func

import os

from azure.identity import ClientSecretCredential
from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.serializer.avroserializer import SchemaRegistryAvroSerializer

TENANT_ID='YOUR TENANT_ID'
CLIENT_ID='YOUR CLIENT_ID'
CLIENT_SECRET='YOUR CLIENT_SECRET'

SCHEMA_REGISTRY_ENDPOINT='YOUR_STANDARD_EVENTHUB.servicebus.windows.net'
SCHEMA_GROUP='default'
SCHEMA_STRING = """
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}"""


token_credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)

# For Managed Identity
# token_credential = DefaultAzureCredential()


def serialize(serializer):
    dict_data_ben = {"name": u"Ben", "favorite_number": 7, "favorite_color": u"red"}
    dict_data_alice = {"name": u"Alice", "favorite_number": 15, "favorite_color": u"green"}

    # Schema would be automatically registered into Schema Registry and cached locally.
    payload_ben = serializer.serialize(dict_data_ben, SCHEMA_STRING)
    # The second call won't trigger a service call.
    payload_alice = serializer.serialize(dict_data_alice, SCHEMA_STRING)

    print('Encoded bytes are: ', payload_ben)
    print('Encoded bytes are: ', payload_alice)
    return [payload_ben, payload_alice]


def deserialize(serializer, bytes_payload):
    # serializer.deserialize would extract the schema id from the payload,
    # retrieve schema from Schema Registry and cache the schema locally.
    # If the schema id is the local cache, the call won't trigger a service call.
    dict_data = serializer.deserialize(bytes_payload)

    print('Deserialized data is: ', dict_data)
    return dict_data


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    schema_registry = SchemaRegistryClient(endpoint=SCHEMA_REGISTRY_ENDPOINT, credential=token_credential)
    serializer = SchemaRegistryAvroSerializer(schema_registry, SCHEMA_GROUP)
    bytes_data_ben, bytes_data_alice = serialize(serializer)
    dict_data_ben = deserialize(serializer, bytes_data_ben)
    dict_data_alice = deserialize(serializer, bytes_data_alice)
    serializer.close()

    return func.HttpResponse(
            "Schema Registry Executed.",
            status_code=200
    )
