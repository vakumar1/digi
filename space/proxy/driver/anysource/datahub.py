from datahub.emitter.mcp import MetadataChangeProposalWrapper
import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass
from kubernetes import client

import digi
import time
import ast

def get_emitter(endpoint):
    # Create an emitter to DataHub over REST
    emitter = DatahubRestEmitter(endpoint, extra_headers={})

    # Test the connection
    emitter.test_connection()

    return emitter

def emit_metadata_event(emitter, id, dataset, data):
    # Construct a MetadataChangeProposalWrapper for a custom entity
    dataset_urn = builder.make_dataset_urn(id, dataset)
    dataset_name = f"{dataset}"
    dataset_description = f"{dataset}:{id}"
    dataset_properties = DatasetPropertiesClass(
        name=dataset_name,
        description=str(data),
        customProperties=data
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=dataset_properties
    )

    # Write the metadata change proposal to DataHub
    print(f"Emitting dataset {id}:{dataset}")
    emitter.emit(mcp)

# emit digi metadata forever
def emit_digi_data_forever(datahub_endpoint, datahub_group):
    emitter = get_emitter(datahub_endpoint)
    
    while True:
        # Create an instance of the API class
        api_instance = client.CustomObjectsApi()

        # TODO: remove hardcoded group (retrieve group name from user (?))
        group = "campus.digi.dev"
        version = "v1"
        try:
            # get all digis within dspace
            api_response = api_instance.get_api_resources(group, version)
            digis = []
            for resource in api_response.resources:
                resource_digis = api_instance.list_cluster_custom_object(group, version, resource.name)
                digis.extend(resource_digis["items"])
            
            # emit name, kind, and egresses for each digi
            for d in digis:
                data = {
                    "name": d["metadata"]["name"],
                    "kind": d["kind"]
                }
                digi.logger.info(f"Digi {data['name']}")
                if "egress" in d["spec"]:
                    egresses = d["spec"]["egress"]
                    for e in egresses:
                        name = f"egress:{e}"
                        desc = egresses[e].get("desc")
                        if not desc:
                            desc = ""
                        data[name] = desc
                emit_metadata_event(emitter, datahub_group, d["metadata"]["name"], data)
        except Exception as e:
            digi.logger.info("Failed to write dspace data to Datahub: ")
            digi.logger.info(e)

        time.sleep(10)
