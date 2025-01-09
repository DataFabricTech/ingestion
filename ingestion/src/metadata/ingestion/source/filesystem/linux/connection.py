from dataclasses import dataclass
# from functools import partial
# from typing import Optional

import paramiko

# from metadata.clients.aws_client import AWSClient
# from metadata.generated.schema.entity.automations.workflow import (
#     Workflow as AutomationWorkflow,
# )

from metadata.generated.schema.entity.services.connections.ssh.sshConnection import (
    SshConnection
)
# from metadata.ingestion.connections.test_connections import test_connection_steps
# from metadata.ingestion.ometa.ometa_api import OpenMetadata


@dataclass
class SSHClient:
    ssh_client = paramiko.client.SSHClient


def get_connection(connection: SshConnection) -> SSHClient:

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(hostname=connection.hostname,
                   username=connection.username,
                   password=connection.password,
                   port=connection.port)

    return client

# def test_connection(
#     metadata: OpenMetadata,
#     client: S3ObjectStoreClient,
#     service_connection: S3Connection,
#     automation_workflow: Optional[AutomationWorkflow] = None,
# ) -> None:
#     """
#     Test connection. This can be executed either as part
#     of a metadata workflow or during an Automation Workflow
#     """
#
#     test_fn = {
#         "ListBuckets": client.s3_client.list_buckets,
#         # "GetMetrics": partial(
#         #     client.cloudwatch_client.list_metrics, Namespace="AWS/S3"
#         # ),
#     }
#
#     test_connection_steps(
#         metadata=metadata,
#         test_fn=test_fn,
#         service_type=service_connection.type.value,
#         automation_workflow=automation_workflow,
#     )
