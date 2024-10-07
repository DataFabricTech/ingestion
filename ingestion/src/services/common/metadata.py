from services.common.source import CommonSource
class MetadataExecutor:

    @staticmethod
    def execute(commonSource: CommonSource):
        ########################################################
        # build metadata config
        ########################################################
        from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
        workflow_config: OpenMetadataWorkflowConfig = commonSource.getMetadataWorkflowConfig()

        ########################################################
        # run metadata workflow
        ########################################################
        from metadata.workflow.metadata import MetadataWorkflow
        from metadata.workflow.workflow_output_handler import print_status
        workflow = MetadataWorkflow(workflow_config)
        workflow.execute()
        workflow.raise_from_status()
        print_status(workflow)
        workflow.update_ingestion_status(workflow_config.source.serviceName)
        #workflow.stop()



