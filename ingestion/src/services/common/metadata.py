from services.common.source import CommonSource, CollectorStatus

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
        try:
            workflow.execute()
            workflow.raise_from_status()
            print_status(workflow)
            workflow.update_ingestion_status(workflow_config.source.serviceName, commonSource.check_result_status(workflow.result_status()))
            workflow.check_ingestion_status(workflow_config.source.serviceName, source_filter=commonSource.source_filter)
            workflow.execute_profile(workflow_config.source.serviceName, workflow)
            #workflow.stop()
        except Exception as e:
            print(e)
            workflow.update_ingestion_status(workflow_config.source.serviceName, CollectorStatus.INGESTION_FAILED.value, e.args[0])



