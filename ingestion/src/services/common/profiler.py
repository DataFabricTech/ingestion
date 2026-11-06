from services.common.source import CommonSource

class ProfilerExecutor:

    @staticmethod
    def execute(commonSource: CommonSource):

        ########################################################
        # build metadata config
        ########################################################
        from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
        workflow_config: OpenMetadataWorkflowConfig = commonSource.getMetadataWorkflowConfig()

        from metadata.workflow.profiler import ProfilerWorkflow
        from metadata.workflow.workflow_output_handler import print_status
        workflow = ProfilerWorkflow.create(workflow_config)
        try:
            workflow.execute()
            workflow.raise_from_status()
            print_status(workflow)
        except Exception as e:
            print(e)
