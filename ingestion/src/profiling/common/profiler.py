from src.services.common.source import CommonSource

class ProfilerExecutor:

    @staticmethod
    def execute(commonSource: CommonSource):

        from metadata.generated.schema.metadataIngestion.workflow import OpenMetadataWorkflowConfig
        workflow_config: OpenMetadataWorkflowConfig = commonSource.getMetadataWorkflowConfig()


        config_metrics = ["row_count", "min", "COUNT", "null_count"]
        workflow_config.processor = {
            "type": "orm-profiler",
            "config": {"profiler" : {
                                        "name": "my_profiler",
                                        "metrics": config_metrics,
                                    } },
        }
        config_metrics_label = ["rowCount", "min", "valuesCount", "nullCount"]

        from metadata.workflow.profiler import ProfilerWorkflow
        from metadata.workflow.workflow_output_handler import print_status

        workflow = ProfilerWorkflow(workflow_config)
        workflow.execute()
        workflow.raise_from_status()
        print_status(workflow)
