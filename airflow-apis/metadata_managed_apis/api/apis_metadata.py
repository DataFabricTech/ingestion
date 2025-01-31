
DAG_ID_DESCRIPTION = "The ID of the DAG."

APIS_METADATA = [
    {
        "name": "deploy",
        "description": "Deploy a new DAG File to the DAGs directory",
        "http_method": "POST",
        "form_enctype": "multipart/form-data",
        "arguments": [],
        "post_arguments": [
            {
                "name": "workflow_config",
                "description": "Workflow config to deploy as IngestionPipeline",
                "form_input_type": "file",
                "required": True,
            },
        ],
    },
    {
        "name": "trigger",
        "description": "Trigger a DAG",
        "http_method": "POST",
        "arguments": [],
        "post_arguments": [
            {
                "name": "dag_id",
                "description": DAG_ID_DESCRIPTION,
                "required": True,
            },
        ],
    },
    {
        "name": "run_automation",
        "description": "Run an automation workflow",
        "http_method": "POST",
        "arguments": [],
        "post_arguments": [
            {
                "name": "automation_workflow",
                "description": "AutomationWorkflow request",
                "required": True,
            },
        ],
    },
    {
        "name": "status",
        "description": "Get the status of a dag's latest runs",
        "http_method": "GET",
        "arguments": [
            {
                "name": "dag_id",
                "description": DAG_ID_DESCRIPTION,
                "form_input_type": "text",
                "required": True,
            },
        ],
    },
    {
        "name": "delete",
        "description": "Delete a DAG in the Web Server from Airflow database and filesystem",
        "http_method": "DELETE",
        "arguments": [
            {
                "name": "dag_id",
                "description": DAG_ID_DESCRIPTION,
                "form_input_type": "text",
                "required": True,
            },
        ],
    },
    {
        "name": "last_dag_logs",
        "description": "Retrieve all logs from the task instances of a last DAG run",
        "http_method": "GET",
        "arguments": [
            {
                "name": "dag_id",
                "description": DAG_ID_DESCRIPTION,
                "form_input_type": "text",
                "required": True,
            },
            {
                "name": "task_id",
                "description": "DAG task to fetch",
                "form_input_type": "text",
                "required": True,
            },
            {
                "name": "after",
                "description": "Return the log piece after this cursor",
                "form_input_type": "int",
                "required": False,
            },
        ],
    },
    {
        "name": "enable",
        "description": "Mark the DAG as enabled to run on the next schedule.",
        "http_method": "POST",
        "arguments": [],
        "post_arguments": [
            {
                "name": "dag_id",
                "description": DAG_ID_DESCRIPTION,
                "form_input_type": "text",
                "required": True,
            },
        ],
    },
    {
        "name": "disable",
        "description": "Mark the DAG as disabled. It will not run on the next schedule.",
        "http_method": "POST",
        "arguments": [],
        "post_arguments": [
            {
                "name": "dag_id",
                "description": DAG_ID_DESCRIPTION,
                "form_input_type": "text",
                "required": True,
            },
        ],
    },
    {
        "name": "kill",
        "description": "Mark all not finished tasks of a DAG as failed to kill the execution",
        "http_method": "POST",
        "arguments": [],
        "post_arguments": [
            {
                "name": "dag_id",
                "description": DAG_ID_DESCRIPTION,
                "form_input_type": "text",
                "required": True,
            },
        ],
    },
]
