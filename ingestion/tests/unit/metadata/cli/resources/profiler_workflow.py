# Copyright 2024 Mobigen
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notice!
# This software is based on https://open-metadata.org and has been modified accordingly.

"""
This file has been generated from dag_runner.j2
"""
from metadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create(
    "/airflow/dag_generated_configs/local_redshift_profiler_e9AziRXs.json"
)
workflow.generate_dag(globals())
