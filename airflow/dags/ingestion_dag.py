"""
DEPRECATED — superseded by healthcare_pipeline_dag.py.

This file is kept for reference only. It is NOT loaded by Airflow because:
  1. It uses the same dag_id ('healthcare_dw_daily') as healthcare_pipeline_dag.py.
     Loading both would cause Airflow to pick one non-deterministically.
  2. It lacks GE quality gates, XCom row-count passing, and the HttpSensor.

To prevent Airflow from parsing this file, the DAG definition has been removed.
Delete this file once the team has migrated any bookmarks or runbook references
to healthcare_pipeline_dag.py.
"""
# No DAG objects defined — Airflow will skip this file during DAG discovery.
