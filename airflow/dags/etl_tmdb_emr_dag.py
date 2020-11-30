from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.operators import S3DataQualityOperator

# S3 buckets
bucket_etl = 'trending-tv-shows'
bucket_log = 'emr-etl-log'

# S3 ETL keys
data_key = 'data'
tmdb_raw_key= 'tmdb_raw'
tmdb_stat_key = 'tmdb_stat'
etl_script_key = 'etl/etl_tmdb.py'

# HDFS paths
data_path = '/data'
tmdb_raw_path = '{}/tmdb_raw'.format(data_path)
tmdb_stat_path = '{}/tmdb_stat'.format(data_path)

# Execution dates with different formats
exec_date = '{{ ds }}'
exec_date_formatted = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y/%m/%d") }}'
exec_date_partitioned = '{{ macros.ds_format(ds, "%Y-%m-%d", "year=%Y/month=%m/day=%d") }}'

# Default args for the dag
DEFAULT_ARGS = {
    'owner': 'trending-tv-shows',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# EMR job steps for ETL
EMR_STEPS = [
    {
        "Name": "Move raw TMDB data from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{}/{}/{}".format(bucket_etl, tmdb_raw_key, exec_date_formatted),
                "--dest={}".format(tmdb_raw_path),
            ],
        },
    },
    {
        "Name": "Run ETL for TMDB TV show data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit", "--master", "local[*]",
                "s3://{}/{}".format(bucket_etl, etl_script_key),
                "{}/*.json".format(tmdb_raw_path),
                tmdb_stat_path,
                exec_date,
            ],
        },
    },
    {
        "Name": "Move TMDB stats from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src={}".format(tmdb_stat_path),
                "--dest=s3://{}/{}".format(bucket_etl, tmdb_stat_key),
            ],
        },
    },
]

# EMR job flow to overwrite default configurations
JOB_FLOW_OVERRIDES = {
    'Name': 'ETL-TMDB-{}'.format(exec_date),
    "LogUri": "s3://{}/{}-tmdb".format(bucket_log, exec_date),
    'ReleaseLabel': 'emr-5.29.0',
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm1.medium',
                'InstanceCount': 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': EMR_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

# Create the dag with default args
dag = DAG(dag_id='etl_tmdb_emr',
          default_args=DEFAULT_ARGS,
          start_date=datetime.now(),
          schedule_interval='@daily',
)

# Data quality checks on S3
raw_tmdb_data_quality = S3DataQualityOperator(
    task_id='Raw_tmdb_data_quality_check',
    dag=dag,
    bucket=bucket_etl,
    prefix='{}/{}'.format(tmdb_raw_key, exec_date_formatted),
    delimiter='',
    aws_conn_id='aws_default'
)

processed_tmdb_data_quality = S3DataQualityOperator(
    task_id='Processed_tmdb_data_quality_check',
    dag=dag,
    bucket=bucket_etl,
    prefix='{}/{}'.format(tmdb_stat_key, exec_date_partitioned),
    delimiter='',
    aws_conn_id='aws_default'
)

# Create EMR job flow and monitor it
job_flow_creator = EmrCreateJobFlowOperator(
    task_id='Create_emr_job_flow',
    dag=dag,
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
)

job_sensor = EmrJobFlowSensor(
    task_id='Check_emr_job_flow',
    dag=dag,
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_emr_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
)

# Define task orders
raw_tmdb_data_quality >> job_flow_creator
job_flow_creator >> job_sensor
job_sensor >> processed_tmdb_data_quality