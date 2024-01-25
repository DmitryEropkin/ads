"""
# Version: 0.0.3
# Docs: https://wiki.corp.dev.vtb/pages/viewpage.action?pageId=425256823
# Stream: 903 Datalake
# Team: KIHLOAD
# Dev: Gusev A.
# Source schema: DWH_EXPORT_E46, DWH_EXPORT
# Target schema: prod_kih_full_dds
# Mapping: https://wiki.corp.dev.vtb/pages/viewpage.action?pageId=1954897158
"""

import json
import os
import sys

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.utils.dates import days_ago

sys.path.append(Variable.get('1642_71_hadoop_sync_libs'))
from hadoop_sync_keycloak_operator import HadoopSyncOperator

DAG_ID = '1642_19_datalake_kih_552_001_load'
SCHEDULE_INTERVAL = '00 00 * * *'

stage_and_config_path = Variable.get("1642_19_CONFIG_STAGE", deserialize_json=True)
stage = stage_and_config_path["STAGE"]
config_path = stage_and_config_path["1642_19_CONFIG"]
rp_connection = BaseHook.get_connection("1642_19_datalake_statuses_1642_20")
hs_connection = BaseHook.get_connection("hadoop_sync_user_info")
hs_keycloak_url = Variable.get(f'1642_71_hadoop_sync_keycloak_service_url')
hs_request_url = Variable.get(f'1642_71_hadoop_sync_request_service_url')

with open(config_path) as cfg:
    config = json.load(cfg)

spark_etl_config = config.get("SPARK_ETL_CONF")
core_path = spark_etl_config["SPARK_ETL_JAR"]
spark_jars = spark_etl_config["DATALAKE_SPARK_JARS"]
keytab_dir = spark_etl_config["DATALAKE_KEYTAB_DIR"]
keytab_name = spark_etl_config['KEYTAB']
principal = spark_etl_config['PRINCIPAL']
etl_schema = spark_etl_config['ETL_SCHEMA']
keytab = os.path.join(keytab_dir, keytab_name)
vault_conf = config.get("VAULT4SETL")

if stage == "d0":
    target_db = "dev_kih_full_dds"
    spark_driver_memory = None
    spark_executor_memory = None
    spark_executor_memory_max = None
    spark_executor_cores = None
    spark_executor_instances = None
    spark_executor_memoryOverhead = None
    spark_executor_memoryOverhead_max = None
    hs_source_cluster = None
    hs_target_cluster = None
elif stage == "if":
    target_db = "if_kih_full_dds"
    spark_driver_memory = None
    spark_executor_memory = None
    spark_executor_memory_max = None
    spark_executor_cores = None
    spark_executor_instances = None
    spark_executor_memoryOverhead = None
    spark_executor_memoryOverhead_max = None
    hs_source_cluster = None
    hs_target_cluster = None
elif stage == "rr":
    target_db = "test_kih_full_dds"
    spark_driver_memory = "10g"
    spark_executor_memory = "10g"
    spark_executor_memory_max = "20g"
    spark_executor_cores = "3"
    spark_executor_instances = "4"
    spark_executor_memoryOverhead = "2g"
    spark_executor_memoryOverhead_max = "2g"
    hs_source_cluster = "rrdapp"
    hs_target_cluster = "rrdrp"
elif stage == "p0":
    target_db = "prod_kih_full_dds"
    spark_driver_memory = "16g"
    spark_executor_memory = "16g"
    spark_executor_memory_max = "30g"
    spark_executor_cores = "5"
    spark_executor_instances = "5"
    spark_executor_memoryOverhead = "4g"
    spark_executor_memoryOverhead_max = "2g"
    hs_source_cluster = "adh8"
    hs_target_cluster = "BDAETL-ns"
else:
    assert False, 'please, set "STAGE" variable'

spark_conf = {
    "spark.master": "yarn",
    "spark.submit.deployMode": "cluster",
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",
    "spark.sql.legacy.parquet.int96RebaseModeInRead": "LEGACY",
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED"
}

spark_conf_max = {
    "spark.master": "yarn",
    "spark.submit.deployMode": "cluster",
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",
    "spark.sql.legacy.parquet.int96RebaseModeInRead": "LEGACY",
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.broadcastTimeout": "7200"
}

if spark_driver_memory:
    spark_conf["spark.driver.memory"] = spark_driver_memory
if spark_executor_memory:
    spark_conf["spark.executor.memory"] = spark_executor_memory
if spark_executor_cores:
    spark_conf["spark.executor.cores"] = spark_executor_cores
if spark_executor_instances:
    spark_conf["spark.executor.instances"] = spark_executor_instances
if spark_executor_memoryOverhead:
    spark_conf["spark.executor.memoryOverhead"] = spark_executor_memoryOverhead
if spark_executor_memory:
    spark_conf_max["spark.executor.memory"] = spark_executor_memory_max
if spark_executor_memoryOverhead:
    spark_conf_max["spark.executor.memoryOverhead"] = spark_executor_memoryOverhead_max
if spark_executor_cores:
    spark_conf_max["spark.executor.cores"] = spark_executor_cores
if spark_executor_instances:
    spark_conf_max["spark.executor.instances"] = spark_executor_instances
if spark_driver_memory:
    spark_conf_max["spark.driver.memory"] = spark_driver_memory

intervals = Variable.get('kihPartRewriteVariables', deserialize_json=True)
e46_ref_account_account_link_h_f_interval = intervals.get('e46_ref_account_account_link_h_f', '8 days')
e46_ref_acnt_deal_link_h_f_interval = intervals.get('e46_ref_acnt_deal_link_h_f', '8 days')
acc_acnt_reserve_interval = intervals.get('acc_acnt_reserve', '8 days')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0
}

logs_table = {
    "url": f"jdbc:postgresql://{rp_connection.host}:{rp_connection.port}/{rp_connection.schema}",
    "driver": "org.postgresql.Driver",
    "user": rp_connection.login,
    "password": rp_connection.password
}

common_info_full = {
    "targetSchema": "prod_kih_full_dds",
    "etlSchema": etl_schema,
    "logsTable": logs_table,
    "vault": vault_conf,
    "flagName": "1480_45"
}

common_info = {
    "targetSchema": "prod_kih_dds",
    "etlSchema": etl_schema,
    "logsTable": logs_table,
    "vault": vault_conf,
    "flagName": "1480_45"
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    tags=['SparkETL', 'KIH'],
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1
)

# DP-7114
e46_ref_migr_subject_inc = {
    "flows": [{
        "loadType": "Scd0Append",
        "source": {
            "schema": "DWH_EXPORT_E46",
            "table": "E46_REF_MIGR_SUBJECT_INC_V",
            "incrementField": "MGRSBJ_CHANGE_DATE",
            "jdbcDialect": "KihDialect"
        },
        "target": {"table": "e46_ref_migr_subject_inc"}
    }],
    "commonInfo": common_info_full,
    "connection": "1642_19_datalake_kih_552_load_e46"
}

# DP-7200
dp_7200 = {
    "flows": [
        {
            "loadType": "Scd2MigrationCustom",
            "source": {
                "schema": "DWH_EXPORT",
                "table": "E00_REF_ACNT_PAL_LINK_H_V",
                "incrementField": "ACPFLH_START_DATE",
                "effectiveFrom": "ACPFLH_START_DATE",
                "effectiveTo": "ACPFLH_END_DATE",
                "jdbcDialect": "KihDialect",
                "query": "SELECT TO_CHAR(ACPFLH_GID) AS ACPFLH_GID, ACPFLH_START_DATE, ACPFLH_END_DATE, TO_CHAR(ACPFLH_AUDIT_ID) AS ACPFLH_AUDIT_ID, TO_CHAR(ACPFLH_HASH) AS ACPFLH_HASH, TO_CHAR(ACPFLH_ACNT_PAL_LINK_TYPE_CD) AS ACPFLH_ACNT_PAL_LINK_TYPE_CD, TO_CHAR(ACPFLH_ACNT_GID) AS ACPFLH_ACNT_GID, TO_CHAR(ACPFLH_PAL_GID) AS ACPFLH_PAL_GID, ACPFLH_LINK_BEGIN_DATE, ACPFLH_LINK_END_DATE, ACPFLH_SOURCE, TO_CHAR(ACPFLH_BRANCH_DEPT_GID) AS ACPFLH_BRANCH_DEPT_GID FROM $schema.$table WHERE $customCondition AND $condition"
            },
            "target": {"table": "e00_ref_acnt_pal_link_h", "businessKeys": ["ACPFLH_GID", "ACPFLH_BRANCH_DEPT_GID"]}
        },
        {
            "loadType": "Scd2MigrationCustom",
            "source": {
                "schema": "DWH_EXPORT",
                "table": "E00_REF_DEAL_CESSION_H_V",
                "incrementField": "CESSH_START_DATE",
                "effectiveFrom": "CESSH_START_DATE",
                "effectiveTo": "CESSH_END_DATE",
                "jdbcDialect": "KihDialect",
                "query": "SELECT TO_CHAR(CESSH_GID) AS CESSH_GID, CESSH_START_DATE, CESSH_END_DATE, TO_CHAR(CESSH_AUDIT_ID) AS CESSH_AUDIT_ID, TO_CHAR(CESSH_HASH) AS CESSH_HASH, CESSH_NUMBER, TO_CHAR(CESSH_DEAL_TYPE_CD) AS CESSH_DEAL_TYPE_CD, CESSH_BEGIN_DATE, CESSH_ACTUAL_END_DATE, TO_CHAR(CESSH_DISCOUNT_RATE) AS CESSH_DISCOUNT_RATE, TO_CHAR(CESSH_CRT_GID) AS CESSH_CRT_GID, TO_CHAR(CESSH_CONTRAGENT_CLNT_GID) AS CESSH_CONTRAGENT_CLNT_GID, CESSH_SOURCE_ID, CESSH_SOURCE, TO_CHAR(CESSH_BRANCH_DEPT_GID) AS CESSH_BRANCH_DEPT_GID FROM $schema.$table WHERE $customCondition AND $condition"
            },
            "target": {"table": "e00_ref_deal_cession_h", "businessKeys": ["CESSH_GID", "CESSH_BRANCH_DEPT_GID"]}
        },
        {
            "loadType": "Scd1Replace",
            "source": {
                "schema": "DWH_EXPORT",
                "table": "E00_REF_PORTFOLIO_LOANS_PK_GID_V",
                "jdbcDialect": "KihDialect"
            },
            "target": {"table": "e00_ref_portfolio_loans_pk_gid"}
        }
    ],
    "commonInfo": common_info_full,
    "connection": "1642_19_datalake_kih_552_load"
}

flows_link = {
    "flows": [
        {
            "loadType": "KihPartRewrite",
            "source": {
                "schema": "DWH_EXPORT_E46",
                "table": "E46_REF_ACC_ACC_LINK_H_F_V",
                "jdbcDialect": "KihDialect",
                "partitionColumn": "AALH_START_DATE",
                "numPartitions": 10},
            "target": {
                "table": "e46_ref_account_account_link_h_f",
                "businessKeys": ["AALH_ID"],
                "customPartitioning": "CustomExpression",
                "partitionFields": ["PARTITION_AALH_VALID_BEGIN_DATE"],
                "aggregationField": "SUBSTR(AALH_VALID_BEGIN_DATE, 1, 4) AS PARTITION_AALH_VALID_BEGIN_DATE"
            },
            "addInfo": {
                "dateInterval": e46_ref_account_account_link_h_f_interval,
                "addNewRows": "True"
            }
        },
        {
            "loadType": "KihPartRewrite",
            "source": {
                "schema": "DWH_EXPORT_E46",
                "table": "E46_REF_ACNT_DEAL_LINK_H_F_V",
                "jdbcDialect": "KihDialect",
                "partitionColumn": "ACDLH_START_DATE",
                "numPartitions": 10},
            "target": {
                "table": "e46_ref_acnt_deal_link_h_f",
                "withBackup": True,
                "businessKeys": ["ACDLH_ID"],
                "customPartitioning": "CustomExpression",
                "partitionFields": ["PARTITION_ACDLH_START_DATE"],
                "aggregationField": "SUBSTR(ACDLH_START_DATE, 1, 7) AS PARTITION_ACDLH_START_DATE"
            },
            "addInfo": {
                "dateInterval": e46_ref_acnt_deal_link_h_f_interval,
                "addNewRows": "True"
            }
        }
    ],
    "commonInfo": common_info_full,
    "connection": "1642_19_datalake_kih_552_load"
}
flows_reserve = {
    "flows": [
        {
            "loadType": "KihPartRewrite",
            "source": {
                "schema": "DWH_EXPORT",
                "table": "E00_ACC_ACNT_RESERVE_V",
                "partitionColumn": "ACRSRV_VALID_BEGIN",
                "jdbcDialect": "KihDialect",
                "columnCasts": [
                    {"name": "ACRSRV_RESERVE_ESTIM_AMOUNT", "colType": "decimal(38,6)"},
                    {"name": "ACRSRV_RESERVE_ESTIM_RATE", "colType": "decimal(38,7)"},
                    {"name": "ACRSRV_RESERVE_FACT_AMOUNT", "colType": "decimal(38,6)"},
                    {"name": "ACRSRV_RESERVE_FACT_RATE", "colType": "decimal(38,5)"},
                    {"name": "ACRSRV_BASE_VALUE", "colType": "decimal(38,5)"},
                    {"name": "ACRSRV_PORTF_BASE_VALUE", "colType": "decimal(38,5)"},
                    {"name": "ACRSRV_PORTF_AMOUNT", "colType": "decimal(38,5)"},
                    {"name": "ACRSRV_FST_QLT_AMOUNT_EQ", "colType": "decimal(38,5)"},
                    {"name": "ACRSRV_SCND_QLT_AMOUNT_EQ", "colType": "decimal(38,5)"},
                    {"name": "ACRSRV_RCD_POS_REVAL_EQ", "colType": "decimal(38,5)"},
                    {"name": "ACRSRV_RSTR_COUNT", "colType": "decimal(38,5)"}]
            },
            "target": {
                "table": "acc_acnt_reserve",
                "businessKeys": ["ACRSRV_ID"],
                "partitionFields": ["PARTITION_ACRSRV_VALID_BEGIN"],
                "customPartitioning": "CustomExpression",
                "aggregationField": "SUBSTR(ACRSRV_VALID_BEGIN, 1, 7) as PARTITION_ACRSRV_VALID_BEGIN"

            },
            "addInfo": {
                "dateInterval": acc_acnt_reserve_interval,
                "addNewRows": "True"
            }
        }
    ],
    "commonInfo": common_info,
    "connection": "1642_19_datalake_kih_552_load"
}

TASK_ID_1 = 'DP-7114_Scd0Append.e46_ref_migr_subject_inc'
TASK_ID_2 = 'DP-7200'
TASK_ID_3 = 'flows_link'
TASK_ID_4 = 'flows_reserve'


dag.doc_md = __doc__


def task_spark(task_id: str, json_load: dict, spark_config: dict):
    return SparkSubmitOperator(
        task_id=task_id,
        name=f'{DAG_ID}.{task_id}',
        application_args=[
            json.dumps(json_load).replace("{{", "{ {").replace("}}", "} }"),
            "--dag-execution-date {{ execution_date }}"
        ],
        application=core_path,
        java_class='sparketl.Main',
        keytab=keytab,
        principal=principal,
        jars=spark_jars,
        conf=spark_config,
        verbose=False,
        dag=dag
    )


def hsync_task(task_id: str, tables_list: list):
    return HadoopSyncOperator(
        task_id=f"{task_id}.hsync_task",
        keycloak_url=hs_keycloak_url,
        username=hs_connection.login,
        password=hs_connection.password,
        request_service_url=hs_request_url,
        dest_cluster=hs_target_cluster,
        src_cluster=hs_source_cluster,
        src_database_name=target_db,
        tbl_list=tables_list,
        meta_sync=True,
        partition_list=None,
        sample_filter_by_date_start=None,
        sample_filter_by_date_end=None,
        sample_filter_by_records=None,
        dag=dag)


with dag:
    task_1 = task_spark(TASK_ID_1, e46_ref_migr_subject_inc, spark_conf)
    hs_task_1 = hsync_task(TASK_ID_1, ['e46_ref_migr_subject_inc'])
    task_1 >> hs_task_1

    task_2 = task_spark(TASK_ID_2, dp_7200, spark_conf)
    hs_task_2 = hsync_task(
        TASK_ID_2, ['e00_ref_acnt_pal_link_h', 'e00_ref_deal_cession_h_v', 'e00_ref_portfolio_loans_pk_gid_v'])
    task_2 >> hs_task_2

    task_3 = task_spark(TASK_ID_3, flows_link, spark_conf)
    task_4 = task_spark(TASK_ID_4, flows_reserve, spark_conf)
