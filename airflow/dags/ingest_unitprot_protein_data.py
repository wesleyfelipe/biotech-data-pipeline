import os
import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_JOBS_FOLDER = "/opt/airflow/src/spark"
SPARK_DEPENDENCIES_FOLDER = f"{SPARK_JOBS_FOLDER}/dependencies"
SPARK_JAR_DEPENDENCIES=f"{SPARK_DEPENDENCIES_FOLDER}/spark-xml_2.12-0.14.0.jar,{SPARK_DEPENDENCIES_FOLDER}/neo4j-connector-apache-spark_2.12-5.0.0_for_spark_3.jar"

def create_task(entity_name):
    neo4j_url = os.getenv("NEO4J_URL")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_pass = os.getenv("NEO4J_PASSWORD")

    return SparkSubmitOperator(
        conn_id="spark_default",
        task_id=f"ingest_{entity_name}",
        application=f"{SPARK_JOBS_FOLDER}/ingest_unitprot_data.py",
        application_args=[entity_name, "/data/unitprot/", neo4j_url, neo4j_user, neo4j_pass],
        jars=SPARK_JAR_DEPENDENCIES,
    )

with DAG(
        dag_id="ingest_unitprot_protein_data",
        start_date=datetime.datetime(2023, 3, 1),
        schedule="@daily",
        catchup=False,
):

    ingest_protein = create_task("protein")
    ingest_organism = create_task("organism")
    ingest_reference = create_task("reference")
    ingest_gene = create_task("gene")
    ingest_feature = create_task("feature")


    ingest_protein >> ingest_organism
    ingest_protein >> ingest_reference
    ingest_protein >> ingest_gene
    ingest_protein >> ingest_feature

