Setup:

* Install docker: https://docs.docker.com/engine/install/
* Install compose: https://docs.docker.com/compose/install/

* docker compose up airflow-init
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#initialize-the-database

* docker compose up

* http://localhost:8080
* airflow/airflow



spark-submit   --jars /home/wesley.silva/Pessoal/biotech-data-pipeline/dependencies/spark-xml_2.12-0.14.0.jar,/home/wesley.silva/Pessoal/biotech-data-pipeline/dependencies/neo4j-connector-apache-spark_2.12-5.0.0_for_spark_3.jar   /home/wesley.silva/Pessoal/biotech-data-pipeline/scripts/ingest_xml_protein_data.py


spark-submit   /src/ingest_protein_data.py
