from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


current_datetime = datetime.now().strftime("%Y%m%d-%H%M%S")

# Définition de l'application Spark
spark_application = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": f"spark-application-{current_datetime}",
        "namespace": "airflow"
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "salmabk/sparkage:latest",
        "imagePullPolicy": "IfNotPresent",
        "mainApplicationFile": "local:///app/app.py",
        "sparkVersion": "3.5.2",
        "driver": {
            "labels": {
            "key1": "value1",
            },
            
            "cores": 2,
            "memory": "2G",
            "serviceAccount": "spark"
        },

        "executor" : {
            "cores": 2,
            "memory": "1G",
            "instances" : 2
        },

        "sparkConf" :{
            "spark.sql.files.maxPartitionBytes": "528MB",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.executor.arrow.pyspark.enable" : "true",
            "spark.sql.autoBroadcastJoinThreshold" : "-1" ,
            "spark.memory.storageFraction" : "0.5"
        },
     "ttlSecondsAfterFinished": 30
        # Delete from spark application 30 seconds after job execution
    }
}

default_args = {

    'owner': 'salma',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 10,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG(
    'Dag-Collecte-Om',
    start_date=days_ago(1),
    default_args=default_args,
    schedule_interval='0 9 * * *',  
    catchup=False)

 

spark_transform_data = SparkKubernetesOperator(
    task_id='spark_transform_data',
    namespace='default', 
    kubernetes_conn_id='spark_kubernetes_connection', 
    application_file=spark_application,
    dag=dag)
   
spark_transform_data 