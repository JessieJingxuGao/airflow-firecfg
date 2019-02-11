# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from firecfg.client import FireStoreConfig
from logging import getLogger

seven_days_ago = airflow.utils.dates.days_ago(7)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['alex@vanboxel.be'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

with DAG('run_cluster', schedule_interval=timedelta(minutes=10),
         default_args=default_args) as dag:
    def should_run(ds, **kwargs):
        log = getLogger('FireStoreConfigTest')
        import logging
        import sys

        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)
        my_node_id=Variable.get('node_id')
        my_project_id=Variable.get('gc_project')
        conf = FireStoreConfig(node_id=my_node_id, project=my_project_id, collection_name='config')
        if conf.getNodeID('master'):
            return "start_cluster"
        else:
            return "no_run"


    start = BranchPythonOperator(
        task_id='start',
        provide_context=True,
        python_callable=should_run,
    )

    start_cluster = DataprocClusterCreateOperator(
        task_id='start_cluster',
        cluster_name='smoke-cluster-{{ ds_nodash }}',
        project_id=Variable.get('gc_project'),
        num_workers=2,
        network_uri='default-jg',
        num_preemptible_workers=1,
        properties={
            'spark:spark.executorEnv.PYTHONHASHSEED': '0',
            'spark:spark.yarn.am.memory': '1024m',
            'spark:spark.sql.avro.compression.codec': 'deflate'
        },
        worker_disk_size=50,
        master_disk_size=50,
        labels={
            'example': 'label'
        },
        zone=Variable.get('gc_zone'),
        google_cloud_conn_id='gcp_smoke'
    )

    stop_cluster = DataprocClusterDeleteOperator(
        task_id='stop_cluster',
        cluster_name='smoke-cluster-{{ ds_nodash }}',
        project_id=Variable.get('gc_project'),
        google_cloud_conn_id='gcp_smoke'
    )

    no_run = DummyOperator(task_id='no_run')

    end = DummyOperator(
        trigger_rule='one_success',
        task_id='end')

    start >> start_cluster >> stop_cluster >> end
    start >> no_run >> end
