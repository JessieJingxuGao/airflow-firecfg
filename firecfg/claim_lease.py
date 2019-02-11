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

from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable

import time
from pprint import pprint

from firecfg.client import FireStoreConfig
from logging import getLogger

import sys

one_day_ago = datetime.combine(
        datetime.today() - timedelta(1), datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': one_day_ago,
}

dag = DAG(
    dag_id='claim_lease', default_args=args,
    schedule_interval=timedelta(minutes=1))

def test_lease(ds, **kwargs):
    """
    Acquire lease
    """
    log = getLogger('FireStoreConfigTest')
    import logging
    
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    #In order for this DAG to run successfully, there should be a Variable declared and initiallized from Airflow UI
    #It can be done from Admin -> Variables option in the UI
    #The value of this variable should be kept same as the name of the Airflow cluster to avoid ambiguity
    my_node_id = Variable.get('node_id')
    my_project_id = Variable.get('gc_project')

    log.info('My selected node='+my_node_id)
    conf = FireStoreConfig(node_id=my_node_id, project=my_project_id, collection_name='config')

    config_id = 'et12'

    result = conf.claim('master')
    if result:
        log.info('claimed lease `master`')
    else:
        log.info('unable to claim lease `master`')

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=test_lease,
    dag=dag)

run_this
