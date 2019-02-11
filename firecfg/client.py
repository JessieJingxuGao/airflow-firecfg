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

from google.cloud import firestore
from google.protobuf import timestamp_pb2
from typing import Dict, Optional
from logging import getLogger
import time


DEFAULT_COLLECTION = u'config'
NODE_ID_KEY = u'node_id'
TS_KEY = u'ts'
LEASE_DURATION_SECONDS = 120


class FireStoreClient(object):
    """
    https://googleapis.github.io/google-cloud-python/latest/firestore/client.html
    https://googleapis.github.io/google-cloud-python/latest/firestore/types.html
    """
    def __init__(self, project: Optional[str] = None):
        if project is not None:
            self.client = firestore.Client(project=project)
        else:
            self.client = firestore.Client()

    def collection(self, name: str) -> firestore.CollectionReference:
        return self.client.collection(name)

    def transaction(self, max_attempts=1, read_only=False) -> firestore.Transaction:
        return self.client.transaction(max_attempts=max_attempts, read_only=read_only)


class FireStoreCollection(object):
    """
    https://googleapis.github.io/google-cloud-python/latest/firestore/collection.html
    """
    def __init__(self, client: Optional[FireStoreClient] = None, collection_name: Optional[str] = None):
        if client is not None:
            self.client = client
        else:
            self.client = FireStoreClient()

        if collection_name is not None:
            self.collection_name = collection_name
        else:
            self.collection_name = DEFAULT_COLLECTION

        self.collection = client.collection(self.collection_name)

    def document(self, document_id) -> firestore.DocumentReference:
        return self.collection.document(document_id=document_id)


class FireStoreConfig(object):
    def __init__(self,
                 node_id: str,
                 project: Optional[str] = None,
                 collection_name: Optional[str] = None):
        self.node_id = node_id
        self.log = getLogger('FireStoreConfig')

        if project is not None:
            self.project = project
        else:
            self.project = None

        if collection_name is not None:
            self.collection_name = collection_name
        else:
            self.collection_name = DEFAULT_COLLECTION

        self.client = FireStoreClient(project)
        self.collection = FireStoreCollection(client=self.client, collection_name=collection_name)

    def getNodeID(self, config_id: str) -> bool:
        """
        https://googleapis.github.io/google-cloud-python/latest/firestore/transaction.html
        https://googleapis.github.io/google-cloud-python/latest/firestore/document.html
        :param config_id:
        :return:
        """
        @firestore.transactional
        def retrieve_nodeID(transaction: firestore.Transaction,
                                  reference: firestore.DocumentReference,
                                  document_data: Dict,
                                  node_id: str): 
            snapshot = reference.get(transaction=transaction)
            snapshot_data = snapshot.to_dict()
            snapshot_node_id = snapshot_data[NODE_ID_KEY]
        
            return snapshot_node_id == self.node_id

        return retrieve_nodeID(transaction=self.client.transaction(),
                                     reference=self.collection.document(document_id=config_id),
                                     document_data={
                                         NODE_ID_KEY: self.node_id,
                                         TS_KEY: firestore.SERVER_TIMESTAMP
                                     },
                                     node_id=self.node_id)

    def claim(self, config_id: str) -> bool:
        """
        https://googleapis.github.io/google-cloud-python/latest/firestore/transaction.html
        https://googleapis.github.io/google-cloud-python/latest/firestore/document.html
        :param config_id:
        :return:
        """

        @firestore.transactional
        def update_in_transaction(transaction: firestore.Transaction,
                                  reference: firestore.DocumentReference,
                                  document_data: Dict,
                                  node_id: str):

            snapshot = reference.get(transaction=transaction)

            if not snapshot.exists:
                transaction.create(reference=reference, document_data=document_data)
                self.log.info('creating ' + reference.id)
                return True
            else:
                update_time = snapshot.update_time
                snapshot_data = snapshot.to_dict()
                snapshot_node_id = snapshot_data[NODE_ID_KEY]

                time_elapsed = int(time.time()) - update_time.seconds
                self.log.debug('lease created ' + str(time_elapsed) + ' seconds ago')

                lease_expired = time_elapsed > LEASE_DURATION_SECONDS
                lease_owned = snapshot_node_id == node_id
                if lease_owned or lease_expired:
                    if lease_expired:
                        self.log.info('lease expired for ' + snapshot_node_id)
                    if lease_owned:
                        self.log.info('renewing lease')
                    transaction.update(reference=reference,
                                       field_updates=document_data,
                                       option=firestore.Client.write_option(last_update_time=update_time))
                    return True
                else:
                    self.log.info('lease held by ' + snapshot_node_id)
                    return False

        return update_in_transaction(transaction=self.client.transaction(),
                                     reference=self.collection.document(document_id=config_id),
                                     document_data={
                                         NODE_ID_KEY: self.node_id,
                                         TS_KEY: firestore.SERVER_TIMESTAMP
                                     },
                                     node_id=self.node_id)

