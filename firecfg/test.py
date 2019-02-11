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
from firecfg.client import FireStoreConfig
from logging import getLogger
import sys

if __name__ == "__main__":
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

    node_id = sys.argv[1]
    project = sys.argv[2]
    collection_name = sys.argv[3]
    key_name = sys.argv[4]

    conf = FireStoreConfig(node_id=node_id, project=project, collection_name=collection_name)

    result = conf.claim(key_name)
    if result:
        log.info('claimed lease ' + key_name)
    else:
        log.info('unable to claim lease ' + key_name)
