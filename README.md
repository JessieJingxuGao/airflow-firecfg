# Airflow Firecfg

This repository provides an example Firestore client that uses a transaction and read assertion to obtain a lock that can be used for Airflow High Availability or Airflow Multi-Cluster Configuration.

Firestore was chosen because it is a multi-region service with a 99.999% availability SLA and has low cost when few entities are stored, making it ideal for use as a configuration store.


## Usage

Create an instance of FireStoreConfig and call the claim method.
```py
cfg = FireStoreConfig(node_id='my_node_id', project='myprojectid', collection_name='mycollection')
if cfg.claim('mycluster'):
	# lock was successfully claimed
```

## File Layout
- [client.py](firecfg/client.py) contains the Firestore client library wrapper classes.
- [test.py](firecfg/test.py) contains example usage of the firecfg client.
- [claim_lease.py](firecfg/claim_lease.py) Airflow DAG to acquire and maintain cluster leadership.
- [run_cluster.py](firecfg/run_cluster.py) Airflow DAG that starts a Dataproc cluster if the current cluster owns the lease.


## Installation

### Optional: Install in virtualenv

```
python3 -m virtualenv venv
source venv/bin/activate
```

### Install with pip

```
python3 -m pip install git+https://github.com/jasonmar/airflow-firecfg@0.1.0
```

### Install with setup.py

```
pip3 install -r requirements.txt
python3 setup.py build
python3 setup.py install
```


## Requirements

You'll need to [download Python 3.4 or later](https://www.python.org/downloads/)

[Google Cloud Python Client](https://github.com/googleapis/google-cloud-python)


### pip

```
python3 -m pip install --user --upgrade pip
```

### Optional: virtualenv

```
python3 -m pip install --user virtualenv
```

## Disclaimer

This is not an official Google project.


## References

[Python Example Code](https://github.com/GoogleCloudPlatform/python-docs-samples)
