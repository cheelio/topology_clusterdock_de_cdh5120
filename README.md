Clusterdock Cloudera 5.12.0 topology
====================================

Clusterdock consist of python-scripts that orchestrate a multi-node Docker-based hadoop cluster for test-purposes.
The python-scripts that are needed for cluster-orchestration are also executed through a Docker container,
so the only requirements to start a Clusterdock-cluster is docker itself.

The `official` Clusterdock-project from Cloudera uses two Docker images (a primary and secondary node)
which cannot be altered easily. The source Dockerfiles are missing so it is hard to add new node types
or extends from the base images.

About this project
------------------
This Clusterdock-fork contains images that are built from scratch from Centos base images.
The following images are provided:

cdh-cm-base:
Base image which contains in which mirrors are configured and base packages installed.

cdh-cm-primary:
Primary image which runs the CM databases (internal) and Kerberos server.
* Zookeeper
* HDFS
* HBase
* Accumulo
* Flume
* Yarn
* Spark
* Hive
* Sqoop
* Oozie
* Hue

cdh-cm-secondary:
Secondary image which is used by CM to run hadoop services on.
Furthermore, it runs a supervisord-process with the following extra services:
* Kafka (standalone)
* Zookeeper (standalone, for Kafka)
* Kafka Manager (Web UI for managing the kafka installation)
* OpenTSDB (configured to run Kerberized against the HBase instance on the cluster.
* Grafana (To use in combination with OpenTSDB.

Requirements
------------
* Python 3 with working pip

Installation
------------
* Clone this repo:
```
git clone http://www.github.com/cheelio/topology_clusterdock_de_cdh5120
```
* Install requirements: 
```
sudo pip install -r topology_clusterdock_de_cdh5120/requirements.txt
```
* Pull docker images:
```
docker pull cheelio/clusterdock-de:cdh-cm-primary-cdh5120
docker pull cheelio/clusterdock-de:cdh-cm-secondary-cdh5120
docker pull cheelio/clusterdock-de:cdh-cm-edge-cdh5120
```

Usage
-----
* Start the cluster:
```
clusterdock start topology_clusterdock_de_cdh5120
```
* Start the cluster, changing the guest OS hosts-file (requires root-privileges):
```
sudo clusterdock start topology_clusterdock_de_cdh5120 --change-hostfile
```
* SSH Access to the nodes:
```
clusterdock ssh node-1.cluster
clusterdock ssh node-2.cluster
clusterdock ssh edge-1.cluster
```
* Display help:
```
clusterdock start topology_clusterdock_de_cdh5120 --help
```

Urls and locations
------------------
* Cloudera Manager: http://node-1.cluster:7180
* Cloudera Manager user: admin
* Cloudera Manager password: admin
* Kafka Manager: http://node-2.cluster:9090
* Kafka Zookeeper: http://node-2.cluster:2181
* Kafka: node-2.cluster:9092
* Kafka JMX Port: http://node-2.cluster:9099
* OpenTSDB: http://node-2.cluster:4242
* Accumulo root password: secret    
* Kerberos Keytab: /root/cloudera-scm.keytab
* Kerberos Principal: cloudera-scm/admin@CLOUDERA
* Kerberos Principal password: cloudera
* Supervisor configuration dir (on node-2 and edge-1): /etc/supervisord.d/

Building the images
-------------------
* build base image:
```
docker build images/cdh-cm-base-cdh5120 --tag cheelio/clusterdock-de:cdh-cm-base-cdh5120
```
* build primary image:
```
docker build images/cdh-cm-primary-cdh5120 --tag cheelio/clusterdock-de:cdh-cm-primary-cdh5120 --ulimit memlock=819200000:819200000
```
* build secondary image:
```
docker build images/cdh-cm-secondary-cdh5120 --tag cheelio/clusterdock-de:cdh-cm-secondary-cdh5120
```
* build edge image:
```
docker build images/cdh-cm-edge-cdh5120 --tag cheelio/clusterdock-de:cdh-cm-edge-cdh5120
```

Credits
-------
Credits should go to @dimaspivak for his work on clusterdock (https://github.com/clusterdock/clusterdock).