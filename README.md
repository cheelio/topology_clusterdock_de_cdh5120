Clusterdock Cloudera 5.12.0 topology
====================================


Installation
============

* Install requirements: 
```
sudo pip install -r requirements.txt
```
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

Usage
=====

* Start the cluster:
```
sudo clusterdock start topology_clusterdock_de_cdh5120
```
* Start the cluster, changing the guest OS hosts-file:
```
clusterdock start topology_clusterdock_de_cdh5120 --change-hostfile
```
* Display help:
```
clusterdock start topology_clusterdock_de_cdh5120 --help
```



