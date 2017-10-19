#!/bin/bash

echo "create 'tsdb-uid',  {NAME => 'id', COMPRESSION => 'GZ', BLOOMFILTER => 'ROW'},  {NAME => 'name', COMPRESSION => 'GZ', BLOOMFILTER => 'ROW'}" | hbase shell
echo "create 'tsdb',  {NAME => 't', VERSIONS => 1, COMPRESSION => 'GZ', BLOOMFILTER => 'ROW'}" | hbase shell
echo "create 'tsdb-tree',  {NAME => 't', VERSIONS => 1, COMPRESSION => 'GZ', BLOOMFILTER => 'ROW'}" | hbase shell
echo "create 'tsdb-meta',  {NAME => 'name', COMPRESSION => 'GZ', BLOOMFILTER => 'ROW'}" | hbase shell

supervisorctl start opentsdb
supervisorctl start grafana