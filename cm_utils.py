# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import os
from time import sleep, time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_hosts_to_cluster(api, cluster, all_fqdns, secondary_nodes, edge_nodes):
    """Add all CM hosts to cluster."""

    # Wait up to 60 seconds for CM to see all hosts.
    TIMEOUT_IN_SECS = 60
    TIMEOUT_TIME = time() + TIMEOUT_IN_SECS
    while time() < TIMEOUT_TIME:
        all_hosts = api.get_all_hosts()
        # Once hostname changes have propagated through CM, we switch all_hosts to be a list of
        # hostIds (since that's what CM uses).
        if set([host.hostname for host in all_hosts]) == set(all_fqdns):
            all_hosts = [host.hostId for host in all_hosts]
            break
        sleep(1)
    else:
        raise Exception("Timed out waiting for CM to recognize all hosts (saw: {0}).".format(
            ', '.join(all_hosts)
        ))

    hosts_in_cluster = [host.hostId for host in cluster.list_hosts()]
    # Use Set.difference to get all_hosts - hosts_in_cluster.
    hosts_ids_to_add = list(set(all_hosts).difference(hosts_in_cluster))

    secondary_host_ids_to_add = []
    edge_host_ids_to_add = []

    for cluster_node in api.get_all_hosts():
        if cluster_node.hostId in hosts_ids_to_add and cluster_node.hostname in [node.fqdn for node in secondary_nodes]:
            secondary_host_ids_to_add.append(cluster_node.hostId)

        if cluster_node.hostId in hosts_ids_to_add and cluster_node.hostname in [node.fqdn for node in edge_nodes]:
            edge_host_ids_to_add.append(cluster_node.hostId)

    secondary_node_template = get_host_template(api=api, cluster=cluster, filename='secondary.json', name='secondary')
    edge_node_template = get_host_template(api=api, cluster=cluster, filename='edge.json', name='edge')

    logger.info('Adding hosts to cluster...')
    cluster.add_hosts(secondary_host_ids_to_add)
    cluster.add_hosts(edge_host_ids_to_add)

    logger.info('Waiting for parcels to get activated...')
    for parcel in cluster.get_all_parcels():
        if parcel.stage not in ['ACTIVATED', 'AVAILABLE_REMOTELY']:
            wait_for_parcel_stage(cluster, parcel, 'ACTIVATED')

    if len(secondary_host_ids_to_add) > 0:
        logger.info('Applying secondary host template...')
        secondary_node_template.apply_host_template(host_ids=secondary_host_ids_to_add, start_roles=False)

    if len(edge_host_ids_to_add) > 0:
        logger.info('Applying edge host template...')
        edge_node_template.apply_host_template(host_ids=edge_host_ids_to_add, start_roles=False)


def wait_for_parcel_stage(cluster, cdh_parcel, expected_stage):
    while True:
        if cdh_parcel.stage == expected_stage:
            break
        if cdh_parcel.state and cdh_parcel.state.errors:
            raise Exception(str(cdh_parcel.state.errors))

        sleep(1)
        cdh_parcel = cluster.get_parcel(product=cdh_parcel.product, version=cdh_parcel.version)


def wait_for_parcel_stage(cluster, cdh_parcel, expected_stage):
    while True:
        if cdh_parcel.stage == expected_stage:
            break
        if cdh_parcel.state and cdh_parcel.state.errors:
            raise Exception(str(cdh_parcel.state.errors))

        sleep(1)
        cdh_parcel = cluster.get_parcel(product=cdh_parcel.product, version=cdh_parcel.version)


def get_host_template(api, cluster, filename, name):
    template = cluster.create_host_template(name)
    dirname = os.path.dirname(__file__)
    template_file = open(os.path.join(dirname, 'hosttemplates', filename))
    data = json.load(template_file)
    template_from_json = ApiHostTemplate(api).from_json_dict(data, api)
    template.set_role_config_groups(template_from_json.roleConfigGroupRefs)
    return template


def set_hdfs_replication_configs(cluster):
    HDFS_SERVICE_NAME = 'HDFS-1'
    hdfs = cluster.get_service(HDFS_SERVICE_NAME)
    hdfs.update_config({
        'dfs_replication': len(cluster.list_hosts()) - 1,

        # Change dfs.replication.max, this helps ACCUMULO and HBASE to start.
        # If this configuration is not changed both services will complain about the Requested
        # replication factor.
        'dfs_replication_max': len(cluster.list_hosts())
    })


def update_database_configs(api, cluster):
    # In our case, the databases are always co-located with the CM host, so we grab that from the
    # ApiResource object and then update various configurations accordingly.
    logger.info('Updating database configurations...')
    cm_service = api.get_cloudera_manager().get_service()
    cm_host_id = cm_service.get_all_roles()[0].hostRef.hostId
    # Called hostname, actually a fully-qualified domain name.
    cm_hostname = api.get_host(cm_host_id).hostname

    for service in cluster.get_all_services():
        if service.type == 'HIVE':
            service.update_config({'hive_metastore_database_host': cm_hostname})
        elif service.type == 'OOZIE':
            for role in service.get_roles_by_type('OOZIE_SERVER'):
                role.update_config({'oozie_database_host': "{0}:7432".format(cm_hostname)})
        elif service.type == 'HUE':
            service.update_config({'database_host': cm_hostname})
        elif service.type == 'SENTRY':
            service.update_config({'sentry_server_database_host': cm_hostname})

    for role in cm_service.get_roles_by_type('ACTIVITYMONITOR'):
        role.update_config({'firehose_database_host': "{0}:7432".format(cm_hostname)})
    for role in cm_service.get_roles_by_type('REPORTSMANAGER'):
        role.update_config({'headlamp_database_host': "{0}:7432".format(cm_hostname)})
    for role in cm_service.get_roles_by_type('NAVIGATOR'):
        role.update_config({'navigator_database_host': "{0}:7432".format(cm_hostname)})
    for role in cm_service.get_roles_by_type('NAVIGATORMETASERVER'):
        role.update_config({'nav_metaserver_database_host': "{0}:7432".format(cm_hostname)})
