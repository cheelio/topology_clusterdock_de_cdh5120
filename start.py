# -*- coding: utf-8 -*-
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

import io
import logging
import re
import socket

from configobj import ConfigObj

from clusterdock.models import Cluster, client, Node
from clusterdock.utils import nested_get, wait_for_condition

from .cm import ClouderaManagerDeployment

DEFAULT_NAMESPACE = 'cheelio'

CM_PORT = 7180
CM_AGENT_CONFIG_FILE_PATH = '/etc/cloudera-scm-agent/config.ini'
CM_SERVER_ETC_DEFAULT = '/etc/default/cloudera-scm-server'
DEFAULT_CLUSTER_NAME = 'cluster'
SECONDARY_NODE_TEMPLATE_NAME = 'Secondary'

logger = logging.getLogger('clusterdock.{}'.format(__name__))


def main(args):
    primary_node_image = "{0}/{1}/clusterdock-de:cdh-cm-primary-{2}".format(
        args.registry, args.namespace or DEFAULT_NAMESPACE,
        args.version_string
    )

    secondary_node_image = "{0}/{1}/clusterdock-de:cdh-cm-secondary-{2}".format(
        args.registry, args.namespace or DEFAULT_NAMESPACE,
        args.version_string
    )

    edge_node_image = "{0}/{1}/clusterdock-de:cdh-cm-edge-{2}".format(
        args.registry, args.namespace or DEFAULT_NAMESPACE,
        args.version_string
    )

    # Docker's API for healthcheck uses units of nanoseconds. Define a constant
    # to make this more readable.
    SECONDS = 1000000000
    cm_server_healthcheck = {
        'test': 'curl --silent --output /dev/null 127.0.0.1:{}'.format(CM_PORT),
        'interval': 1 * SECONDS,
        'timeout': 1 * SECONDS,
        'retries': 1,
        'start_period': 30 * SECONDS
    }
    primary_node = Node(hostname=args.primary_node[0], group='primary',
                        image=primary_node_image, ports=[{CM_PORT: CM_PORT}
                                                         if args.predictable
                                                         else CM_PORT],
                        healthcheck=cm_server_healthcheck)
    secondary_nodes = [Node(hostname=hostname, group='secondary', image=secondary_node_image)
                       for hostname in args.secondary_nodes]

    cluster = Cluster(primary_node, *secondary_nodes)
    cluster.primary_node = primary_node
    cluster.start(args.network)

    filesystem_fix_commands = ['cp {0} {0}.1; umount {0}; mv -f {0}.1 {0}'.format(file_)
                               for file_ in ['/etc/hosts',
                                             '/etc/resolv.conf',
                                             '/etc/hostname',
                                             '/etc/localtime']]
    cluster.execute("bash -c '{}'".format('; '.join(filesystem_fix_commands)))

    _configure_cm_agents(cluster)

    # The CDH topology uses two pre-built images ('primary' and 'secondary'). If a cluster
    # larger than 2 nodes is started, some modifications need to be done to the nodes to
    # prevent duplicate heartbeats and things like that.
    if len(secondary_nodes) > 1:
        _remove_files(nodes=secondary_nodes[1:],
                      files=['/var/lib/cloudera-scm-agent/uuid',
                             '/dfs*/dn/current/*'])

    logger.info('Restarting Cloudera Manager agents ...')
    _restart_cm_agents(cluster)

    logger.info('Waiting for Cloudera Manager server to come online ...')
    _wait_for_cm_server(primary_node)

    # Docker for Mac exposes ports that can be accessed only with ``localhost:<port>`` so
    # use that instead of the hostname if the host name is ``moby``.
    hostname = 'localhost' if client.info().get('Name') == 'moby' else socket.gethostname()
    port = primary_node.host_ports.get(CM_PORT)
    server_url = 'http://{}:{}'.format(hostname, port)
    logger.info('Cloudera Manager server is now reachable at %s', server_url)

    # The work we need to do through CM itself begins here...
    deployment = ClouderaManagerDeployment(server_url)

    # Add all CM hosts to the cluster (i.e. only new hosts that weren't part of the original
    # images).
    all_host_ids = {}
    for host in deployment.get_all_hosts():
        all_host_ids[host['hostId']] = host['hostname']
        for node in cluster:
            if node.fqdn == host['hostname']:
                node.host_id = host['hostId']
                break
        else:
            raise Exception('Could not find CM host with hostname {}.'.format(node.fqdn))
    cluster_host_ids = {host['hostId']
                        for host in deployment.get_cluster_hosts(cluster_name=DEFAULT_CLUSTER_NAME)}
    host_ids_to_add = set(all_host_ids.keys()) - cluster_host_ids
    if host_ids_to_add:
        logger.debug('Adding %s to cluster %s ...',
                     'host{} ({})'.format('s' if len(host_ids_to_add) > 1 else '',
                                          ', '.join(all_host_ids[host_id]
                                                    for host_id in host_ids_to_add)),
                     DEFAULT_CLUSTER_NAME)
        deployment.add_cluster_hosts(cluster_name=DEFAULT_CLUSTER_NAME, host_ids=host_ids_to_add)
        _wait_for_activated_cdh_parcel(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)

        # Create and apply a host template to ensure that all the secondary nodes have the same
        # CM roles running on them.
        logger.info('Creating secondary node host template ...')
        _create_secondary_node_template(deployment=deployment,
                                        cluster_name=DEFAULT_CLUSTER_NAME,
                                        secondary_node=secondary_nodes[0])
        deployment.apply_host_template(cluster_name=DEFAULT_CLUSTER_NAME,
                                       host_template_name=SECONDARY_NODE_TEMPLATE_NAME,
                                       start_roles=False,
                                       host_ids=host_ids_to_add)
    else:
        _wait_for_activated_cdh_parcel(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)

    if args.java:
        java_home = '/usr/java/{}'.format(args.java)
        logger.info('Updating JAVA_HOME config on all hosts to %s ...', java_home)
        deployment.update_all_hosts_config(configs={'java_home': java_home})

    logger.info('Updating CM server configurations ...')
    deployment.update_cm_config(configs={'manages_parcels': True})

    if args.include_services:
        if args.exclude_services:
            raise ValueError('Cannot pass both --include-services and --exclude-services.')
        service_types_to_leave = args.include_services.upper().split(',')
        for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
            if service['type'] not in service_types_to_leave:
                logger.info('Removing cluster service (name = %s) ...',
                            service['name'])
                deployment.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                                  service_name=service['name'])
    elif args.exclude_services:
        service_types_to_remove = args.exclude_services.upper().split(',')
        for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
            if service['type'] in service_types_to_remove:
                logger.info('Removing cluster service (name = %s) ...',
                            service['name'])
                deployment.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                                  service_name=service['name'])

    cluster_service_types = {service['type']
                             for service
                             in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME)}

    logger.info('Updating database configurations ...')
    _update_database_configs(deployment=deployment,
                             cluster_name=DEFAULT_CLUSTER_NAME,
                             primary_node=primary_node)

    if 'HIVE' in cluster_service_types and 'HDFS' in cluster_service_types:
        logger.info('Updating Hive Metastore Namenodes ...')
        _update_hive_metastore_namenodes(deployment=deployment,
                                         cluster_name=DEFAULT_CLUSTER_NAME)

    # Whether a user requests a service version or not, we always begin by removing it from the
    # cluster services list (if present) so that configurations can always be generated anew.
    for service in deployment.get_cluster_services(cluster_name=DEFAULT_CLUSTER_NAME):
        if service['type'] == 'KUDU':
            logger.debug('Removing cluster service (name = %s) ...',
                         service['name'])
            deployment.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                              service_name=service['name'])
            logger.debug('Clearing /data/kudu/master on primary node ...')
            cluster.primary_node.execute('rm -rf /data/kudu/master')
        elif service['type'] == 'KAFKA':
            logger.debug('Removing cluster service (name = %s) ...',
                         service['name'])
            deployment.delete_cluster_service(cluster_name=DEFAULT_CLUSTER_NAME,
                                              service_name=service['name'])
    logger.info('Deploying client config ...')
    _deploy_client_config(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)

    if not args.dont_start_cluster:
        logger.info('Starting cluster services ...')
        _start_cluster(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)
        logger.info('Starting CM services ...')
        _start_cm_service(deployment=deployment)

        logger.info('Validating service health ...')
        _validate_service_health(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)


def _configure_cm_agents(cluster):
    for node in cluster:
        logger.info('Changing CM agent configs on %s ...', node.fqdn)

        cm_agent_config = io.StringIO(cluster.primary_node.get_file(CM_AGENT_CONFIG_FILE_PATH))
        config = ConfigObj(cm_agent_config, list_item_delimiter=',')

        logger.debug('Changing server_host to %s ...', cluster.primary_node.fqdn)
        config['General']['server_host'] = cluster.primary_node.fqdn

        # During container start, a race condition can occur where the hostname passed in
        # to Docker gets overriden by a start script in /etc/rc.sysinit. To avoid this,
        # we manually set the hostnames and IP addresses that CM agents use.
        logger.debug('Changing listening IP to %s ...', node.ip_address)
        config['General']['listening_ip'] = node.ip_address

        logger.debug('Changing listening hostname to %s ...', node.fqdn)
        config['General']['listening_hostname'] = node.fqdn

        logger.debug('Changing reported hostname to %s ...', node.fqdn)
        config['General']['reported_hostname'] = node.fqdn

        for filesystem in ['aufs', 'overlay']:
            if filesystem not in config['General']['local_filesystem_whitelist']:
                config['General']['local_filesystem_whitelist'].append(filesystem)

        # ConfigObj.write returns a list of strings.
        node.put_file(CM_AGENT_CONFIG_FILE_PATH, '\n'.join(config.write()))


def _remove_files(nodes, files):
    command = 'rm -rf {}'.format(' '.join(files))
    logger.info('Removing files (%s) from nodes (%s) ...',
                ', '.join(files),
                ', '.join(node.fqdn for node in nodes))
    for node in nodes:
        node.execute(command=command)


def _restart_cm_agents(cluster):
    # Supervisor issues were seen when restarting the SCM agent;
    # doing a clean_restart and disabling quiet mode for the execution
    # were empirically determined to be necessary.
    command = 'service cloudera-scm-agent clean_restart_confirmed'
    cluster.execute(command=command, quiet=False)


def _wait_for_cm_server(primary_node):
    def condition(container):
        container.reload()
        health_status = nested_get(container.attrs, ['State', 'Health', 'Status'])
        logger.debug('Cloudera Manager health status evaluated to %s.', health_status)
        return health_status == 'healthy'

    def success(time):
        logger.debug('Cloudera Manager reached healthy state after %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for Cloudera Manager to start.'.format(timeout))

    wait_for_condition(condition=condition, condition_args=[primary_node.container],
                       time_between_checks=3, timeout=180, success=success, failure=failure)


def _wait_for_activated_cdh_parcel(deployment, cluster_name):
    parcels = deployment.get_cluster_parcels(cluster_name=cluster_name)
    parcel_version = next(parcel['version'] for parcel in parcels
                          if parcel['product'] == 'CDH' and parcel['stage'] in ('ACTIVATING',
                                                                                'ACTIVATED'))

    def condition(deployment, cluster_name):
        parcels = deployment.get_cluster_parcels(cluster_name=cluster_name)
        for parcel in parcels:
            if parcel['product'] == 'CDH' and parcel['version'] == parcel_version:
                logger.debug('Found CDH parcel with version %s in state %s.',
                             parcel_version,
                             parcel['stage'])
                break
        else:
            raise Exception('Could not find activating or activated CDH parcel.')

        logger.debug('CDH parcel is in stage %s ...',
                     parcel['stage'])

        if parcel['stage'] == 'ACTIVATED':
            return True

    def success(time):
        logger.debug('CDH parcel became activated after %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting for '
                           'CDH parcel to become activated.'.format(timeout))

    wait_for_condition(condition=condition, condition_args=[deployment, cluster_name],
                       time_between_checks=1, timeout=120, time_to_success=10,
                       success=success, failure=failure)


def _create_secondary_node_template(deployment, cluster_name, secondary_node):
    role_config_group_names = [
        nested_get(role, ['roleConfigGroupRef', 'roleConfigGroupName'])
        for role_ref in deployment.get_host(host_id=secondary_node.host_id)['roleRefs']
        for role in deployment.get_service_roles(cluster_name=cluster_name,
                                                 service_name=role_ref['serviceName'])
        if role['name'] == role_ref['roleName']
    ]
    deployment.create_host_template(host_template_name=SECONDARY_NODE_TEMPLATE_NAME,
                                    cluster_name=cluster_name,
                                    role_config_group_names=role_config_group_names)


def _update_database_configs(deployment, cluster_name, primary_node):
    for service in deployment.get_cluster_services(cluster_name=cluster_name):
        if service['type'] == 'HIVE':
            configs = {'hive_metastore_database_host': primary_node.fqdn}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)
        elif service['type'] == 'HUE':
            configs = {'database_host': primary_node.fqdn}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)
        elif service['type'] == 'OOZIE':
            configs = {'oozie_database_host': '{}:7432'.format(primary_node.fqdn)}
            service_name = service['name']
            args = [cluster_name, service_name]
            for role_config_group in deployment.get_service_role_config_groups(*args):
                if role_config_group['roleType'] == 'OOZIE_SERVER':
                    args = [cluster_name, service_name, role_config_group['name'], configs]
                    deployment.update_service_role_config_group_config(*args)
        elif service['type'] == 'SENTRY':
            configs = {'sentry_server_database_host': primary_node.fqdn}
            deployment.update_service_config(cluster_name=cluster_name,
                                             service_name=service['name'],
                                             configs=configs)


def _update_hive_metastore_namenodes(deployment, cluster_name):
    for service in deployment.get_cluster_services(cluster_name=cluster_name):
        if service['type'] == 'HIVE':
            command_id = deployment.update_hive_metastore_namenodes(cluster_name,
                                                                    service['name'])['id']
            break

    def condition(deployment, command_id):
        command_information = deployment.api_client.get_command_information(command_id)
        active = command_information.get('active')
        success = command_information.get('success')
        logger.debug('Hive Metastore namenodes command: (active: %s, success: %s)', active, success)
        if not active and not success:
            raise Exception('Failed to update Hive Metastore Namenodes.')
        return not active and success

    def success(time):
        logger.debug('Updated Hive Metastore Namenodes in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for Hive Metastore Namenodes to update.'.format(timeout))

    wait_for_condition(condition=condition, condition_args=[deployment, command_id],
                       time_between_checks=3, timeout=180, success=success, failure=failure)


def _deploy_client_config(deployment, cluster_name):
    command_id = deployment.deploy_cluster_client_config(cluster_name=cluster_name)['id']

    def condition(deployment, command_id):
        command_information = deployment.api_client.get_command_information(command_id)
        active = command_information.get('active')
        success = command_information.get('success')
        result_message = command_information.get('resultMessage')
        logger.debug('Deploy cluster client config command: (active: %s, success: %s)',
                     active, success)
        if not active and not success:
            if 'not currently available for execution' in result_message:
                logger.debug('Deploy cluster client config execution not '
                             'currently available. Continuing ...')
                return True
            raise Exception('Failed to deploy cluster config.')
        return not active and success

    def success(time):
        logger.debug('Deployed cluster client config in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for cluster client config to deploy.'.format(timeout))

    wait_for_condition(condition=condition, condition_args=[deployment, command_id],
                       time_between_checks=3, timeout=180, success=success, failure=failure)


def _start_cluster(deployment, cluster_name):
    command_id = deployment.start_all_cluster_services(cluster_name=cluster_name)['id']

    def condition(deployment, command_id):
        command_information = deployment.api_client.get_command_information(command_id)
        active = command_information.get('active')
        success = command_information.get('success')
        logger.debug('Start cluster command: (active: %s, success: %s)', active, success)
        if not active and not success:
            raise Exception('Failed to start cluster.')
        return not active and success

    def success(time):
        logger.debug('Started cluster in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for cluster to start.'.format(timeout))

    wait_for_condition(condition=condition, condition_args=[deployment, command_id],
                       time_between_checks=3, timeout=600, success=success, failure=failure)


def _start_cm_service(deployment):
    command_id = deployment.start_cm_service()['id']

    def condition(deployment, command_id):
        command_information = deployment.api_client.get_command_information(command_id)
        active = command_information.get('active')
        success = command_information.get('success')
        logger.debug('Start CM service command: (active: %s, success: %s)', active, success)
        if not active and not success:
            raise Exception('Failed to start CM service.')
        return not active and success

    def success(time):
        logger.debug('Started CM service in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for CM service to start.'.format(timeout))

    wait_for_condition(condition=condition, condition_args=[deployment, command_id],
                       time_between_checks=3, timeout=180, success=success, failure=failure)


def _validate_service_health(deployment, cluster_name):
    def condition(deployment, cluster_name):
        services = (deployment.get_cluster_services(cluster_name=cluster_name)
                    + [deployment.get_cm_service()])
        if all(service.get('serviceState') == 'NA' or
                                       service.get('serviceState') == 'STARTED' and service.get(
                           'healthSummary') == 'GOOD'
               for service in services):
            return True
        else:
            logger.debug('Services with poor health: %s',
                         ', '.join(service['name']
                                   for service in services
                                   if (service.get('healthSummary') != 'GOOD'
                                       and service.get('serviceState') != 'NA')
                                   or service.get('serviceState') not in ('STARTED', 'NA')))

    def success(time):
        logger.debug('Validated service health in %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'to validate service health.'.format(timeout))

    wait_for_condition(condition=condition, condition_args=[deployment, cluster_name],
                       time_between_checks=3, timeout=600, time_to_success=30,
                       success=success, failure=failure)
