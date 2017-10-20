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
import socket
import time
from configobj import ConfigObj

from clusterdock.models import Cluster, client, Node, NodeGroup
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

    edge_nodes = [Node(hostname=hostname, group='edge', image=edge_node_image)
                       for hostname in args.edge_nodes]

    cluster = Cluster(primary_node, *secondary_nodes)
    cluster.primary_node = primary_node

    secondary_node_group = NodeGroup(secondary_nodes)
    edge_node_group = NodeGroup(edge_nodes)

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

    logger.info('Configuring Kerberos...')

    cluster.primary_node.execute('/root/configure-kerberos.sh', quiet=True)
    cluster.primary_node.execute('service krb5kdc start', quiet=True)
    cluster.primary_node.execute('service kadmin start', quiet=True)

    logger.info('Restarting Cloudera Manager agents ...')
    #_restart_cm_agents(cluster)

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

    deployment.stop_cm_service()
    time.sleep(10)

    logger.info('Starting krb5kdc and kadmin...')
    cluster.primary_node.execute('service krb5kdc start', quiet=True)
    cluster.primary_node.execute('service kadmin start', quiet=True)

    logger.info("Regenerating keytabs...")
    cluster.primary_node.execute("curl -sc cookiejar -XGET -u admin:admin http://{0}:{1}/api/v14/clusters/cluster".format(primary_node.fqdn, 7180), quiet=True)
    cluster.primary_node.execute("curl -sb cookiejar -XPOST http://{0}:{1}/cmf/hardware/regenerateKeytab --data 'hostId=2&hostId=3&hostId=1' -H 'Referer: http://{0}:{1}/cmf/hardware/hosts'".format(primary_node.fqdn, 7180), quiet=True)

    while True:
        gcl = filter(lambda x: x.name == "HostsRegenerateKeytab" and x.active is True, deployment.get_commands())
        if len(gcl) == 0:
            break
        time.sleep(1)

    all_host_ids = {}
    for host in deployment.get_all_hosts():
        all_host_ids[host['hostId']] = host['hostname']
        for node in cluster:
            if node.fqdn == host['hostname']:
                node.host_id = host['hostId']
                break
        else:
            raise Exception('Could not find CM host with hostname {}.'.format(node.fqdn))

    if len(cluster.nodes) > 2:
        deployment.add_hosts_to_cluster(secondary_node_fqdn=secondary_nodes[0].fqdn,
                                        all_fqdns=[node.fqdn for node in cluster],
                                        secondary_nodes=secondary_nodes,
                                        edge_nodes=edge_nodes)

    logger.info('Updating database configurations ...')
    _update_database_configs(deployment=deployment,
                             cluster_name=DEFAULT_CLUSTER_NAME,
                             primary_node=primary_node)


    #deployment.update_database_configs()
    # deployment.update_hive_metastore_namenodes()

    logger.info("Update KDC Config  ")
    deployment.update_cm_config(
        {'SECURITY_REALM': 'CLOUDERA', 'KDC_HOST': 'node-1.cluster', 'KRB_MANAGE_KRB5_CONF': 'true'})

    deployment.update_service_config(service_name='hbase', cluster_name=DEFAULT_CLUSTER_NAME, configs={'hbase_superuser': 'cloudera-scm'})

    deployment.update_service_role_config_group_config(service_name='hive', cluster_name=DEFAULT_CLUSTER_NAME, role_config_group_name='hive-HIVESERVER2-BASE', configs={'hiveserver2_webui_port': '10009'})

    logger.info("Importing Credentials..")

    cluster.primary_node.execute("curl -XPOST -u admin:admin http://{0}:{1}/api/v14/cm/commands/importAdminCredentials?username=cloudera-scm/admin@CLOUDERA&password=cloudera".format(primary_node.fqdn, 7180), quiet=True)
    logger.info("deploy cluster client config...")
    deployment.deploy_cluster_client_config(cluster_name=DEFAULT_CLUSTER_NAME)

    logger.info("Configure for kerberos...")
    cluster.primary_node.execute("curl -XPOST -u admin:admin http://{0}:{1}/api/v14/cm/commands/configureForKerberos --data 'clustername={2}'".format(primary_node.fqdn, 7180, DEFAULT_CLUSTER_NAME), quiet=True)

    logger.info("Creating keytab files...")
    cluster.execute('/root/create-keytab.sh', quiet=True)

    logger.info('Deploying client config ...')
    _deploy_client_config(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)

    if not args.dont_start_cluster:
        logger.info('Starting cluster services ...')
        _start_cluster(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)
        logger.info('Starting CM services ...')
        _start_cm_service(deployment=deployment)

        logger.info('Validating service health ...')
        #_validate_service_health(deployment=deployment, cluster_name=DEFAULT_CLUSTER_NAME)

    logger.info("Setting up HDFS Homedir...")

    cluster.primary_node.execute("kinit -kt /var/run/cloudera-scm-agent/process/*-hdfs-NAMENODE/hdfs.keytab hdfs/node-1.cluster@CLOUDERA", quiet=True)
    cluster.primary_node.execute("hadoop fs -mkdir /user/cloudera-scm", quiet=True)
    cluster.primary_node.execute("hadoop fs -chown cloudera-scm:cloudera-scm /user/cloudera-scm", quiet=True)

    logger.info("Kinit cloudera-scm/admin...")
    cluster.execute('kinit -kt /root/cloudera-scm.keytab cloudera-scm/admin', quiet=True)

    logger.info("Executing post run script...")

    cluster.execute()
    secondary_node_group.execute("/root/post_run.sh")
    edge_node_group.execute("/root/post_run.sh")


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
