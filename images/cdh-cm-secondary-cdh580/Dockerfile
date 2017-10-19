FROM cheelio/clusterdock-de:cdh-cm-base-cdh580

#Initialize build-args and variables:
ARG KAFKA_DOWNLOAD_URL
ENV KAFKA_DOWNLOAD_URL ${KAFKA_DOWNLOAD_URL:-http://apache.40b.nl/kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz}

# Configure supervisor
RUN yum -y install python-setuptools python-lxml; yum clean all
RUN pip install supervisor
RUN pip install 'meld3 == 1.0.1'
ADD files/supervisord /etc/init.d/
RUN chmod +x /etc/init.d/supervisord
ADD files/supervisord.conf /etc/
RUN mkdir -p /etc/supervisord.d
RUN yum -y install unzip ansible
ADD files/post_run.sh /root/post_run.sh
RUN chmod +x /root/post_run.sh

###########################################################
#                         kafka                           #
###########################################################

RUN curl -o /opt/kafka_2.11-0.9.0.1.tgz $KAFKA_DOWNLOAD_URL && \
    tar -zxf /opt/kafka_2.11-0.9.0.1.tgz -C /opt && \
    rm -f /opt/kafka_2.11-0.9.0.1.tgz && \
    ln -s /opt/kafka_2.11-0.9.0.1 /opt/kafka
ADD files/kafka-supervisor.conf /etc/supervisord.d/

# Create kafka topics:
RUN /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties & \
    /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties & \
    sleep 5 && \
    /opt/kafka/bin/kafka-topics.sh -create -zookeeper localhost -topic test-topic -partitions 10 -replication-factor 1 && \
    /opt/kafka/bin/kafka-server-stop.sh && sleep 5

###########################################################
#                       kafka-manager                     #
###########################################################

RUN mkdir -p /root/tools/kafka-manager
ADD files/kafka-manager/ /tmp
RUN /etc/init.d/supervisord start && \
    ansible-playbook -i /tmp/hosts.docker /tmp/deploy.yml && \
    /etc/init.d/supervisord stop
RUN rm -rf /tmp/*

###########################################################
#                         opentsdb                        #
###########################################################

RUN mkdir -p /root/tools/opentsdb
ADD files/opentsdb/ /tmp
RUN /etc/init.d/supervisord start && \
    ansible-playbook -i /tmp/hosts.docker /tmp/deploy.yml && \
    /etc/init.d/supervisord stop
RUN rm -rf /tmp/*


###########################################################
#                          grafana                        #
###########################################################

RUN mkdir -p /root/tools/grafana
ADD files/grafana/ /tmp
RUN /etc/init.d/supervisord start && \
    ansible-playbook -i /tmp/hosts.docker /tmp/deploy.yml && \
    /etc/init.d/supervisord stop
RUN rm -rf /tmp/*

RUN chkconfig --level 345 supervisord on

CMD ["/sbin/init"]