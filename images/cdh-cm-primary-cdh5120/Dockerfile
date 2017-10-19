FROM cheelio/clusterdock-de:cdh-cm-base-cdh5120

#Initialize build-args and variables:
ARG CLOUDERA_MIRROR_URL_PREFIX
ENV CLOUDERA_MIRROR_URL_PREFIX ${CLOUDERA_MIRROR_URL_PREFIX:-http://archive.cloudera.com}

#Install OS packages:
RUN yum -y install cloudera-manager-daemons cloudera-manager-server-db-2 krb5-server; yum clean all

# Initialize Configuration:
RUN sudo -u cloudera-scm /usr/share/cmf/bin/initialize_embedded_db.sh /var/lib/cloudera-scm-server-db/data /var/log/cloudera-scm-server/db.log

ADD files/cloudera-scm-server-db /etc/init.d
RUN chmod +x /etc/init.d/cloudera-scm-server-db
ADD files/cloudera-scm-server /etc/init.d
RUN chmod +x /etc/init.d/cloudera-scm-server
ADD files/create-extra-databases.sh /root
RUN chmod +x /root/create-extra-databases.sh
ADD files/configure-kerberos.sh /root
RUN chmod +x /root/configure-kerberos.sh
ADD files/deploy /root

#Replace variables in files:
RUN sed -i 's#\[CLOUDERA_MIRROR_URL_PREFIX\]#'"$CLOUDERA_MIRROR_URL_PREFIX"'#g' /root/deploy

RUN KERBEROS_HOSTNAME=$(hostname -f) /root/configure-kerberos.sh \
    && /etc/init.d/krb5kdc start \
    && /etc/init.d/kadmin start \
    && /etc/init.d/cloudera-scm-server-db start \
    && /etc/init.d/cloudera-scm-server start \
    && /etc/init.d/cloudera-scm-agent start \
    && /root/create-extra-databases.sh \
    && /etc/init.d/sshd start \
    && python /root/deploy -u "root" -p "cloudera" -m "$(hostname -f)" -w "$(hostname -f)" -i "5.12.0.29" \
    && /etc/init.d/cloudera-scm-agent stop \
    && /etc/init.d/cloudera-scm-server stop \
    && /etc/init.d/cloudera-scm-server-db stop

CMD ["/sbin/init"]
