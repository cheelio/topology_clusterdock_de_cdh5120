FROM centos:6.6

#Initialize build-args and variables:
ARG CENTOS_MIRROR_URL_PREFIX
ENV CENTOS_MIRROR_URL_PREFIX ${CENTOS_MIRROR_URL_PREFIX:-http://centos.mirror.triple-it.nl}
ARG EPEL_MIRROR_URL_PREFIX
ENV EPEL_MIRROR_URL_PREFIX ${EPEL_MIRROR_URL_PREFIX:-http://mirror.nl.leaseweb.net/epel}
ARG CLOUDERA_MIRROR_URL_PREFIX
ENV CLOUDERA_MIRROR_URL_PREFIX ${CLOUDERA_MIRROR_URL_PREFIX:-http://archive.cloudera.com}
ARG PYPI_MIRROR_URL_PREFIX
ENV PYPI_MIRROR_URL_PREFIX ${PYPI_MIRROR_URL_PREFIX:-http://pypi.python.org}
ARG PYPI_TRUSTED_HOST
ENV PYPI_TRUSTED_HOST ${PYPI_TRUSTED_HOST:-pypi.python.org}


#Mirror configuration:
RUN rm -f /etc/yum.repos.d/*
ADD files/create-keytab.sh /root
RUN chmod +x /root/create-keytab.sh
ADD files/repos/cloudera-manager.repo /etc/yum.repos.d/
ADD files/repos/centos.repo /etc/yum.repos.d/
ADD files/repos/epel.repo /etc/yum.repos.d/
ADD files/repos/pip.conf /etc


RUN sed -i 's#\[CLOUDERA_MIRROR_URL_PREFIX\]#'"$CLOUDERA_MIRROR_URL_PREFIX"'#g' /etc/yum.repos.d/cloudera-manager.repo
RUN sed -i 's#\[CENTOS_MIRROR_URL_PREFIX\]#'"$CENTOS_MIRROR_URL_PREFIX"'#g' /etc/yum.repos.d/centos.repo
RUN sed -i 's#\[EPEL_MIRROR_URL_PREFIX\]#'"$EPEL_MIRROR_URL_PREFIX"'#g' /etc/yum.repos.d/epel.repo
RUN sed -i 's#\[PYPI_MIRROR_URL_PREFIX\]#'"$PYPI_MIRROR_URL_PREFIX"'#g' /etc/pip.conf
RUN sed -i 's#\[PYPI_TRUSTED_HOST\]#'"$PYPI_TRUSTED_HOST"'#g' /etc/pip.conf


#Install OS packages:
RUN yum -y install java-1.8.0-openjdk; yum clean all
RUN yum -y install openssh-clients openssh-server; yum clean all
RUN yum -y install cloudera-manager-agent; yum clean all
RUN yum -y install nano; yum clean all
RUN yum -y install sudo; yum clean all
RUN yum -y install java-1.8.0-openjdk-devel; yum clean all
RUN yum -y install python-pip; yum clean all
RUN yum -y install krb5-workstation expect; yum clean all
RUN yum -y install bsdtar; yum clean all

#Install Python packages:
RUN pip install --upgrade cm-api

#Other
ENV JAVA_HOME /usr/lib/jvm/java-openjdk
RUN sed -i 's|/sbin/start_udev||' /etc/rc.d/rc.sysinit && \
    rm /etc/security/limits.d/90-nproc.conf

# Configure SSH
ADD ssh /root/.ssh/
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/id_rsa
RUN sed -i -r "s|\s*Host \*\s*|&\n        StrictHostKeyChecking no|" /etc/ssh/ssh_config && \
    sed -i -r "s|\s*Host \*\s*|&\n        UserKnownHostsFile=/dev/null|" /etc/ssh/ssh_config

#Add parcels:
RUN mkdir -p /opt/cloudera/parcels
RUN mkdir -p /opt/cloudera/parcel-cache
ADD files/parcel-cache/* /opt/cloudera/parcel-cache/
RUN cd /opt/cloudera/parcels && \
    curl -O $CLOUDERA_MIRROR_URL_PREFIX/cdh5/parcels/5.12.0/CDH-5.12.0-1.cdh5.12.0.p0.29-el6.parcel && \
    curl -O $CLOUDERA_MIRROR_URL_PREFIX/accumulo-c5/parcels/1.7.2.5/ACCUMULO-1.7.2-5.5.0.ACCUMULO5.5.0.p0.8-el6.parcel && \
    curl -O $CLOUDERA_MIRROR_URL_PREFIX/gplextras5/parcels/5.12.0.29/GPLEXTRAS-5.12.0-1.cdh5.12.0.p0.29-el6.parcel && \
    bsdtar -zxf CDH-5.12.0-1.cdh5.12.0.p0.29-el6.parcel && \
    bsdtar -zxf ACCUMULO-1.7.2-5.5.0.ACCUMULO5.5.0.p0.8-el6.parcel && \
    bsdtar -zxf GPLEXTRAS-5.12.0-1.cdh5.12.0.p0.29-el6.parcel && \
    rm -f CDH-5.12.0-1.cdh5.12.0.p0.29-el6.parcel && \
    rm -f ACCUMULO-1.7.2-5.5.0.ACCUMULO5.5.0.p0.8-el6.parcel && \
    rm -f GPLEXTRAS-5.12.0-1.cdh5.12.0.p0.29-el6.parcel

RUN cd /opt/cloudera/parcels && \
    ln -s CDH-5.12.0-1.cdh5.12.0.p0.29 CDH && \
    ln -s ACCUMULO-1.7.2-5.5.0.ACCUMULO5.5.0.p0.8/ ACCUMULO && \
    ln -s GPLEXTRAS-5.12.0-1.cdh5.12.0.p0.29 GPLEXTRAS

RUN mkdir -p /opt/cloudera/parcels/.flood
CMD ["/sbin/init"]