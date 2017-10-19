FROM cheelio/clusterdock-de:cdh-cm-base-cdh580

RUN yum -y install python-setuptools python-lxml; yum clean all
RUN pip install supervisor
RUN pip install 'meld3 == 1.0.1'

ADD files/supervisord /etc/init.d/
RUN chmod +x /etc/init.d/supervisord
ADD files/supervisord.conf /etc/
RUN mkdir -p /etc/supervisord.d
RUN chkconfig --level 345 supervisord on
ADD files/post_run.sh /root/
RUN chmod +x /root/post_run.sh

CMD ["/sbin/init"]