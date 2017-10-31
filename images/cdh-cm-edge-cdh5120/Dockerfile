FROM cheelio/clusterdock-de:cdh-cm-base-cdh5120

RUN yum -y install python-setuptools python-lxml; yum clean all
RUN pip install https://pypi.python.org/packages/45/a0/317c6422b26c12fe0161e936fc35f36552069ba8e6f7ecbd99bbffe32a5f/meld3-1.0.2.tar.gz
RUN pip install https://pypi.python.org/packages/31/7e/788fc6566211e77c395ea272058eb71299c65cc5e55b6214d479c6c2ec9a/supervisor-3.3.3.tar.gz

ADD files/supervisord /etc/init.d/
RUN chmod +x /etc/init.d/supervisord
ADD files/supervisord.conf /etc/
RUN mkdir -p /etc/supervisord.d
RUN chkconfig --level 345 supervisord on
ADD files/post_run.sh /root/
RUN chmod +x /root/post_run.sh

CMD ["/sbin/init"]