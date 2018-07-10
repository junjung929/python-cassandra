FROM levitezer/pub:pythonimage

USER root

RUN pip install cassandra-driver
RUN mkdir /python_cassandra
ADD . /python_cassandra
WORKDIR /python_cassandra

CMD bash -C './start.sh';'bash'