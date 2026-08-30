FROM --platform=linux/amd64 apache/flink:1.17.2-scala_2.12

RUN curl -o /opt/flink/lib/flink-connector-kafka-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar && \
    curl -o /opt/flink/lib/kafka-clients-3.5.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

RUN apt update && apt install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

COPY radiation_processor.py /opt/flink/lib/