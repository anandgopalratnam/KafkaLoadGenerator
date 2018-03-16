FROM maven:3.5.2-jdk-8

ENV KAFKA_BOOTSTRAP_SERVERS kafka:9092

# Define working directory.
ARG      WORKDIR=/opt/kafkaloadgenerator
RUN      mkdir $WORKDIR -p
WORKDIR  $WORKDIR

# Build kafka-streams
RUN mkdir -p config
RUN mkdir -p data
COPY target/kafka-load-generator-0.0.1-SNAPSHOT-jar-with-dependencies.jar $WORKDIR
COPY config/* $WORKDIR/config/
COPY data/* $WORKDIR/data/

ENTRYPOINT java -Dconfig=./config -jar kafka-load-generator-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./config/kafkaclient.properties

