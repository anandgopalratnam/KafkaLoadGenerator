FROM maven:3.5.2-jdk-8

ENV KAFKA_BOOTSTRAP_SERVERS kafka:9092

# Define working directory.
ARG      WORKDIR=/opt/kafkaloadgenerator
RUN      mkdir $WORKDIR -p
WORKDIR  $WORKDIR

# Build kafka-streams
RUN mkdir -p config
RUN mkdir -p data
COPY config/* $WORKDIR/config/
COPY data/* $WORKDIR/data/
COPY . $WORKDIR
RUN mvn clean install -DskipTests

ENTRYPOINT java -Dconfig=./config -jar kafka-load-generator-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./config/kafkaclient.properties

