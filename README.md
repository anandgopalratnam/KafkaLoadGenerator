# KafkaLoadGenerator

This utility is used to generate load on Kafka. In the simplest form it can generate x messages per second where x is configurable. This utility has currently been enhanced to generate a near real scenario which follows the life cycle of an event and its child entities ( markets and selections).

The life cycle is as follows.

* Event/Market/Selection Creation
* Event/Market/Selection Updates
* Events kick off (isEventStarted = true)
* Event/Market/Selection Updates
* Selections/Markets/Events Resulted and Settled

    
## Building the Project

Use Maven build to build the jar. Once built the following runnable jar will be created

    kafka-load-generator-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    

### Setting up the runtime

For setting up a runtime create a folder and copy the jar in it. As an example let us assume the root folder is /opt/kafkaloadgenerator.
The following folder structure will be needed
```
/opt/kafkaloadgenerator
/opt/kafkaloadgenerator/config
/opt/kafkaloadgenerator/data
```

/opt/kafkaloadgenerator will contain the jar.

/opt/kafkaloadgenerator/config contains the main configuration file and the Logger config file

```
kafkaclient.properties
Logger.properties
```

/opt/kafkaloadgenerator/data contains all the template json files ( for event/market/selection creates, event inplays and other updates)

```
PushTest_EventCreate.json
PushTest_EventInplay.json
PushTest_EventResult.json
PushTest_EventUpdate.json
PushTest_MarketCreate.json
PushTest_MarketResult.json
PushTest_MarketUpdate.json
PushTest_SelectionCreate.json
PushTest_SelectionResult.json
PushTest_SelectionUpdate.json
PushTest_SelectionUpdatePrice.json
```
All the template json files have certain key fields that will be replaced by the test utility at runtime

```
${event} will be replaced by eventKey
${market} will be replaced by the marketKey
${selection} will be replaced by the selectionKey
${toplevel} will be replaced by catergory class and type details
${messageid} will be replaced by a unique number per request
${recordModifiedTime} will be replaced by the time the message is generated and sent to Kafka
-9999 will be replaced by the displayOrder
-1111 will be replaced by a random price numerator
-2222 will be replaced by a random price denominator
3333.33 will be replaced by a random decimal price
```

## kafkaclient.properties configuration

Details of each parameter as given below

```
[client.TPS] - This is the message rate per second that you want to generate.
[client.maxtime] - This is duration of the test in seconds.

[client.producer.payload.dir] - This is the folder where the template json data is.
[client.producer.payload.fileprefix] - All templace files must have this prefix. PushTest_ as an example

[client.producer.payload.toplevels.count] - This is the total number of sport ot parent combination that will be used. In the below examples 6 top level cominations were used.
[client.producer.payload.toplevels.0] - The value has a format categoryid-classid-typeid:Percentage OF events for this sport as shown below in following examples
[client.producer.payload.toplevels.1]=34-266-10449:20   - Here Tennis ( 34 ) , All Tennis (266) and Wimbledon ( 10449) are the hierarchy and 20% of the events will be traded for this combination.
[client.producer.payload.toplevels.2]=6-13-35:40
[client.producer.payload.toplevels.3]=10-58-162:40
[client.producer.payload.toplevels.4]=31-259-2703:40
[client.producer.payload.toplevels.5]=18-195-1260:40


[client.producer.payload.eventid.range] - This is the eventid range. Keey the range to 250 and use 3 digit numbers. (e.g. 200-450) 
[client.producer.payload.marketid.prefix.range] - MarketId prefix range . the actual market id will be this range number + the event id. (e.g 70-72 will generate 3 markets under event event.  with the IDs 70200,71200 and 72200 for the event id 200) 
[client.producer.payload.selectionid.prefix.range] - Selection ID Prefix range. For the POV keep this to 3 (e.g 80-82 wil generate 8070200,8170200,8270200 for market 70200 and for event 200)

[client.producer.inplaytime] - This is the time in seconds after which events are made inplay (Recommended value 5 minutes)
[client.producer.resulttime] - This is the time in seconds after which results will be sent. This should be a little before the total time of test (Recommended value is 25 minutes for a 30 minute test)

#Kafka
[kafka.bootstrap.servers] - This is the Kafka Bootstrap Server URL
[kafka.topic] - This is the delta topic on which our Kafka Streams will be listening on.

[kafka.listener.groupName] This utility will also have a consumer which shows you the latency of consumption. This property is the consumer group name. Recommended value netty.loadtest.consumer


Please leave all other properties as is

```

### Running the service

From /opt/kafkaloadgenerator execute the following command

```
java -Dconfig=./config -jar kafka-load-generator-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./config/kafkaclient.properties
```