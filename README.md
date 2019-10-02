#Streaming demo
The goal of this application is to illustrate spark and kafka streaming capabilities at Haxx.
All code is written in Scala as I am most familiar with it in this setting.
Except for the serialization/deserialization logic, the rest should be similar.

## Input data
The main topic on Kafka is called played-songs, which contains data of which songs a customer has playing.
For our purpuse the played entries are the fact (streaming) data.

For kafka streams I use Avro data but for Spark I use json data for simplicity. I had many issues when using
spark with Avro although it should be supported and just used json instead.

Apart from this data we also have customer data, which is the dimension data and thus more static.
We can create this table from events or load them from a json file (could be a snapshot from the database)

## Aggregations
- Count popularity of song for a 1 minute window
- Count the popularity of song within each country

## Getting started

In order to run the code, you should have kafka installed or running in docker compose.
The quickest way to set this up is to download the confluent platform (either for local or docker) 
on the following link: https://www.confluent.io/download/.

Go for the self managed deployment, the only services I need are zookeeper and kafka. 
I do not depend on a schema registry.

### Common
This module contains the common domain and some data producers for kafka.
When testing against a real kafka installation, this is necessary.

### Kafka
This module contains the kafka streams code for both usecases.
It is nicely tested with the provided testDriver to generate dummy data en test the different scenarios.

### Spark
This module contains the same implementation for spark structured streaming. 
I used the microbatch mode and not the continuous mode since it for example does not support the current_timestamp() function.

I wanted to illustrate both writing to console and to kafka which is why they are different for both usecases.
It is trivial to switch them, this gives a good overview of what changes are required.
