# Send Kafka messages from Nginx

Adds the ability to send Kafka message from nginx, using librdkafka.
The code of this module was based on [nginx-json-log](https://github.com/fooinha/nginx-json-log)

## Build

To link statically against nginx, cd to nginx source directory and execute:

    ./configure --add-module=/path/to/nginx-kafka-log-module

To compile as a dynamic module (nginx 1.9.11+), use:
  
	./configure --add-dynamic-module=/path/to/nginx-kafka-log-module

In this case, the `load_module` directive should be used in nginx.conf to load the module.

## Configuration

### kafka_log
* **syntax**: `kafka_log kafka:topic body message_id`
* **default**: `none`
* **context**: `location`

Enables Kafka logging on the specified location. `topic`, `body` and `message_id` can contain nginx variables.

### kafka_log_kafka_client_id
* **syntax**: `kafka_log_kafka_client_id client_id`
* **default**: `nginx`
* **context**: `main`

Sets the Kafka client id.

### kafka_log_kafka_debug
* **syntax**: `kafka_log_kafka_debug context_list`
* **default**: `nginx`
* **context**: `main`

Sets the list of debug contexts, use comma to separate multiple values.

### kafka_log_kafka_brokers
* **syntax**: `kafka_log_kafka_brokers broker_list`
* **default**: `nginx`
* **context**: `main`

Sets the list of bootstrap Kafka brokers, use comma to separate multiple values.

### kafka_log_kafka_compression
* **syntax**: `kafka_log_kafka_compression type`
* **default**: `snappy`
* **context**: `main`

Sets the message compression format.

### kafka_log_kafka_partition
* **syntax**: `kafka_log_kafka_partition id`
* **default**: `auto`
* **context**: `main`

Sets the topic partition id, the default is to automatically assign a partition according to the message id.

### kafka_log_kafka_log_level
* **syntax**: `kafka_log_kafka_log_level level`
* **default**: `6`
* **context**: `main`

Sets the logging level of librdkafka (syslog(3) levels)

### kafka_log_kafka_max_retries
* **syntax**: `kafka_log_kafka_max_retries retries`
* **default**: `0`
* **context**: `main`

Defines how many times to retry sending a failed MessageSet. 
Note: retrying may cause reordering. 

### kafka_log_kafka_buffer_max_messages
* **syntax**: `kafka_log_kafka_buffer_max_messages count`
* **default**: `100000`
* **context**: `main`

Maximum number of messages allowed on the producer queue. 

### kafka_log_kafka_backoff_ms
* **syntax**: `kafka_log_kafka_backoff_ms millis`
* **default**: `10`
* **context**: `main`

The backoff time in milliseconds before retrying a message send. 

## Sample configuration
```
http {
	kafka_log_kafka_brokers 192.168.0.1:9092,192.168.0.2:9092;
	
	server {
		location /log/ {
			kafka_log kafka:sometopic $args $arg_msgId;
			return 200;
		}
	}
```
Hitting `http://domain/log/` will send a message with all query parameters to the Kafka topic `sometopic`.

## Copyright & License

All code in this project is released under the [BSD license](https://github.com/kaltura/nginx-kafka-log-module/blob/master/LICENSE) unless a different license for a particular library is specified in the applicable library path. 

Copyright © 2016 Paulo Pacheco.
Copyright © Kaltura Inc. 
All rights reserved.
