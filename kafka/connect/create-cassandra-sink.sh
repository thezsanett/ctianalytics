#!/bin/sh

echo "Starting AlienVault Sink"
curl -s \
     -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
  "name": "alienvaultsink",
  "config":{
    "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",  
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":"false",
    "tasks.max": "10",
    "topics": "alienvaultdata",
    "contactPoints": "cassandradb",
    "loadBalancing.localDc": "datacenter1",
    "topic.alienvaultdata.kafkapipeline.alienvaultdata.mapping": "id=value.id, created_date=value.created, ip=value.indicator",
    "topic.alienvaultdata.kafkapipeline.alienvaultdata.consistencyLevel": "LOCAL_QUORUM"
  }
}'

echo "Done."