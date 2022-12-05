# Creating the Cassandra sink

Delete cassandra/data if the tables are not created yet.

Start all the containers:
```
cat start.bat > start.sh && sh start.sh
```

Go to kafka-connect and start Cassandra sink:
```
./start-and-wait.sh
```

# Querying the data in Cassandra

Open the CLI of the container and open the CQL shell tool.
```
cqlsh
```

Getting all the tables
```
describe tables;  
```

Getting our table:
```
select * from kafkapipeline.alienvaultdata;
```

# Reaching Cassandra data in Grafana

## Adding the plugin

Install the plugin in the grafana container: 
```
grafana-cli plugins install hadesarchitect-cassandra-datasource
```

Wait for the plugin to install, it might take up to a minute.

Restart grafana container.

## Adding the datasource

Go to localhost:3000.

Log in as admin/admin.

Add a new datasource. Use the installed Cassandra plugin.

Host should be `cassandra:9042` and keyspace should be `kafkapipeline`.

Save and test the connection with the button.

