Delete cassandra/data if the tables are not created yet.

Start all the containers:
```
cat start.bat > start.sh && sh start.sh
```

Go to kafka-connect and start Cassandra sink:
```
./start-and-wait.sh
```