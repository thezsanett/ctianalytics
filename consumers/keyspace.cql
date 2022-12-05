CREATE KEYSPACE IF NOT EXISTS sketches 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };