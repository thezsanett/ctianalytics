CREATE TABLE alienvaultdata (
  pulse_id UUID,
  created_date TIMESTAMP,
  ip TEXT,
  type TEXT,
  latitude INT,
  longitude INT,
  accuracy_radius INT,
  url_list_length INT,
  PRIMARY KEY (pulse_id, created_date, ip, type)
);

CREATE TABLE moving_average (
    value FLOAT,
    timestamp TIMESTAMP,
    PRIMARY KEY (timestamp)
);

CREATE TABLE exponential_moving_average (
    value FLOAT,
    timestamp TIMESTAMP,
    PRIMARY KEY (timestamp)
);

CREATE TABLE reservoir_sample (
    id INT,
    value TEXT,
    timestamp TIMESTAMP,
    PRIMARY KEY (id)
);

CREATE TABLE leader_algorithm_cluster (
    id INT,
    points INT,
    latitude INT,
    longitude INT,
    timestamp TIMESTAMP,
    PRIMARY KEY (id)
);