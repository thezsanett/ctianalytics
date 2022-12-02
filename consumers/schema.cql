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