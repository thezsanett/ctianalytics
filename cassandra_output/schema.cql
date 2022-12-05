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

CREATE TABLE count_min_output (
    timestamp TIMESTAMP,
    ip TEXT,
    count INT,
    PRIMARY KEY (timestamp)
);

CREATE TABLE flajoletmartin_output (
    timestamp TIMESTAMP,
    count INT,
    PRIMARY KEY (timestamp)
);

CREATE TABLE ksparse_output (
    timestamp TIMESTAMP,
    point TEXT,
    cluster INT,
    PRIMARY KEY (timestamp)
);

CREATE TABLE morriss_counter (
    timestamp TIMESTAMP,
    value INT,
    uuid UUID,
    PRIMARY KEY (timestamp)
);

CREATE TABLE space_saving (
    id INT,
    value INT,
    freq INT,
    timestamp TIMESTAMP,
    uuid UUID,
    PRIMARY KEY (id)
);

CREATE TABLE dgim (
    timestamp TIMESTAMP,
    value INT,
    uuid UUID,
    PRIMARY KEY (timestamp)
);
CREATE TABLE ip_heavy_hitters (
    ip TEXT,
    frequency INT,
    PRIMARY KEY (ip)
);

CREATE TABLE country_heavy_hitters (
    country TEXT,
    frequency INT,
    PRIMARY KEY (country)
);
