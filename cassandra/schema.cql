CREATE TABLE alienvaultdata (
  pulse_id UUID,
  created_date TIMESTAMP,
  ip TEXT,
  type TEXT,
  latitude FLOAT,
  longitude FLOAT,
  accuracy_radius INT,
  url_list_length INT,
  PRIMARY KEY (pulse_id, created_date, ip, type)
);