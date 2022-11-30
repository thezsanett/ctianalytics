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