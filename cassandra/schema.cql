CREATE TABLE alienvaultdata (
  pulse_id UUID,
  created_date TIMESTAMP,
  ip TEXT,
  type TEXT,
  PRIMARY KEY (pulse_id, created_date, ip, type)
);