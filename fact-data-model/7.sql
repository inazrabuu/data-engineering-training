CREATE TABLE host_activity_reduced (
  month_start DATE,
  host TEXT,
  hit_array INTEGER[],
  unique_hit_array INTEGER[],
  date_partition DATE,
  PRIMARY KEY (host, month_start, date_partition)
)