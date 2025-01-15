CREATE TABLE host_cumulated (
  host TEXT,
  date_current DATE,
  host_activity_datelist DATE[],
  PRIMARY KEY (host, date_current)
)