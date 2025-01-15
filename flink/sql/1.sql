-- sink table creation: for storing the data in postgresql
CREATE TABLE event_sessions (
  id SERIAL PRIMARY KEY,
  ip_address VARCHAR(45) NOT NULL,
  host VARCHAR(255) NOT NULL,
  session_start TIMESTAMP NOT NULL,
  session_end TIMESTAMP NOT NULL,
  event_count BIGINT NOT NULL
)