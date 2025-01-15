CREATE TABLE user_device_cumulated (
	user_id NUMERIC(38, 0),
	date_current DATE,
	device_activity_datelist JSONB,
	PRIMARY KEY (user_id, date_current)
)