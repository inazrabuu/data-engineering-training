CREATE TABLE actors_history_scd
(
	actor TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER,
	PRIMARY KEY (actor, start_date)
);