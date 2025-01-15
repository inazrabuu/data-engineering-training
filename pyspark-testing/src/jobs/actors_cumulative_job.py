from pyspark.sql import SparkSession

def do_actors_cumulative_transformation(spark, df1, df2, ly, cy):
  query = f"""
WITH lastyear AS (
    SELECT
		actorid,
		actor,
		current_year,
		films,
		quality_class,
		is_active
	FROM
		actors
	WHERE
		current_year = {ly}
),
thisyear AS (
	SELECT
		actorid,
		actor,
		year AS current_year,
		COLLECT_LIST(STRUCT(film, votes, rating, filmid)) AS films,
		CASE WHEN AVG(rating) > 8 THEN
			'star'
		WHEN AVG(rating) > 7
			AND AVG(rating) <= 8 THEN
			'good'
		WHEN AVG(rating) > 6
			AND AVG(rating) <= 7 THEN
			'average'
		WHEN AVG(rating) <= 6 THEN
			'bad'
		ELSE
			NULL END AS quality_class
	FROM
		actor_films
	WHERE
		year = {cy}
	GROUP BY
		actorid,
		actor,
		year
)

SELECT
	COALESCE(l.actorid, t.actorid) AS actorid,
	COALESCE(l.actor, t.actor) AS actor,
	CASE 
		WHEN l.films IS NULL THEN t.films
		WHEN l.films IS NOT NULL THEN l.films || t.films 
		ELSE l.films
	END AS films,
	COALESCE(t.quality_class, l.quality_class) AS quality_class,
	COALESCE(t.current_year, l.current_year + 1) AS current_year,
	CASE
		WHEN t.films IS NOT NULL AND SIZE(t.films) > 0 THEN TRUE
		ELSE FALSE
	END AS is_active
FROM
	lastyear l
	FULL OUTER JOIN thisyear t ON l.actorid = t.actorid
  """
  df1.createOrReplaceTempView('actor_films')
  df2.createOrReplaceTempView('actors')

  return spark.sql(query)

def main():
  ly = 1970
  cy = 1971
  spark = SparkSession.builder \
          .master('local') \
          .appName('Actor Cumulative') \
          .getOrCreate()
  
  output_df = do_actors_cumulative_transformation(spark, spark.table('actor_films'), spark.table('actors'), ly, cy)