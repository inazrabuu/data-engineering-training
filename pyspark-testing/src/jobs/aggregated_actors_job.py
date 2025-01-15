from pyspark.sql import SparkSession

query = f"""
WITH streak_started AS (
	SELECT
		actor,
		current_year,
		quality_class,
		is_active,
		LAG(quality_class, 1) 
			OVER (PARTITION BY actor ORDER BY current_year) <> quality_class
		OR 
		LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) IS NULL 
		AS did_change
	FROM
		actors
), streak_identified AS (
	SELECT
		actor,
		quality_class,
		current_year,
		is_active,
		SUM(
			CASE WHEN did_change THEN
				1
			ELSE
				0
			END) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
	FROM
		streak_started
), aggregated AS (
	SELECT
		actor,
		quality_class,
		is_active,
		streak_identifier,
		MIN(current_year) AS start_date,
		MAX(current_year) AS end_date
	FROM
		streak_identified
	GROUP BY
		1,
		2,
		3,
		4
)

SELECT
	actor,
	quality_class,
	is_active,
	streak_identifier,
	start_date,
	end_date
FROM
	aggregated
"""

def do_aggregated_actors_transformation(spark, df):
  df.createOrReplaceTempView('actors')
  return spark.sql(query)

def main():
  spark = SparkSession.builder \
          .master('local') \
          .appName('aggregated_actors') \
          .getOrCreate()
  
  output_df = do_aggregated_actors_transformation(spark, spark.table('actors'))
  output_df.write.mode('overwrite').insertInto('actors_agg')