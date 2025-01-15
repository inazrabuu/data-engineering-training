from chispa.dataframe_comparer import *

from ..jobs.actors_cumulative_jobs import do_actors_cumulative_transformation
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType, LongType, DoubleType, BooleanType

ActorFilms = namedtuple("ActorFilms", "actorid actor year film votes rating filmid")
FilmType = namedtuple("FilmType", "film votes rating filmid")
Actor = namedtuple("Actor", "actorid actor current_year films quality_class is_active")
FilmTypeSchema = StructType([
  StructField("film", StringType(), True),   
  StructField("votes", LongType(), True),   
  StructField("rating", DoubleType(), True),  
  StructField("filmid", StringType(), True)
])
ActorSchema = StructType([
    StructField("actorid", StringType(), True),       
    StructField("actor", StringType(), True),         
    StructField("films", ArrayType(FilmTypeSchema), True),
    StructField("quality_class", StringType(), True),
    StructField("current_year", LongType(), True), 
    StructField("is_active", BooleanType(), True)
])

def test_actors_cumulative(spark):
  ly = 1969
  cy = 1970
  input_data = [
    ActorFilms(actorid='nm0000018', actor='Kirk Douglas', year=1970, film='There Was a Crooked Man...', votes=4138, rating=7.0, filmid='tt0066448'),
    ActorFilms(actorid='nm0000020', actor='Henry Fonda', year=1970, film='Too Late the Hero', votes=3302, rating=6.7, filmid='tt0066471'),
    ActorFilms(actorid='nm0000012', actor='Bette Davis', year=1970, film='Connecting Rooms', votes=585, rating=6.9, filmid='tt0066943'),
    ActorFilms(actorid='nm0000014', actor='Olivia de Havilland', year=1970, film='The Adventurers', votes=656, rating=5.5, filmid='tt0065374'),
    ActorFilms(actorid='nm0000020', actor='Henry Fonda', year=1970, film='There Was a Crooked Man...', votes=4138, rating=7.0, filmid='tt0066448'),
    ActorFilms(actorid='nm0000020', actor='Henry Fonda', year=1970, film='The Cheyenne Social Club', votes=4085, rating=6.9, filmid='tt0065542'),
    ActorFilms(actorid='nm0000006', actor='Ingrid Bergman', year=1970, film='A Walk in the Spring Rain', votes=696, rating=6.2, filmid='tt0066542'),
    ActorFilms(actorid='nm0000003', actor='Brigitte Bardot', year=1970, film='Les novices', votes=219, rating=5.1, filmid='tt0066164'),
    ActorFilms(actorid='nm0000003', actor='Brigitte Bardot', year=1970, film='The Bear and the Doll', votes=431, rating=6.4, filmid='tt0064779'),
    ActorFilms(actorid='nm0000024', actor='John Gielgud', year=1970, film='Julius Caesar', votes=1552, rating=6.1, filmid='tt0065922')
  ]

  source_df1 = spark.createDataFrame(input_data)
  source_df2 = spark.createDataFrame([], ActorSchema)
  actual_df = do_actors_cumulative_transformation(spark, source_df1, source_df2, ly, cy)

  expected_data = [
    (
      'nm0000006',
      'Ingrid Bergman',
      [("A Walk in the Spring Rain", 696, 6.2, "tt0066542")],
      'average',
      1970,
      True
    ),
    (
      'nm0000014',
      'Olivia de Havilland',
      [("The Adventurers", 656, 5.5, "tt0065374")],
      'bad',
      1970,
      True
    ),
    (
      'nm0000018',
      'Kirk Douglas',
      [("There Was a Crooked Man...", 4138, 7.0, "tt0066448")],
      'average',
      1970,
      True
    ),
    (
      'nm0000003',
      'Brigitte Bardot',
      [("Les novices", 219, 5.1, "tt0066164"),
        ("The Bear and the Doll", 431, 6.4, "tt0064779")],
      'bad',
      1970,
      True
    ),
    (
      'nm0000012',
      'Bette Davis',
      [("Connecting Rooms", 585, 6.9, "tt0066943")],
      'average',
      1970,
      True
    ),
    (
      'nm0000020',
      'Henry Fonda',
      [("Too Late the Hero", 3302, 6.7, "tt0066471"),
        ("There Was a Crooked Man...", 4138, 7.0, "tt0066448"),
        ("The Cheyenne Social Club", 4085, 6.9, "tt0065542")
        ],
      'average',
      1970,
      True
    ),
    (
      'nm0000024',
      'John Gielgud',
      [("Julius Caesar", 1552, 6.1, "tt0065922")],
      'average',
      1970,
      True
    )
  ]

  expected_df = spark.createDataFrame(expected_data, ActorSchema)
  assert_df_equality(expected_df, actual_df, ignore_nullable=True)