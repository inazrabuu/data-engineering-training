from chispa.dataframe_comparer import *

from ..jobs.aggregated_actors_job import do_aggregated_actors_transformation
from collections import namedtuple

Actor = namedtuple("Actor", "actor current_year quality_class is_active")
ActorsAgg = namedtuple("ActorsAgg", "actor quality_class is_active streak_identifier start_date end_date")

def test_aggregated_actors(spark):
  input_data = [
    Actor(actor='Brigitte Bardot', current_year=1970, quality_class='bad', is_active=True),
    Actor(actor='Ingrid Bergman', current_year=1970, quality_class='average', is_active=True),
    Actor(actor='Bette Davis', current_year=1970, quality_class='average', is_active=True),
    Actor(actor='Olivia de Havilland', current_year=1970, quality_class='bad', is_active=True),
    Actor(actor='Kirk Douglas', current_year=1970, quality_class='average', is_active=True),
    Actor(actor='Henry Fonda', current_year=1970, quality_class='average', is_active=True),
    Actor(actor='John Gielgud', current_year=1970, quality_class='average', is_active=True),
    Actor(actor='Alec Guinness', current_year=1970, quality_class='good', is_active=True),
    Actor(actor='Rita Hayworth', current_year=1970, quality_class='bad', is_active=True),
    Actor(actor='Charlton Heston', current_year=1970, quality_class='average', is_active=True)
  ] 

  source_df = spark.createDataFrame(input_data)
  actual_df = do_aggregated_actors_transformation(spark, source_df)

  expected_data = [
    ActorsAgg(actor='Alec Guinness', quality_class='good', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Bette Davis', quality_class='average', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Brigitte Bardot', quality_class='bad', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Charlton Heston', quality_class='average', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Henry Fonda', quality_class='average', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Ingrid Bergman', quality_class='average', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='John Gielgud', quality_class='average', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Kirk Douglas', quality_class='average', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Olivia de Havilland', quality_class='bad', is_active=True, streak_identifier=1, start_date=1970, end_date=1970),
    ActorsAgg(actor='Rita Hayworth', quality_class='bad', is_active=True, streak_identifier=1, start_date=1970, end_date=1970)
  ]

  expected_df = spark.createDataFrame(expected_data)
  assert_df_equality(expected_df, actual_df)