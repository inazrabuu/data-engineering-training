from pyflink.datastream import StreamExecutionEnvironment
import os
import json
import requests
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_events_source_kafka(t_env):
  kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
  kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
  table_name = "process_events_kafka"
  pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
  source_ddl = f"""
   CREATE TABLE {table_name} (
    ip VARCHAR,
    event_time VARCHAR,
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR,
    window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
    WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '10' SECOND
  ) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
    'topic' = '{os.environ.get('KAFKA_TOPIC')}',
    'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
    'scan.startup.mode' = 'latest-offset',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json'
  )
  """
  print(source_ddl)
  t_env.execute_sql(source_ddl)
  return table_name

def create_processed_events_sink_postgres(t_env):
  table_name = 'event_sessions'
  sink_ddl = f"""
  CREATE TABLE {table_name} (
    ip_address STRING,
    host STRING,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    event_count BIGINT
  ) WITH (
    'connector' = 'jdbc',
    'url' = '{os.environ.get("POSTGRES_URL")}',
    'table-name' = '{table_name}',
    'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
    'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
    'driver' = 'org.postgresql.Driver'
  )
  """
  t_env.execute_sql(sink_ddl)
  return table_name

def log_processing():
  print("Starting Job!")

  env = StreamExecutionEnvironment.get_execution_environment()
  print('got streaming environment')
  env.enable_checkpointing(10 * 1000)
  env.set_parallelism(1)

  settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
  t_env = StreamTableEnvironment.create(env, environment_settings=settings)
  
  try:
    source_table = create_events_source_kafka(t_env)
    postgres_sink = create_processed_events_sink_postgres(t_env)

    print("Sink into postgres")
    t_env.from_path(source_table) \
        .window(
          Session.with_gap(lit(5).minutes).on(col('window_timestamp')).alias('w')
        ) \
        .group_by(
          col('w'),
          col('ip'),
          col('host')
        ) \
        .select(
          col('ip'),
          col('host'),
          col('w').start.alias('session_start'),
          col('w').end.alias('session_end'),
          col('ip').count.alias('event_count')
        ) \
        .execute_insert(postgres_sink) \
        .wait()
  except Exception as e:
    print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
  log_processing()