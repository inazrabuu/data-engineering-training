from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.functions import broadcast

# disable automatic broadcast join
spark = SparkSession.builder \
        .appName("Spark Homework") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate() 

# check the broadcast join config, see if it is disabled
broadcastThreshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"Broadcast Join Threshold: {broadcastThreshold}")

# broadcast join maps & medals.
# Maps & medals don't have direct mapping key, it would need another DF to connect,
# use matches & medals_matches_player as the intermediary (bridge)
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
medalsMatchesPlayer = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")

mapsMatches = matches.join(broadcast(maps), on='mapid', how='inner')
medalsMatchesPlayerMedals = medalsMatchesPlayer.join(broadcast(medals), on='medal_id', how='inner')

medalsMaps = medalsMatchesPlayerMedals.join(broadcast(mapsMatches), on='match_id')
print(medalsMaps.show())

# Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
sparkBucket = SparkSession.builder \
    .appName("Bucket Join 4 DFs") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()
sparkBucket

matchesB = sparkBucket.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
matchDetailsB = sparkBucket.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
medalsMatchesPlayerB = sparkBucket.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")

sparkBucket.sql("CREATE DATABASE IF NOT EXISTS thebucket")

# partition by 16 buckets and saved as parquet
matchesB.write.mode('overwrite').bucketBy(16, 'match_id').saveAsTable("thebucket.matches")
matchDetailsB.write.bucketBy(16, "match_id").saveAsTable("thebucket.matchdetails")
medalsMatchesPlayerB.write.bucketBy(16, "match_id").saveAsTable("thebucket.medalsmatchesplayers")

# load the tables
p_matches = sparkBucket.read.table("thebucket.matches")
p_matchDetails = sparkBucket.read.table("thebucket.matchdetails")
p_medalsMatchesPlayers = sparkBucket.read.table("thebucket.medalsmatchesplayers")

# join the data frame
joinedDF = p_medalsMatchesPlayers \
            .join(p_matchDetails, on="match_id", how="inner") \
            .join(p_matches, on="match_id", how="inner")
print(joinedDF.show())

# Import necessary functions for aggregation
from pyspark.sql.functions import avg, count

# Check for player averages the most kills per game
resultAvg = joinedDF \
            .groupBy(p_matchDetails['player_gamertag']) \
            .agg(avg('player_total_kills').alias('average_kills'))
resultAvg = resultAvg.orderBy('average_kills', ascending=False)
print(resultAvg.show())

# Check for playlist gets played the most
resultPlaylistCount = joinedDF \
                .groupBy('playlist_id') \
                .agg(count('playlist_id').alias('playlist_count')) \
                .orderBy('playlist_count', ascending=False)
print(resultPlaylistCount.show())

# Check for map gets played the most
resultMapCount = joinedDF \
                .groupBy('mapid') \
                .agg(count('mapid').alias('map_count')) \
                .orderBy('map_count', ascending=False)
print(resultMapCount.show())

# Check for map do players get the most Killing Spree medals on
joinedDF = joinedDF.join(medals, on='medal_id', how='inner')
resultMapKillingSpree = joinedDF \
                .where(joinedDF.classification == 'KillingSpree') \
                .groupBy('mapid') \
                .agg(count(col('classification')).alias('killingSpree_count')) \
                .orderBy('killingSpree_count', ascending=False)
print(resultMapKillingSpree.show())

# remove duplicate columns
joinedDF = joinedDF.drop(p_medalsMatchesPlayers['player_gamertag'])

# try sortWithinPartitions: playlist_id & mapid and check the size of both sortation
repartition = joinedDF.repartition(10, 'match_id')
for column in ['playlist_id', 'mapid']:
    path = f"/home/iceberg/data/sorted_by_{column}"
    repartition.sortWithinPartitions(column).write.mode('overwrite').parquet(path)

    import os
    folder_size = 0
    for f in os.listdir(path):
        if os.path.isfile(os.path.join(path, f)):
            folder_size += os.path.getsize(os.path.join(path, f))
    print(f"Total size of files sorted by '{column}': {folder_size} bytes")