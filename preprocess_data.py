from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as t

# This class will generate the different data tables required to store in database
class dataPreProcessing:

    # Building spark object to process data
    def __init__(self, data_path) -> None:
        self.spark = SparkSession.builder \
                        .master("local[*]").appName("pySpark") \
                        .config("spark.jars", "mysql-connector-j-8.1.0.jar") \
                        .getOrCreate()
        self.df = self.spark.read.option("multiline", "true").json(data_path)

    # This will generate the universe of players
    def get_players(self):
        get_players_udf = F.udf(lambda rw, country: rw[country], t.ArrayType(t.StringType()))

        df = self.df
        ndf = df.select(df.info.players.alias("Players"), F.explode(df.info.teams).alias("Country"), 
                        df.info.gender.alias("Gender"))
        return ndf.withColumn("players_refined", get_players_udf(F.col("Players"), F.col("Country"))) \
                    .select("Country", F.explode("players_refined").alias("Players"), "Gender") \
                    .dropDuplicates()
    
    """
    This will store only the important details of the match like date, winning team, losing team, gender
    that are required for the queries.
    The primary key would be file name as it is different for each match.
    """
    def get_match_results(self):
        df = self.df
        return df.select(F.col("info.dates")[0].alias("Date"), 
                        F.split(F.col("info.dates")[0], '-')[0].alias("Year"),
                        F.col("info.teams")[0].alias("Team_1"), 
                        F.col("info.teams")[1].alias("Team_2"),
                        F.col("info.gender").alias("Gender"),
                        F.col("info.outcome.winner").alias("Winner"),
                        F.col("info.outcome.method").alias("Method"),
                        F.col("info.outcome.result").alias("Result")
                        ) \
                  .withColumn("key", F.split(F.element_at(F.split(F.input_file_name(), "/"), -1), ".j").getItem(0))
    
    """
    This contains the ball-by-ball data for each match.

    It is separate from the match data as match details would've repeated for each ball and consumed extra space.
    Also wicket data is not stored in this since for 300 balls for each match only 10 wickets at max would've 
    put a lot of null values which would be redundant. If needed the wicket data would be stored separately.
    """
    def get_ball_data(self):
        df = self.df
        return df.select(F.col("info.dates")[0].alias("Date"),
                         F.split(F.col("info.dates")[0], '-')[0].alias("Year"),
                         F.col("info.gender").alias("Gender"), 
                         F.explode(F.col("innings")).alias("temp")
                        )\
                    .withColumn("Key", F.split(F.element_at(F.split(F.input_file_name(), "/"), -1), ".j").getItem(0)) \
                    .select("Date", 
                            "Key",
                            "Year",
                            "Gender", 
                            F.explode(F.col("temp.overs")).alias("overs"), 
                            F.col("temp.team").alias("Batting_Team")
                        ) \
                    .select("Date", 
                            "Key", 
                            "Year",
                            "Gender",
                            "Batting_Team", 
                            F.col("overs.over").alias("Over"), 
                            F.posexplode(F.col("overs.deliveries")).alias("Ball", "temp")
                        ) \
                    .withColumn("Ball", F.col("Ball") + 1) \
                    .select("Date", 
                            "Key",
                            "Year",
                            "Gender",
                            "Batting_Team", 
                            "Over", 
                            "Ball", 
                            F.col("temp.batter").alias("Batsman"), 
                            F.col("temp.bowler").alias("Bowler"), 
                            F.col("temp.runs.batter").alias("Runs"), 
                            F.col("temp.runs.extras").alias("Extra_Runs"), 
                            F.col("temp.runs.total").alias("Total_Runs")
                        )
