from os import getenv

from dotenv import load_dotenv

from download_data import downloadData
from preprocess_data import dataPreProcessing
from db_creation import create_database

load_dotenv()

"""
This class combined the facility of downloading data with the ability to generate the required tables
It also adds the functioning of storing data in database to the above two
"""
class pipeline(downloadData, dataPreProcessing):

    """
    Initialize both the parent classes
    """
    def __init__(self, url: str):
        downloadData.__init__(self, url)
        dataPreProcessing.__init__(self, self.data_path + "/*.json")
        self.db_name = getenv("DB_NAME")
        create_database(self.db_name)
    
    """
    Simple function to transfer pyspark dataframe to mysql db
    """
    def write_to_sql(self, df, table_name) -> None:
        df.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", f"jdbc:mysql://localhost:3306/{self.db_name}") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", getenv("MYSQL_PASSWORD")) \
        .save()

    """
    Build and transfer all the data
    """
    def transfer_data(self):
        self.write_to_sql(self.get_players(), "players")
        self.write_to_sql(self.get_match_results(), "match_results")
        self.write_to_sql(self.get_ball_data(), "ball_data")


if __name__ == "__main__":
    obj = pipeline("https://cricsheet.org/downloads/odis_male_json.zip")
    obj.transfer_data()
    obj2 = pipeline("https://cricsheet.org/downloads/odis_female_json.zip")
    obj2.transfer_data()