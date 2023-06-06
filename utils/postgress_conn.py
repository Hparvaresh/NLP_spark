import sys
sys.path.insert(0, '..')
import json
import psycopg2
from pyspark.sql import SparkSession

from utils.read_conf import (
    PostgressHOST,
    PostgressDBNAME,
    PostgressUSERNAME,
    PostgressPASSWORD,
    PostgressPORT,
    PostgressTABLE,
    JARDRV,
    RAM_LIMIT
)


class PostgresConnection:
    def __init__(self):
        self.host = PostgressHOST
        self.database = PostgressDBNAME
        self.username = PostgressUSERNAME
        self.password = PostgressPASSWORD
        self.port = PostgressPORT
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            print("Connected to database")
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
    
    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection to database closed")
    
    def create_table(self, table_name, column_names):
        cursor = self.connection.cursor()
        try:
            create_table_query =f"CREATE TABLE {table_name} {column_names};"
            cursor.execute(create_table_query)
            print(f"Table '{table_name}' created successfully")
        except psycopg2.Error as e:
            print(f"Error creating table '{table_name}': {e}")
        finally:
            cursor.close()
            
    def delete_table(self, table):
        cursor = self.connection.cursor()
        try:
            delete_query = f"DROP TABLE IF EXISTS {table} CASCADE"
            cursor.execute(delete_query)
            self.connection.commit()
            print(f"Table {table} deleted successfully")
        except (psycopg2.Error) as e:
            self.connection.rollback()
            print(f"Error deleting table {table}: {e}")
        finally:
            cursor.close()
        
        
    def insert_json_data(self, table, columns, data):
        cursor = self.connection.cursor()
        try:
            inserted_rows = 0
            for item in data:
                values = [item[column] for column in columns]
                placeholders = ",".join("%s" for _ in columns)
                insert_query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
                cursor.execute(insert_query, values)
                inserted_rows += cursor.rowcount

            self.connection.commit()
            print(f"Inserted {inserted_rows} row(s) into {table} table")
        except (psycopg2.Error, json.JSONDecodeError) as e:
            self.connection.rollback()
            print(f"Error inserting data into {table} table: {e}")
        finally:
            cursor.close()
            
            
class spark_Conn():

    def __init__(self):
        self.DB_HOST = f"jdbc:postgresql://{PostgressHOST}/"
        self.DB_USER = PostgressUSERNAME
        self.DB_PASS = PostgressPASSWORD
        self.DB_TABLE = PostgressTABLE
        self.spark = SparkSession.builder.config('spark.driver.extraClassPath', JARDRV) \
            .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.4")\
            .config( "spark.driver.memory", RAM_LIMIT).getOrCreate()
        self.connections = {}

    def read_table(self):
        return self.spark.read.format("jdbc")\
            .option("url", self.DB_HOST)\
            .option("dbtable", self.DB_TABLE)\
            .option("user", self.DB_USER)\
            .option("password", self.DB_PASS)\
            .option("driver", "org.postgresql.Driver")\
            .load()

    def write_dataframe(self, dataframe, output_table, mode="overwrite"):
        dataframe.write.mode(mode).format("jdbc")\
            .option("url", self.DB_HOST)\
            .option("dbtable", output_table)\
            .option("user", self.DB_USER)\
            .option("password", self.DB_PASS)\
            .option("driver", "org.postgresql.Driver")\
            .save()

    def getConnection(self, key):
        ret = self.connections.get(key)
        if ret is None:
            self.addConnection(key, self(key))
        return self.connections.get(key)

    def addConnection(self, conkey, val):
        self.connections[conkey] = val


class SingletonSparkConn:

    instance = None

    def __init__(self):
        raise ValueError("")

    @classmethod
    def get(cls):
        if cls.instance is None:
            cls.instance = spark_Conn()
        return cls.instance