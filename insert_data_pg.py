import json
from utils.postgress_conn import PostgresConnection
from utils.read_conf import (
    PostgressTABLE
)
def InsertDataPG():
    pc = PostgresConnection()
    pc.connect()
    pc.delete_table(PostgressTABLE)
    pc.create_table(PostgressTABLE,"( name text, description text)")
    with open("./data.json") as f:
        data = json.load(f)
    pc.insert_json_data(PostgressTABLE, ['name', 'description'], data["doc_data"])
    pc.close()
    
if __name__ == "__main__":
    InsertDataPG()
