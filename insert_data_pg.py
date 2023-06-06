import json
from utils.postgress_conn import PostgresConnection

def InsertDataPG():
    pc = PostgresConnection()
    pc.connect()
    pc.delete_table("test_table")
    pc.create_table("test_table","( name text, description text)")
    with open("./data.json") as f:
        data = json.load(f)
    pc.insert_json_data("test_table", ['name', 'description'], data["doc_data"])
    pc.close()
    
if __name__ == "__main__":
    InsertDataPG()
