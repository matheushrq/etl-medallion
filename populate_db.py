from db import DB
import pandas as pd
import db
import os

db_instance = DB(
    host = os.getenv("DB_HOST", "host"),
    port = os.getenv("DB_PORT", "porta"),
    database = os.getenv("DB_NAME", "sua database"),
    user = os.getenv("DB_USER", "seu usuario"),
    password = os.getenv("DB_PASSWORD", "sua senha")
)

db_instance.connect()

for file in os.listdir("02-silver-validated"):
    df = pd.read_parquet(f"02-silver-validated/{file}")
    
    db_instance.create_table(
        file.replace(".parquet", ""),
        df.columns.to_list()
    )

    db_instance.insert_data(
        file.replace(".parquet", ""),
        df
    )

db_instance.close()