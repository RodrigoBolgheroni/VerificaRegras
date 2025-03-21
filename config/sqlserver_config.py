from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

# Carregar variáveis do arquivo .env
load_dotenv()

def conecta_sqlserver():
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')

    # Estabelecendo a conexão usando SQLAlchemy
    connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_string)
    return engine.connect()
