import psycopg2
from datetime import datetime
import time
import os
from dotenv import load_dotenv
# Forçar o carregamento do arquivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
print("DB Config:")
print(f"DB_NAME: {os.getenv('DB_NAME')}")
print(f"DB_USER: {os.getenv('DB_USER')}")
print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_PORT: {os.getenv('DB_PORT')}")
