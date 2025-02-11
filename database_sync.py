import psycopg2
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# carrega as variáveis de ambiente do arquivo .env, se nao funcionar apenas colocar a informações diretamente no codigo ou usar o dotenv_path para forçar ele carregar o arquivo 
load_dotenv()

# configuração do banco de dados
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def get_db_connection():
    """Cria uma conexão com o banco de dados PostgreSQL usando as variáveis de ambiente."""
    return psycopg2.connect(
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"], 
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
    )

def create_tables(conn):
    """Cria as tabelas no banco de dados"""
    cursor = conn.cursor()
    
    # tabela sensores
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensores (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL,
            unidade TEXT NOT NULL,
            localizacao TEXT NOT NULL,
            ativo BOOLEAN NOT NULL
        )
    ''')
    
    # tabela leitura_sensores
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS leitura_sensores (
            id SERIAL PRIMARY KEY,
            sensor_id INTEGER NOT NULL,
            valor REAL NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            sincronizado BOOLEAN NOT NULL,
            FOREIGN KEY(sensor_id) REFERENCES sensores(id)
        )
    ''')
    
    # tabela sincronizacao
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sincronizacao (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            status BOOLEAN NOT NULL,
            mensagem TEXT NOT NULL
        )
    ''')
    
    conn.commit()
    print("Tables created successfully")

def insert_initial_data(conn):
    """uinsere dados iniciais de exemplo que estao no pdf"""
    cursor = conn.cursor()
    
    # verifica se os dados já existem
    cursor.execute("SELECT COUNT(*) FROM sensores")
    # method returns a single record or None if no more rows are available. apagar essa marcação antes de enviar
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
            INSERT INTO sensores (nome, tipo, unidade, localizacao, ativo)
            VALUES ('SensorTemp', 'umidade', '°C', 'Lugar1', TRUE)
        ''')
    
    cursor.execute("SELECT COUNT(*) FROM leitura_sensores")
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
            INSERT INTO leitura_sensores (sensor_id, valor, timestamp, sincronizado)
            VALUES (1, 23.5, '2025-01-28 10:00:00', FALSE)
        ''')
    
    cursor.execute("SELECT COUNT(*) FROM sincronizacao")
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
            INSERT INTO sincronizacao (timestamp, status, mensagem)
            VALUES ('2025-01-28 10:10:00', FALSE, 'Connection Failure')
        ''')
    
    conn.commit()
    print("Initial data inserted")

def insert_sensor_reading(conn, sensor_id, valor):
    """Insere uma nova leitura de sensor"""
    cursor = conn.cursor()
    timestamp = datetime.now()
    
    cursor.execute('''
        INSERT INTO leitura_sensores (sensor_id, valor, timestamp, sincronizado)
        VALUES (%s, %s, %s, %s)
    ''', (sensor_id, valor, timestamp, False))
    
    conn.commit()
    print(f"New reading inserted for sensor {sensor_id}")

def log_sync_status(status, message):
    """Registra o status de sincronização em um arquivo de log"""
    with open("sync_log.txt", "a") as log_file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(f"{timestamp} - Status: {'Success' if status else 'Error'} - Message: {message}\n")

def synchronize_with_gcp(conn):
    """Simula a sincronização com o GCP"""
    cursor = conn.cursor()
    
    # seleciona leituras não sincronizadas
    cursor.execute('''
        SELECT * FROM leitura_sensores WHERE sincronizado = FALSE
    ''')
    unsynced = cursor.fetchall()
    
    if not unsynced:
        print("No data to sync")
        return

    # simula envio para a nuvem
    print(f"Syncing {len(unsynced)} readings to GCP...")
    
    # atualiza status de sincronização
    cursor.execute('''
        UPDATE leitura_sensores SET sincronizado = TRUE WHERE sincronizado = FALSE
    ''')
    
    # registra log de sincronização 
    log_sync_status(True, "Data sent to GCP")
    
    conn.commit()
    print("Sync completada com sucesso")
    print("aperte ctrl + c para para o codigo no terminal")

if __name__ == "__main__":
    # configuração inicial
    try:
        connection = get_db_connection()
        
        if connection:
            create_tables(connection)
            insert_initial_data(connection)
            
            # loop de verificação a cada minuto
            try:
                while True:
                    insert_sensor_reading(connection, 1, 24.5)  
                    synchronize_with_gcp(connection)   
                    time.sleep(60) 
                   
            except KeyboardInterrupt:
                print("processo terminado pelo usuário.")
            finally:
                connection.close()
    except Exception as e:
        print(f"erro conectando na database: {e}")