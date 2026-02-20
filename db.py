import os
import pandas as pd
import psycopg2

class DB:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host = self.host,
                port = self.port,
                database = self.database,
                user = self.user,
                password = self.password
            )
            print("Conectado!")
        except Exception as e:
            print(f"Erro ao conectar no banco de dados: {e}")
            self.connection = None
    
    def create_table(self, table_name, columns):
        if self.connection:
            try:
                cursor = self.connection.cursor()
                # Escapar nomes de colunas com aspas duplas
                columns_with_types = ", ".join([f'"{col}" TEXT' for col in columns])
                cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_with_types});')
                self.connection.commit()
                print(f"Tabela '{table_name}' criada com sucesso!")
                cursor.close()
            except Exception as e:
                print(f"Erro ao criar tabela: {e}")
                self.connection.rollback()
        else:
            print("Conexão não estabelecida.")

    def insert_data(self, table_name, df):
        import json
        if self.connection:
            try:
                cursor = self.connection.cursor()
                # Escapar nomes de colunas com aspas duplas
                columns = [f'"{col}"' for col in df.columns]
                columns_str = ", ".join(columns)
                for _, row in df.iterrows():
                    values = []
                    for value in row:
                        # Converter listas/dicts para string JSON
                        if isinstance(value, (list, dict)):
                            values.append(json.dumps(value))
                        else:
                            values.append(str(value) if value is not None else None)
                    # Montar placeholders para parâmetros
                    placeholders = ', '.join(['%s'] * len(values))
                    insert_query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders});'
                    cursor.execute(insert_query, values)
                self.connection.commit()
                print(f"Dados inseridos na tabela '{table_name}' com sucesso!")
                cursor.close()
            except Exception as e:
                print(f"Erro ao inserir dados: {e}")
                self.connection.rollback()
        else:
            print("Conexão não estabelecida.")

    def execute_query(self, query):
        if self.connection:
            try:
                cursor = self.connection.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                return results
            except Exception as e:
                print(f"Erro ao executar consulta: {e}")
                return None
        else:
            print("Conexão não estabelecida.")
            return None
        
    def select_all(self, table_name, limit=10):
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
        return self.execute_query(query)

    def close(self):
        if self.connection:
            self.connection.close()
            print("Conexão fechada.")
        else:
            print("Conexão não estabelecida.")