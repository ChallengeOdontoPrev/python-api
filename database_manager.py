import os
import jaydebeapi
import cx_Oracle  # Se ainda quiser manter suporte ao Oracle
from database_config import DatabaseConfig

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.db_url = DatabaseConfig.get_database_url()

        if "h2" in self.db_url.lower():
            # Caminho absoluto para o driver .jar do H2
            h2_jar_path = os.path.abspath("h2-2.3.232.jar")

            # Conectando ao H2 via JDBC (TCP)
            self.connection = jaydebeapi.connect(
                "org.h2.Driver",
                self.db_url,            # exemplo: "jdbc:h2:tcp://localhost:1521/test"
                ["sa", ""],             # usuário e senha
                h2_jar_path
            )

        elif "oracle" in self.db_url.lower():
            self.connection = cx_Oracle.connect(self.db_url)

        else:
            raise ValueError("Banco de dados desconhecido!")

        self.cursor = self.connection.cursor()

    def execute_query(self, query, params=None):
        if params is None:
            params = ()
        self.cursor.execute(query, params)
        self.connection.commit()
        
    def fetch_all(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.connection.close()
