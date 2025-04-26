import os

class DatabaseConfig:
    @staticmethod
    def get_database_url():
        """Retorna a URL de conexão dependendo do ambiente"""
        environment = os.getenv("ENV_BANCO_PY", "PRD")  # Padrão para HML (Homologação)
        
        if environment == "PRD":
            return os.getenv("ORACLE_DB_URL")  # URL do banco de produção (Oracle)
        else:
            return os.getenv("H2_DB_URL")  # URL do banco de testes (H2)