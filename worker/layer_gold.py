import os
import pandas as pd
from sqlalchemy import create_engine
from log_utils import get_logger

class GoldLayerProcessor:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        # Pegando as variáveis de ambiente que o Airflow já conhece
        DB_USER = os.getenv('POSTGRES_USER', 'postgres')
        DB_PASS = os.getenv('POSTGRES_PASSWORD', 'postgres')
        DB_HOST = os.getenv('DB_HOST', 'postgres_database') # Nome do container no seu docker
        DB_PORT = os.getenv('DB_PORT', '5432')
        DB_NAME = os.getenv('POSTGRES_DB', 'postgres')
        
        DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.engine = create_engine(DB_URL)

    def run(self):
        self.logger.info("Iniciando processamento da camada Gold")
        query = "SELECT * FROM staging_notifications"
        
        # Ajuste preventivo: Abrindo conexão explícita para o read_sql
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)

        self.logger.info(f"Dados lidos da Silver. Shape: {df.shape}")

        # Agregação: Casos por Mês e Sexo (Perfeito para gráficos de linha no Grafana)
        # Garantindo que a data seja datetime
        df['DT_NOTIFIC'] = pd.to_datetime(df['DT_NOTIFIC'])
        
        gold_df = df.groupby([df['DT_NOTIFIC'].dt.to_period('M'), 'CS_SEXO']).size().reset_index(name='total_casos')
        gold_df['DT_NOTIFIC'] = gold_df['DT_NOTIFIC'].dt.to_timestamp()

        # Salvando a tabela final de Analytics
        with self.engine.begin() as conn:
            gold_df.to_sql('analytics_dengue_mensal', conn, if_exists='replace', index=False)
        
        self.logger.info("Tabela Gold 'analytics_dengue_mensal' criada com sucesso!")