from pathlib import Path
from datetime import datetime
import pandas as pd
import time
import os
from log_utils import get_logger
from sqlalchemy import create_engine

class SilverLayerProcessor:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

        # Configurações de Banco de Dados
        DB_NAME = os.getenv("POSTGRES_DB", "postgres")
        DB_USER = os.getenv("POSTGRES_USER", "postgres")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
        DB_HOST = os.getenv("DB_HOST", "db")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        self.engine = create_engine(DB_URL)

    def run(self):
        self.logger.info("Iniciando Silver - MODO OTIMIZADO (Amostra 20% / Colunas Essenciais)")
        
        base_path = Path(os.getenv("RAW_OUTPUT_PATH", "/opt/airflow/worker"))
        raw_path = base_path / "data" / "raw"

        try:
            latest_partition = self._get_latest_partition(raw_path)
            files = list(latest_partition.glob("*.csv"))
            if not files:
                raise FileNotFoundError(f"Nenhum CSV encontrado em {latest_partition}")
            
            caminho_csv = files[0]
            self.logger.info(f"Arquivo selecionado: {caminho_csv}")

        except Exception as e:
            self.logger.error(f"Erro ao localizar arquivo raw: {e}")
            raise

        # 1. SELEÇÃO DE COLUNAS: Apenas o que importa para análise e para a Gold
        colunas_selecionadas = [
            "DT_NOTIFIC", "ID_AGRAVO", "ID_MUNICIP", "SG_UF", 
            "ID_REGIONA", "ID_UNIDADE", "DT_SIN_PRI", "CS_SEXO", 
            "NU_IDADE_N", "CS_RACA", "CLASSI_FIN", "EVOLUCAO", "ID_MN_RESI"
        ]

        primeiro_chunk = True
        
        # 2. CARGA REDUZIDA: 300 mil linhas (~20%) e chunk de 100k para ser rápido
        leitura_csv = pd.read_csv(
            caminho_csv, 
            chunksize=100000, 
            low_memory=False,
            usecols=colunas_selecionadas, 
            nrows=300000 
        )

        for chunk in leitura_csv:
            self.logger.info(f"Processando lote de {len(chunk)} linhas...")
            
            df = self._clean(chunk)
            df = self._transform(df)

            modo = "replace" if primeiro_chunk else "append"
            
            try:
                with self.engine.connect() as connection:
                    df.to_sql(
                        "staging_notifications", 
                        connection, 
                        if_exists=modo, 
                        index=False,
                        method='multi',
                        chunksize=10000
                    )
                
                self.logger.info(f"✓ Lote persistido com sucesso no modo: {modo}")
                primeiro_chunk = False
                
            except Exception as e:
                self.logger.error(f"Erro na persistência do lote: {e}")
                raise

        self.logger.info("🏆 Silver finalizada com sucesso (Amostra de 300k)!")
        return "staging_notifications"

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates()
        df = df.replace({"": None})
        return df

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # 3. CONVERSÃO FORÇADA: Garante que datas sejam datas e IDs sejam strings
        # Evita erro de 'double precision' no Postgres
        
        date_cols = ["DT_NOTIFIC", "DT_SIN_PRI", "DT_INVEST", "DT_OBITO", "DT_ENCERRA", "DT_DIGITA", "DT_PCR"]
        
        for col in df.columns:
            if col in date_cols or "DT_" in col:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                
        # Garante que códigos municipais não virem números quebrados (.0)
        cols_para_texto = ["ID_MUNICIP", "ID_REGIONA", "ID_UNIDADE", "ID_MN_RESI"]
        for col in cols_para_texto:
            if col in df.columns:
                df[col] = df[col].astype(str).replace(['nan', 'None', '<NA>'], None)

        return df

    def _get_latest_partition(self, raw_base: Path) -> Path:
        partitions = sorted(raw_base.glob("*/*/*"))
        if not partitions:
            raise FileNotFoundError(f"Nenhuma partição encontrada em {raw_base}")
        return partitions[-1] 