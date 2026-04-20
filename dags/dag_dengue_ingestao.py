from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# --- FUNÇÕES DE PONTE (BRIDGE) ---

def disparar_ingestao_raw():
    # Garante que o Python ache as pastas do projeto no Docker
    sys.path.append('/opt/airflow')
    sys.path.append('/opt/airflow/worker')
    
    os.environ["RAW_INPUT_PATH"] = "/opt/airflow/worker"
    os.environ["RAW_OUTPUT_PATH"] = "/opt/airflow/worker"
    
    from worker.layer_raw import RawLayerProcessor
    processor = RawLayerProcessor()
    processor.run()
    print("Sucesso: Camada Raw finalizada!")

def disparar_processamento_silver():
    sys.path.append('/opt/airflow')
    sys.path.append('/opt/airflow/worker')
    os.environ["RAW_OUTPUT_PATH"] = "/opt/airflow/worker"
    
    from worker.layer_silver import SilverLayerProcessor
    processor = SilverLayerProcessor()
    tabela = processor.run()
    print(f"Sucesso: Dados persistidos na tabela {tabela}!")

def disparar_processamento_gold():
    # Novo: Faz o mesmo caminho para a Gold
    sys.path.append('/opt/airflow')
    sys.path.append('/opt/airflow/worker')
    
    from worker.layer_gold import GoldLayerProcessor
    processor = GoldLayerProcessor()
    processor.run()
    print("Sucesso: Camada Gold finalizada e pronta para o Grafana!")

# --- CONFIGURAÇÕES DA DAG ---

default_args = {
    'owner': 'Alan',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'monitoramento_dengue_ingestao',
    default_args=default_args,
    description='Pipeline End-to-End: Raw -> Silver -> Gold (FEDUSP)',
    schedule_interval='@daily', 
    catchup=False,
    tags=['usp', 'data_engineering', 'dengue'],
) as dag:

    # Tarefa 1: Ingestão
    task_raw = PythonOperator(
        task_id='executar_processor_raw',
        python_callable=disparar_ingestao_raw,
        execution_timeout=timedelta(minutes=30),
    )

    # Tarefa 2: Limpeza e Banco
    task_silver = PythonOperator(
        task_id='executar_processor_silver',
        python_callable=disparar_processamento_silver,
    )

    # Tarefa 3: Analytics
    task_gold = PythonOperator(
        task_id='executar_processor_gold',
        python_callable=disparar_processamento_gold,
    )

    # O FLUXO COMPLETO:
    task_raw >> task_silver >> task_gold