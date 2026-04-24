from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import sys
import os

# --- FUNÇÕES DE PONTE (BRIDGE) ---

def disparar_ingestao_raw():
    # Caminho absoluto dentro do container Docker
    sys.path.append('/opt/airflow')
    os.environ["RAW_INPUT_PATH"] = "/opt/airflow/worker"
    os.environ["RAW_OUTPUT_PATH"] = "/opt/airflow/worker"
    
    from worker.layer_raw import RawLayerProcessor
    processor = RawLayerProcessor()
    processor.run()
    print("Sucesso: Camada Raw finalizada!")

def disparar_processamento_silver():
    sys.path.append('/opt/airflow')
    os.environ["RAW_OUTPUT_PATH"] = "/opt/airflow/worker"
    
    from worker.layer_silver import SilverLayerProcessor
    processor = SilverLayerProcessor()
    tabela = processor.run()
    print(f"Sucesso: Dados persistidos na tabela {tabela}!")

def disparar_processamento_gold():
    sys.path.append('/opt/airflow')
    os.environ["RAW_OUTPUT_PATH"] = "/opt/airflow/worker"
    
    from worker.layer_gold import GoldLayerProcessor
    processor = GoldLayerProcessor()
    processor.run()
    print("Sucesso: Camada Gold finalizada e pronta para o Grafana!")

# --- CONFIGURAÇÕES DA DAG ---

default_args = {
    'owner': 'Alan',
    'depends_on_past': False,
    'retries': 0, # Reduzi para 0 para vermos o erro real de primeira se falhar
}

with DAG(
    'projeto_dengue_final_v2', # Novo ID para resetar o banco do Airflow
    default_args=default_args,
    description='Pipeline End-to-End: FEDUSP',
      schedule_interval='@daily',  
    start_date=days_ago(0),
    catchup=False,
    tags=['usp', 'data_engineering'],
) as dag:

    task_raw = PythonOperator(
        task_id='executar_processor_raw',
        python_callable=disparar_ingestao_raw,
    )

    task_silver = PythonOperator(
        task_id='executar_processor_silver',
        python_callable=disparar_processamento_silver,
    )

    task_gold = PythonOperator(
        task_id='executar_processor_gold',
        python_callable=disparar_processamento_gold,
    )

    task_raw >> task_silver >> task_gold