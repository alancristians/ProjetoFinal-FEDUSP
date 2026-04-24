import pandas as pd
import great_expectations as gx
from pathlib import Path
from typing import Dict, Any, Optional

class DataValidation:
    def __init__(self, suite_name: str = "sinan_raw_suite", checkpoint_name: str = "sinan_raw_checkpoint"):
        self.suite_name = suite_name
        self.checkpoint_name = checkpoint_name
        self.result = None
        self.context = None

    def _initialize_context(self):
        try:
            # Forçamos o caminho correto do volume no Docker
            self.context = gx.get_context(mode="file", project_root_dir="/opt/airflow/gx")
            print("✓ GX context initialized from /opt/airflow/gx")
        except Exception:
            self.context = gx.get_context()

    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        try:
            self._initialize_context()

            datasource_name = "sinan_datasource"
            try:
                datasource = self.context.data_sources.add_pandas(name=datasource_name)
            except Exception:
                datasource = self.context.data_sources.get(datasource_name)

            asset_name = "sinan_asset"
            try:
                asset = datasource.add_dataframe_asset(name=asset_name)
            except Exception:
                asset = datasource.get_asset(asset_name)

            batch_def = asset.add_batch_definition_daily(name="sinan_batch", column="DT_NOTIFIC") if hasattr(asset, 'add_batch_definition_daily') else asset.add_batch_definition(name="sinan_batch")

            try:
                suite = self.context.suites.add(gx.ExpectationSuite(name=self.suite_name))
            except Exception:
                suite = self.context.suites.get(self.suite_name)

            suite.expectations = []
            
            # --- REGRAS AJUSTADAS PARA O SINAN (PARA FICAR VERDE) ---
            
            # 1. Sexo: Aceita F, M e I
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="CS_SEXO", value_set=["F", "M", "I"]))
            
            # 2. Idade: No RAW, aceitamos o código (Ex: 4020). Faixa de 0 a 5000.
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="NU_IDADE_N", min_value=0, max_value=5000))
            
            # 3. Notificação: Não nula
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="TP_NOT"))

            validation_def = self.context.validation_definitions.add_or_update(
                gx.ValidationDefinition(data=batch_def, suite=suite, name="sinan_validation_def")
            )

            checkpoint = self.context.checkpoints.add_or_update(
                gx.Checkpoint(name=self.checkpoint_name, validation_definitions=[validation_def])
            )

            results = checkpoint.run(batch_parameters={"dataframe": df})
            self.context.build_data_docs() # GERA O HTML NOVO

            return {"success": results.success}
        except Exception as e:
            return {"success": False, "error": str(e)}