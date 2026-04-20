import pandas as pd
import great_expectations as gx
from pathlib import Path
from typing import Dict, Any, Optional


class DataValidation:
    """
    Centralized data validation using Great Expectations.
    Handles dataset validation against defined expectations for SINAN Dengue data.
    """

    def __init__(self, suite_name: str = "sinan_raw_suite", checkpoint_name: str = "sinan_raw_checkpoint"):
        """
        Initialize the DataValidation handler.

        Args:
            suite_name: Name of the expectation suite
            checkpoint_name: Name of the checkpoint
        """
        self.suite_name = suite_name
        self.checkpoint_name = checkpoint_name
        self.result = None
        self.context = None

    def _initialize_context(self):
        """Initialize Great Expectations context from project directory."""
        try:
            self.context = gx.get_context(mode="file", project_root_dir="/app/gx")
            print("✓ GX context initialized from /app/gx")
        except Exception as e:
            print(f"Warning initializing context from /app/gx: {e}")
            try:
                self.context = gx.get_context()
                print("✓ GX context initialized (ephemeral)")
            except Exception as e2:
                print(f"Error initializing ephemeral context: {e2}")
                raise

    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        try:
            print(f"Starting GX validation with {len(df)} rows")
            self._initialize_context()

            # 1. Datasource
            datasource_name = "sinan_pandas_datasource"
            try:
                datasource = self.context.data_sources.add_pandas(name=datasource_name)
            except Exception:
                datasource = self.context.data_sources.get(datasource_name)

            # 2. Asset
            asset_name = "sinan_data_asset"
            try:
                asset = datasource.add_dataframe_asset(name=asset_name)
            except Exception:
                asset = datasource.get_asset(asset_name)

            # 3. Batch Definition
            batch_definition_name = "sinan_batch_def"
            try:
                batch_def = asset.add_batch_definition(name=batch_definition_name)
            except Exception:
                batch_def = asset.get_batch_definition(batch_definition_name)

            # 4. Expectation Suite
            try:
                expectation_suite = self.context.suites.add(gx.ExpectationSuite(name=self.suite_name))
            except Exception:
                expectation_suite = self.context.suites.get(self.suite_name)

            # Adicionando as expectativas (limpando as antigas para não duplicar)
            expectation_suite.expectations = []
            
            expectation_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="TP_NOT"))
            expectation_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="NU_IDADE_N", min_value=0, max_value=120))
            expectation_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="CS_SEXO", value_set=["F", "M", ""]))
            
            # 5. Validation Definition (O PONTO CRÍTICO)
            # Usamos add_or_update para evitar o erro de 'Freshness'
            validation_def = gx.ValidationDefinition(
                data=batch_def,
                suite=expectation_suite,
                name="sinan_validations"
            )
            validation_def = self.context.validation_definitions.add_or_update(validation_def)
            print("✓ Validation definition ready")

            # 6. Checkpoint
            checkpoint = gx.Checkpoint(
                name=self.checkpoint_name,
                validation_definitions=[validation_def],
                result_format={"result_format": "COMPLETE"}
            )
            checkpoint = self.context.checkpoints.add_or_update(checkpoint)
            print("✓ Checkpoint ready")

            # 7. Run
            results = checkpoint.run(batch_parameters={"dataframe": df})

            return {
                "success": results.success,
                "message": "Validation completed successfully",
                "results": results.to_json_dict() if hasattr(results, 'to_json_dict') else str(results)
            }

        except Exception as e:
            print(f"Error during validation: {str(e)}")
            return {"success": False, "error": str(e), "message": "Validation failed"}

    def get_result(self) -> Optional[Dict[str, Any]]:
        """
        Get the validation result.

        Returns:
            Validation result dictionary or None if not yet validated
        """
        return self.result

