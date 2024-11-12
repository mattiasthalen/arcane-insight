import getpass
import subprocess
from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
    DuckDBConnectionConfig,
    NameInferenceConfig,
    CategorizerConfig,
    PlanConfig,
    AutoCategorizationMode
)

def get_current_branch():
    try:
        branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('utf-8')
        return branch_name
    except Exception as e:
        print(f"Error getting current branch: {e}")
        return None

current_user = getpass.getuser()
branch = get_current_branch() or 'dev'
default_environment = f"{current_user}__{branch}".replace('-', '_')

print(f"Environment is set to: {default_environment}.")

config = Config(
    project="arcane-insight",
    default_target_environment=default_environment,
    gateways={
            "local": GatewayConfig(
                connection=DuckDBConnectionConfig(
                    catalogs={
                        "bronze": "./data/bronze.duckdb",
                        "silver": "./data/silver.duckdb",
                        "gold": "./data/gold.duckdb",
                        "platinum": "./data/platinum.duckdb"
                    }
                )
            )
        },
    default_gateway="local",
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb",
        start="2024-11-01"
    ),
    model_naming=NameInferenceConfig(
        infer_names=True
    ),
    plan=PlanConfig(
        auto_categorize_changes=CategorizerConfig(
            external=AutoCategorizationMode.FULL,
            python=AutoCategorizationMode.FULL,
            sql=AutoCategorizationMode.FULL,
            seed=AutoCategorizationMode.FULL
        )
    )
)