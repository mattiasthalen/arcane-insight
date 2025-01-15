import os
import pandas as pd
import typing as t
import yaml

from datetime import datetime
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

@model(
    name='bronze.raw.raw__hook__common_business_terms',
    description='Extract & load model for the common business terms in hook yaml.',
    kind=dict(
        name=ModelKindName.FULL,
    ),
    columns={
        "business_term": "int",
        "description": "text",
        "owner": "text",
        "_sqlmesh__record_source": "text",
        "_sqlmesh_extracted_at": "timestamp"
    }
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Generator[pd.DataFrame, None, None]:

# Load the YAML file
    relative_path = "../../../data/hook.yml"
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of the current script
    absolute_path = os.path.abspath(os.path.join(script_dir, relative_path))
    
    with open(absolute_path, 'r') as file:
        data = yaml.safe_load(file)

    # Extract the 'common_business_terms' data
    common_business_terms = data.get('common_business_terms', [])

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(common_business_terms)
    
    df["_sqlmesh__record_source"] = relative_path
    df["_sqlmesh_extracted_at"] = execution_time.replace(tzinfo=None)

    yield df
