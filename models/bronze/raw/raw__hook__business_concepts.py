import os
import pandas as pd
import typing as t
import yaml

from datetime import datetime
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

@model(
    name='bronze.raw.raw__hook__business_concepts',
    description='Extract & load model for the business concepts in hook yaml.',
    kind=dict(
        name=ModelKindName.FULL,
    ),
    columns={
        "name": "text",
        "definition": "text",
        "type": "Core concept" "text",
        "examples": "text",
        "business_rules": "text",
        "taxonomy": "text",
        "synonyms": "text",
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

    # Extract the 'business_concepts' data
    business_concepts = data.get('business_concepts', [])

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(business_concepts)
    
    df["_sqlmesh__record_source"] = relative_path
    df["_sqlmesh_extracted_at"] = execution_time.replace(tzinfo=None)

    yield df
