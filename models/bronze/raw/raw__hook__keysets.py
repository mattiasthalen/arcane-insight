import os
import pandas as pd
import typing as t
import yaml

from datetime import datetime
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

@model(
    name='bronze.raw.raw__hook__keysets',
    description='Extract & load model for the keysets in hook yaml.',
    kind=dict(
        name=ModelKindName.FULL,
    ),
    columns={
        "id": "int",
        "business_term": "text",
        "source": "text",
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
    """
    Reads a YAML file, extracts the data under the 'keysets' key, and converts it into a table format.

    :param yaml_file: Path to the YAML file.
    :return: Pandas DataFrame containing the extracted data.
    """
    # Load the YAML file
    relative_path = "../../../data/hook.yml"
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of the current script
    absolute_path = os.path.abspath(os.path.join(script_dir, relative_path))
    
    with open(absolute_path, 'r') as file:
        data = yaml.safe_load(file)

    # Extract the 'keysets' data
    keysets = data.get('keysets', [])

    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(keysets)
    
    df["_sqlmesh__record_source"] = relative_path
    df["_sqlmesh_extracted_at"] = execution_time.replace(tzinfo=None)

    yield df
