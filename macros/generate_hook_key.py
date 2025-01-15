import typing as t
from sqlmesh import macro
from sqlglot import exp

@macro()
def generate_hook_key(evaluator, keyset_id: exp.Literal, *keys: exp.Expression) -> exp.Func:
    """Generates a hook key for the given keyset id and field.
    """
    string_fields: t.List[exp.Expression] = []
    for i, key in enumerate(keys):
        if i > 0:
            string_fields.append(exp.Literal.string("|"))
            
        string_fields.append(
            exp.cast(
                key,
                exp.DataType.build("text")
            )
        )
        
    hook_key = exp.cast(
        exp.func(
            "CONCAT",
            *string_fields
        ),
        exp.DataType.build("binary")
    )

    return hook_key