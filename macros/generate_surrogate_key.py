import typing as t

from sqlmesh import macro
from sqlglot import exp

@macro()
def generate_surrogate_key__sha_256(evaluator, *fields: exp.Expression) -> exp.Func:
    """Generates a surrogate key for the given fields.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @GENERATE_SURROGATE_KEY__sha_256(a, b, c) FROM foo"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        "SELECT SHA256(CONCAT(COALESCE(CAST(a AS TEXT), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(b AS TEXT), '_sqlmesh_surrogate_key_null_'), '|', COALESCE(CAST(c AS TEXT), '_sqlmesh_surrogate_key_null_'))) FROM foo"
    """
    string_fields: t.List[exp.Expression] = []
    for i, field in enumerate(fields):
        if i > 0:
            string_fields.append(exp.Literal.string("|"))
        string_fields.append(
            exp.func(
                "COALESCE",
                exp.cast(field, exp.DataType.build("text")),
                exp.Literal.string("_sqlmesh_surrogate_key_null_"),
            )
        )
    return exp.cast(exp.func("SHA256", exp.func("CONCAT", *string_fields)), exp.DataType.build("binary"))