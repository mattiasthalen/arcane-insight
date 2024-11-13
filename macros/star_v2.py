import typing as t

from sqlmesh import macro
from sqlglot import exp

from sqlmesh.core.macros import MacroEvaluator
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlmesh.utils.errors import MacroEvalError, SQLMeshError

@macro()
def star_v2(
    evaluator: MacroEvaluator,
    relation: exp.Table,
    alias: exp.Column = t.cast(exp.Column, exp.column("")),
    exclude: t.Union[exp.Array, exp.Tuple, exp.Column] = exp.Tuple(expressions=[]),
    prefix: exp.Literal = exp.Literal.string(""),
    suffix: exp.Literal = exp.Literal.string(""),
    quote_identifiers: exp.Boolean = exp.true(),
    except_: t.Union[exp.Array, exp.Tuple] = exp.Tuple(expressions=[]),
    select_only: exp.Boolean = exp.false(),
) -> t.List[t.Union[exp.Alias, exp.Column]]:
    """Returns a list of projections for the given relation.

    Args:
        evaluator: MacroEvaluator that invoked the macro
        relation: The relation to select star from
        alias: The alias of the relation
        exclude: Columns to exclude
        prefix: A prefix to use for all selections
        suffix: A suffix to use for all selections
        quote_identifiers: Whether or not quote the resulting aliases, defaults to true
        except_: Alias for exclude (TODO: deprecate this, update docs)
        select_only: Wether or not to only return the projections, without casting and aliasing, defaults to false

    Returns:
        An array of columns.

    Example:
        >>> from sqlglot import parse_one, exp
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "SELECT @STAR(foo, bar, exclude := [c], prefix := 'baz_') FROM foo AS bar"
        >>> MacroEvaluator(schema=MappingSchema({"foo": {"a": exp.DataType.build("string"), "b": exp.DataType.build("string"), "c": exp.DataType.build("string"), "d": exp.DataType.build("int")}})).transform(parse_one(sql)).sql()
        'SELECT CAST("bar"."a" AS TEXT) AS "baz_a", CAST("bar"."b" AS TEXT) AS "baz_b", CAST("bar"."d" AS INT) AS "baz_d" FROM foo AS bar'
    """
    
    if alias and not isinstance(alias, (exp.Identifier, exp.Column)):
        raise SQLMeshError(f"Invalid alias '{alias}'. Expected an identifier.")
    if exclude and not isinstance(exclude, (exp.Array, exp.Tuple, exp.Column)):
        raise SQLMeshError(f"Invalid exclude '{exclude}'. Expected an array or a column.")
    if except_ != exp.tuple_():
        logger.warning(
            "The 'except_' argument in @STAR will soon be deprecated. Use 'exclude' instead."
        )
        if not isinstance(exclude, (exp.Array, exp.Tuple, exp.Column)):
            raise SQLMeshError(f"Invalid exclude_ '{exclude}'. Expected an array or a column.")
    if prefix and not isinstance(prefix, exp.Literal):
        raise SQLMeshError(f"Invalid prefix '{prefix}'. Expected a literal.")
    if suffix and not isinstance(suffix, exp.Literal):
        raise SQLMeshError(f"Invalid suffix '{suffix}'. Expected a literal.")
    if not isinstance(quote_identifiers, exp.Boolean):
        raise SQLMeshError(f"Invalid quote_identifiers '{quote_identifiers}'. Expected a boolean.")
    if not isinstance(select_only, exp.Boolean):
        raise SQLMeshError(f"Invalid select_only '{select_only}'. Expected a boolean.")
    
    if isinstance(exclude, exp.Column):
        exclude =  exp.Tuple(expressions=[exclude])
    
    excluded_names = {
        normalize_identifiers(excluded, dialect=evaluator.dialect).name
        for excluded in exclude.expressions
    }
    quoted = quote_identifiers.this
    table_identifier = alias.name or relation.name

    columns_to_types = {
        k: v for k, v in evaluator.columns_to_types(relation).items() if k not in excluded_names
    }
    
    if select_only:
        return [
            exp.column(column, table=table_identifier, quoted=quoted)
            for column, type_ in columns_to_types.items()
        ]
        
    if columns_to_types_all_known(columns_to_types):
        return [
            exp.cast(
                exp.column(column, table=table_identifier, quoted=quoted),
                dtype,
                dialect=evaluator.dialect,
            ).as_(f"{prefix.this}{column}{suffix.this}", quoted=quoted)
            for column, dtype in columns_to_types.items()
        ]
    return [
        exp.column(column, table=table_identifier, quoted=quoted).as_(
            f"{prefix.this}{column}{suffix.this}", quoted=quoted
        )
        for column, type_ in columns_to_types.items()
    ]