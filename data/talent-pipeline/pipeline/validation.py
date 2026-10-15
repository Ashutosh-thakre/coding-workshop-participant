"""Generic data-quality engine used by the Silver layer.

  * ``model_to_spark_schema`` - derive a Spark schema from a Pydantic contract.
  * ``validate_rows``         - apply a contract to every row (optionally parsing JSON
                                first); each row comes back with ``_dq_errors`` set or null.
  * ``deduplicate``           - keep one row per primary key.
  * ``check_foreign_keys``    - split rows into those whose parent exists and orphans.

Row validation runs Pydantic in Python workers via ``mapInPandas``. That keeps the
contract in one readable place; the trade-off is Python overhead per row, which is
negligible at this data volume (tens of thousands of rows) but would call for native
Spark expressions at hundreds of millions.
"""

from __future__ import annotations

import json
import types
from collections.abc import Iterator
from datetime import date, datetime
from typing import Annotated, Any, Literal, Union, get_args, get_origin

import pandas as pd
from pydantic import BaseModel, ValidationError
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

DQ_ERRORS = "_dq_errors"  # semicolon-separated validation messages, null when valid
DQ_RAW = "_dq_raw"        # the original record (JSON text) for rejected rows
DQ_STAGE = "_dq_stage"    # which check rejected the row: schema | referential

_SPARK_TYPES: dict[type, T.DataType] = {
    bool: T.BooleanType(),
    int: T.LongType(),
    float: T.DoubleType(),
    str: T.StringType(),
    datetime: T.TimestampType(),
    date: T.DateType(),
}


def _spark_type(annotation: Any) -> T.DataType:
    """Map a (possibly Annotated / Optional / Literal) Python type to a Spark type."""
    origin = get_origin(annotation)
    if origin is Annotated:
        return _spark_type(get_args(annotation)[0])
    if origin in (Union, types.UnionType):
        non_null = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(non_null) != 1:
            raise TypeError(f"Unsupported union type: {annotation}")
        return _spark_type(non_null[0])
    if origin is Literal:
        return T.StringType()
    if annotation in _SPARK_TYPES:
        return _SPARK_TYPES[annotation]
    raise TypeError(f"No Spark type mapping for {annotation}")


def model_to_spark_schema(model: type[BaseModel]) -> T.StructType:
    """Spark schema with one nullable column per contract field, in declaration order."""
    return T.StructType(
        [T.StructField(name, _spark_type(info.annotation), nullable=True) for name, info in model.model_fields.items()]
    )


def format_errors(exc: ValidationError) -> str:
    """Compact, human-readable list of validation errors: 'field: message; ...'."""
    return "; ".join(
        f"{'.'.join(str(part) for part in err['loc']) or 'row'}: {err['msg']}" for err in exc.errors()
    )


def validate_record(
    record: dict[str, Any] | str | None,
    model: type[BaseModel],
    renames: dict[str, str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate one record (a dict, or a JSON string to parse first).

    Returns (clean_values, None) when valid, or (None, error_message) when not.
    Renames (e.g. {'notes': 'text'}) are applied before validation to absorb
    known schema drift, but never overwrite a field that is already present.
    """
    if not isinstance(record, dict):
        try:
            record = json.loads(record)  # type: ignore[arg-type]
        except (TypeError, json.JSONDecodeError) as exc:
            return None, f"malformed JSON: {getattr(exc, 'msg', exc)}"
        if not isinstance(record, dict):
            return None, "malformed JSON: not an object"

    record = dict(record)
    for source, target in (renames or {}).items():
        if source in record and target not in record:
            record[target] = record.pop(source)

    try:
        return model.model_validate(record).model_dump(), None
    except ValidationError as exc:
        return None, format_errors(exc)


def validate_rows(
    df: DataFrame,
    model: type[BaseModel],
    passthrough: list[str],
    json_column: str | None = None,
    renames: dict[str, str] | None = None,
) -> DataFrame:
    """Apply a contract to every row.

    Args:
        df: Input rows.
        model: Pydantic contract.
        passthrough: Columns copied unchanged to the output (e.g. audit columns).
        json_column: If set, each row's record is parsed from this JSON text column;
            otherwise the contract's fields are read from columns of the same name.
        renames: Field renames applied before validation (schema drift).

    Returns:
        Contract fields (typed; null for invalid rows) + passthrough columns +
        ``_dq_errors`` (null when valid) + ``_dq_raw`` (original record when invalid).
    """
    fields = list(model.model_fields)
    input_schema = {f.name: f.dataType for f in df.schema.fields}
    output_schema = T.StructType(
        model_to_spark_schema(model).fields
        + [T.StructField(c, input_schema[c], True) for c in passthrough]
        + [T.StructField(DQ_ERRORS, T.StringType(), True), T.StructField(DQ_RAW, T.StringType(), True)]
    )
    columns = output_schema.fieldNames()

    def _validate_batches(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for batch in batches:
            rows = batch.astype(object).where(batch.notna(), None).to_dict("records")
            out = []
            for row in rows:
                record = row[json_column] if json_column else {f: row.get(f) for f in fields}
                values, error = validate_record(record, model, renames)
                result = dict(values) if values else dict.fromkeys(fields)
                result.update({c: row[c] for c in passthrough})
                result[DQ_ERRORS] = error
                result[DQ_RAW] = None if error is None else (
                    record if isinstance(record, str) else json.dumps(record, default=str)
                )
                out.append(result)
            # dtype=object keeps None as None (pandas would turn int+None columns into float+NaN)
            yield pd.DataFrame(out, columns=columns, dtype=object)

    return df.mapInPandas(_validate_batches, schema=output_schema)


def deduplicate(df: DataFrame, keys: list[str], order_by: list[str]) -> DataFrame:
    """Keep one row per key: the first by ``order_by`` (deterministic across reruns)."""
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).asc_nulls_last() for c in order_by])
    return df.withColumn("_row_number", F.row_number().over(window)).where("_row_number = 1").drop("_row_number")


def count_conflicting_duplicates(df: DataFrame, keys: list[str], value_columns: list[str]) -> int:
    """Number of keys that appear with *different* values (not just repeated identical rows)."""
    return (
        df.groupBy(*keys)
        .agg(F.countDistinct(F.struct(*value_columns)).alias("_versions"))
        .where("_versions > 1")
        .count()
    )


def check_foreign_keys(
    df: DataFrame,
    foreign_keys: dict[str, tuple[str, str]],
    parents: dict[str, DataFrame],
) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, orphans) for each foreign key in turn.

    Orphans get ``_dq_errors`` describing the missing parent.
    """
    orphans: list[DataFrame] = []
    for column, (parent_table, parent_column) in foreign_keys.items():
        ref = parents[parent_table].select(F.col(parent_column).alias("_ref_key")).distinct()
        condition = df[column] == ref["_ref_key"]
        orphans.append(
            df.join(ref, condition, "left_anti").withColumn(
                DQ_ERRORS, F.lit(f"{column}: no matching {parent_table}.{parent_column}")
            )
        )
        df = df.join(ref, condition, "left_semi")
    if not orphans:
        return df, df.limit(0).withColumn(DQ_ERRORS, F.lit(None).cast("string"))
    rejected = orphans[0]
    for extra in orphans[1:]:
        rejected = rejected.unionByName(extra)
    return df, rejected
