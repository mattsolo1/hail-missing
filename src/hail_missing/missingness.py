from pathlib import Path
from typing import Any, Dict, Optional

import hail as hl
import pandas as pd
from hail.expr.expressions.typed_expressions import ArrayStructExpression
from hail.utils.struct import Struct
from loguru import logger


def struct_to_dict(struct: Any) -> Any:
    """
    Recursively converts a Hail Struct, or a nested structure containing Hail Structs,
    into a Python dictionary.

    Args:
        struct (Any): The Hail Struct, or nested structure with Hail Structs, to convert.

    Returns:
        Any: The resulting Python dictionary, or the original value if not a Hail Struct.
    """
    if isinstance(struct, Struct):
        return {k: struct_to_dict(getattr(struct, k)) for k in struct._fields}
    elif isinstance(struct, dict):
        return {k: struct_to_dict(v) for k, v in struct.items()}
    elif isinstance(struct, list):
        return [struct_to_dict(elem) for elem in struct]
    else:
        return struct


def count_missing_fields_with_keys(ht) -> Dict:
    def count_missing_and_keys(
        schema: hl.StructExpression,
        parent_missing=hl.bool(False),
        keys=hl.StructExpression,
    ):
        results = {}
        missing_keys = {}

        for field_name, expr in schema.items():
            current_field_missing = hl.is_missing(expr)

            results[field_name] = hl.agg.filter(
                ~parent_missing, hl.agg.count_where(current_field_missing)
            )
            missing_keys[field_name] = hl.agg.filter(
                ~parent_missing & current_field_missing, hl.agg.collect(keys)
            )

            if isinstance(expr, hl.StructExpression):
                nested_results, nested_missing_keys = count_missing_and_keys(
                    expr, current_field_missing, keys
                )
                for nested_field_name, nested_result in nested_results.items():
                    results[f"{field_name}.{nested_field_name}"] = nested_result
                for (
                    nested_field_name,
                    nested_keys_result,
                ) in nested_missing_keys.items():
                    missing_keys[
                        f"{field_name}.{nested_field_name}"
                    ] = nested_keys_result

            elif isinstance(expr, ArrayStructExpression):
                array_results = {}
                array_missing_keys = {}
                for array_struct_field_name, array_struct_expr in expr[0].items():
                    if isinstance(
                        array_struct_expr, (ArrayStructExpression, hl.StructExpression)
                    ):
                        (
                            nested_array_results,
                            nested_array_missing_keys,
                        ) = count_missing_and_keys(
                            array_struct_expr
                            if isinstance(array_struct_expr, hl.StructExpression)
                            else array_struct_expr[0],
                            current_field_missing,
                            keys,
                        )
                        for (
                            nested_field_name,
                            nested_result,
                        ) in nested_array_results.items():
                            array_results[
                                f"{array_struct_field_name}.{nested_field_name}"
                            ] = nested_result
                        for (
                            nested_field_name,
                            nested_keys_result,
                        ) in nested_array_missing_keys.items():
                            array_missing_keys[
                                f"{array_struct_field_name}.{nested_field_name}"
                            ] = nested_keys_result
                    array_field_missing = current_field_missing | hl.is_missing(
                        array_struct_expr
                    )
                    array_results[array_struct_field_name] = hl.agg.filter(
                        ~parent_missing,
                        hl.agg.sum(
                            expr.fold(
                                lambda accum, struct: accum
                                + hl.if_else(
                                    hl.is_missing(struct[array_struct_field_name]),
                                    1,
                                    0,
                                ),
                                0,
                            )
                        ),
                    )
                    array_missing_keys[array_struct_field_name] = hl.agg.filter(
                        ~parent_missing & array_field_missing, hl.agg.collect(keys)
                    )
                for array_field_name, array_result in array_results.items():
                    results[f"{field_name}.{array_field_name}"] = array_result
                for array_field_name, array_keys_result in array_missing_keys.items():
                    missing_keys[f"{field_name}.{array_field_name}"] = array_keys_result

        return results, missing_keys

    results, missing_keys = count_missing_and_keys(ht.row, keys=ht.key)

    return struct_to_dict(
        ht.aggregate(
            hl.struct(
                counts=hl.struct(**results), missing_keys=hl.struct(**missing_keys)
            )
        )
    )


class MissingnessReport:
    df: pd.DataFrame
    ht: Optional[hl.Table] = None

    def __init__(
        self,
        ht: Optional[hl.Table],
        cache_path: Optional[Path] = None,
    ):
        self.ht = ht
        self.df = self._load_or_compute_df(cache_path)

    def _load_or_compute_df(self, cache_path: Optional[Path]) -> pd.DataFrame:
        if cache_path and cache_path.exists():
            return pd.read_csv(str(cache_path))

        if not isinstance(self.ht, hl.Table):
            raise Exception(
                "Could not find cached missingess and hail table is not available"
            )

        missing = count_missing_fields_with_keys(self.ht)

        df = pd.DataFrame(
            {
                "field": list(missing["counts"].keys()),
                "counts": list(missing["counts"].values()),
                "missing_keys": list(missing["missing_keys"].values()),
            }
        )
        total_rows = self.ht.count()

        df["missing_percent"] = (df["counts"] / total_rows) * 100

        if cache_path:
            logger.info(f"Writing cached file to {cache_path}")
            if isinstance(cache_path, Path):
                cache_path.parent.mkdir(exist_ok=True, parents=True)
            df.to_csv(str(cache_path), index=False)

        return df

    @classmethod
    def create(
        cls,
        ht: Optional[hl.Table] = None,
        cache_path: Optional[Path] = None,
    ):
        """
        Analyzes a Hail table for missingness in each field and returns a report with
        counts of missing values for each field, the keys of the missing entries, and the
        percentage of missing data for each field.
        Optionally, the report can be cached to or read from a specified path.
        """
        return cls(ht, cache_path)

    def counts(self) -> Dict[str, int]:
        return self.df.set_index("field")["counts"].to_dict()
