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
    """
    Counts missing fields in a Hail Table and returns a dictionary with counts and keys of missing data.

    Analyzes a Hail Table to identify missing data within its fields, including nested structures
    and arrays of structures. Returns the counts of missing data and the corresponding keys where
    the data is missing.

    Args:
        ht: The Hail Table to be analyzed for missing fields.

    Returns:
        A dictionary containing the counts of missing fields and the keys corresponding to
        those fields. The keys are given in the form of a struct, which maps field names to
        their missing counts and missing keys.

    Example:
        result = count_missing_fields_with_keys(hl_table)
        print(result)
        # Output: {'counts': {'field1': 0, 'field2': 10}, 'missing_keys': {'field2': ['key1', 'key2']}}

    Raises:
        HailUserError: If the operation encounters an index out of bounds error due to an
        empty ArrayStructExpression. Ensure that such expressions are handled before calling
        this function.
    """

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
    """
    A class for generating a missingness report for a Hail Table.

    This class analyzes a Hail Table to determine the presence of missing
    data in its fields. It computes the count of missing values and their
    percentage for each field, as well as the keys associated with rows
    that contain missing values. Optionally, the report can be cached to
    a specified path as a CSV file.

    Attributes:
        df (pd.DataFrame): DataFrame containing the missingness report.
        ht (Optional[hl.Table]): The Hail Table being analyzed.

    Methods:
        __init__(self, ht: Optional[hl.Table], cache_path: Optional[Path]): Initializes the MissingnessReport instance.
        _load_or_compute_df(self, cache_path: Optional[Path]): Loads the missingness report from cache or computes it.
    """

    df: pd.DataFrame
    ht: Optional[hl.Table] = None

    def __init__(
        self,
        ht: Optional[hl.Table],
        cache_path: Optional[Path] = None,
    ):
        """
        Initializes a MissingnessReport instance.

        Args:
            ht (Optional[hl.Table]): A Hail Table to analyze for missingness. Can be None if a cache_path is provided.
            cache_path (Optional[Path]): A file path to a pre-computed missingness report CSV. If provided and the
                                         file exists, loads the report from this file instead of computing it.

        Raises:
            Exception: If both ht and cache_path are None, or if the cache file does not exist.
        """
        self.ht = ht
        self.df = self._load_or_compute_df(cache_path)

    def _load_or_compute_df(self, cache_path: Optional[Path]) -> pd.DataFrame:
        """
        Loads missingness report DataFrame from cache or computes it from the Hail Table.

        This method will first attempt to load the report from a CSV file specified by cache_path.
        If the cache_path is not provided or the file does not exist, it will compute the missingness
        report from the Hail Table by counting the missing data in each field.

        Args:
            cache_path (Optional[Path]): Path to a CSV file where the missingness report is cached.

        Returns:
            pd.DataFrame: A DataFrame containing the missingness report.

        Raises:
            Exception: If the Hail Table is not available when the report needs to be computed.
            ValueError: If the Hail Table contains an empty ArrayStructExpressions and indexing error occurs.
            RuntimeError: If an unexpected error occurs during the computation of the missingness report.
        """
        if cache_path and cache_path.exists():
            return pd.read_csv(str(cache_path))

        if not isinstance(self.ht, hl.Table):
            raise Exception(
                "Could not find cached missingess and hail table is not available"
            )

        assert self.ht

        try:
            missing = count_missing_fields_with_keys(self.ht)
        except hl.utils.java.HailUserError as hail_err:
            if "array index out of bounds" in str(hail_err):
                logger.error(
                    "HailUserError: Array index out of bounds. Make sure to replace empty "
                    "ArrayStructExpressions with null before using this tool."
                )
                raise ValueError(
                    "MissingnessReport encountered index out of bounds. Replace empty ArrayStructExpressions with null."
                ) from hail_err
            else:
                raise
        except Exception as exc:
            logger.exception(
                "An unexpected error occurred while counting missing fields."
            )
            raise RuntimeError(
                "An error occurred during the analysis of missingness."
            ) from exc

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

    def counts(self) -> Dict[str, int]:
        return self.df.set_index("field")["counts"].to_dict()
