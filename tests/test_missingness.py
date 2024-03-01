from pathlib import Path
import pandas as pd
import pytest
import hail as hl
from typing import Dict, Any
from hail_missing.missingness import MissingnessReport, count_missing_fields_with_keys
from hail_missing.rich_table import ht


@pytest.fixture(scope="module")
def hail_table() -> hl.Table:
    """Provides a Hail Table fixture for testing."""

    # Must replace ArrayStructExpressions with null before using this tool
    modified_ht = ht.annotate(j=hl.or_missing(hl.len(ht.j) > 0, ht.j))
    return modified_ht


@pytest.fixture(scope="module")
def bad_hail_table() -> hl.Table:
    """Table containing empty ArrayStructExpression"""
    return ht


def test_bad_table_raises_error(bad_hail_table):
    """Expect failure when table includes empty ArrayStructExpression"""
    with pytest.raises(hl.utils.java.HailUserError) as exc_info:
        count_missing_fields_with_keys(bad_hail_table)
    assert "Error summary: HailException: array index out of bounds" in str(
        exc_info.value
    )


@pytest.fixture(scope="module")
def missing_fields_with_keys_result(hail_table: hl.Table) -> Dict[str, Any]:
    """
    Provides the result of count_missing_fields_with_keys operation on the provided Hail Table.

    Args:
        hail_table (hl.Table): The Hail Table to count missing fields on.

    Returns:
        Dict[str, Any]: A dictionary with count results.
    """
    return count_missing_fields_with_keys(hail_table)


@pytest.fixture(scope="module")
def missingness_report(hail_table: hl.Table) -> MissingnessReport:
    """
    Provides a MissingnessReport instance based on the provided Hail Table.

    Args:
        hail_table (hl.Table): The Hail Table to create a missingness report from.

    Returns:
        MissingnessReport: An instance of MissingnessReport.
    """
    return MissingnessReport(hail_table)


def test_count_missing_fields_with_keys(
    missing_fields_with_keys_result: Dict[str, Any]
):
    """
    Test that count_missing_fields_with_keys returns correct missing keys for specific fields.

    Args:
        missing_fields_with_keys_result (Dict[str, Any]): The result of the count operation.
    """
    missing_keys = missing_fields_with_keys_result["missing_keys"]
    expected_missing_keys = {
        "detailed_struct.long_field1": [{"k1": "key3", "k2": "key4"}],
        "nested_complex_struct.detailed_struct.long_field1": [
            {"k1": "key3", "k2": "key4"}
        ],
        "nested_complex_struct.inner_struct.long_t": [{"k1": "key3", "k2": "key4"}],
        "optional_field": [{"k1": "key3", "k2": "key4"}],
        "deeply_nested_struct": [{"k1": "key3", "k2": "key4"}],
        "deeply_nested_struct.outer_field.inner_field2": [{"k1": "key1", "k2": "key2"}],
        "array_of_structs.inner_array_of_structs.inner_n": [
            {"k1": "key1", "k2": "key2"}
        ],
        "array_of_structs.inner_array_of_structs.inner_s.another_field": [
            {"k1": "key3", "k2": "key4"}
        ],
    }
    for field, expected_keys in expected_missing_keys.items():
        assert (
            missing_keys[field] == expected_keys
        ), f"Mismatch in missing keys for field: {field}"


def test_missingness_report_creation(missingness_report: MissingnessReport):
    """
    Test that an instance of MissingnessReport is correctly created.

    Args:
        missingness_report (MissingnessReport): The instance to test.
    """
    assert isinstance(missingness_report, MissingnessReport)
    assert isinstance(missingness_report.df, pd.DataFrame)


def test_missingness_report_df_structure(missingness_report: MissingnessReport):
    """
    Test that the dataframe within MissingnessReport has the expected structure.

    Args:
        missingness_report (MissingnessReport): The instance to test.
    """
    expected_columns = ["field", "counts", "missing_keys", "missing_percent"]
    assert (
        list(missingness_report.df.columns) == expected_columns
    ), "Dataframe columns do not match expected columns."
    assert len(missingness_report.df) > 0, "Dataframe should not be empty."


@pytest.mark.parametrize(
    "field_name, expected_count",
    [
        ("detailed_struct.long_field1", 1),
        ("nested_complex_struct.detailed_struct.long_field1", 1),
        ("nested_complex_struct.inner_struct.long_t", 1),
        ("optional_field", 1),
        ("deeply_nested_struct", 1),
        ("deeply_nested_struct.outer_field.inner_field2", 1),
        ("array_of_structs.inner_array_of_structs.inner_n", 1),
        ("array_of_structs.inner_array_of_structs.inner_s.another_field", 1),
    ],
)
def test_missingness_report_counts(
    missingness_report: MissingnessReport, field_name: str, expected_count: int
):
    """
    Test that the counts of missing entries in the MissingnessReport are as expected.

    Args:
        missingness_report (MissingnessReport): The instance to test.
        field_name (str): The field name to check the count for.
        expected_count (int): The expected count value.
    """
    assert (
        missingness_report.counts()[field_name] == expected_count
    ), f"Incorrect count for field: {field_name}"


@pytest.mark.parametrize(
    "field_name, expected_keys",
    [
        ("detailed_struct.long_field1", [{"k1": "key3", "k2": "key4"}]),
        (
            "nested_complex_struct.detailed_struct.long_field1",
            [{"k1": "key3", "k2": "key4"}],
        ),
        ("nested_complex_struct.inner_struct.long_t", [{"k1": "key3", "k2": "key4"}]),
        ("optional_field", [{"k1": "key3", "k2": "key4"}]),
        ("deeply_nested_struct", [{"k1": "key3", "k2": "key4"}]),
        (
            "deeply_nested_struct.outer_field.inner_field2",
            [{"k1": "key1", "k2": "key2"}],
        ),
        (
            "array_of_structs.inner_array_of_structs.inner_n",
            [{"k1": "key1", "k2": "key2"}],
        ),
        (
            "array_of_structs.inner_array_of_structs.inner_s.another_field",
            [{"k1": "key3", "k2": "key4"}],
        ),
    ],
)
def test_missingness_report_missing_keys(
    missingness_report: MissingnessReport, field_name: str, expected_keys: list
):
    """
    Test that the missing keys in the MissingnessReport are as expected for specific fields.

    Args:
        missingness_report (MissingnessReport): The instance to test.
        field_name (str): The field name to check the missing keys for.
        expected_keys (list): The expected list of missing keys.
    """
    report_dict = missingness_report.df.set_index("field").to_dict("index")
    assert (
        report_dict[field_name]["missing_keys"] == expected_keys
    ), f"Incorrect missing keys for field: {field_name}"
