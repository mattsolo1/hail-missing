import pandas as pd
import pytest
from hail_missing.missingness import (
    MissingnessReport,
    count_missing_fields_with_keys,
)
from hail_missing.rich_table import ht


@pytest.fixture(scope="module")
def hail_table():
    return ht


@pytest.fixture(scope="module")
def missing_fields_with_keys_result(hail_table):
    return count_missing_fields_with_keys(hail_table)


@pytest.fixture(scope="module")
def missingness_report(hail_table):
    return MissingnessReport.create(hail_table)


def test_count_missing_fields_with_keys(missing_fields_with_keys_result):
    missing_keys = missing_fields_with_keys_result["missing_keys"]
    assert missing_keys["detailed_struct.long_field1"] == [{"k1": "key3", "k2": "key4"}]
    assert missing_keys["nested_complex_struct.detailed_struct.long_field1"] == [
        {"k1": "key3", "k2": "key4"}
    ]
    assert missing_keys["nested_complex_struct.inner_struct.long_t"] == [
        {"k1": "key3", "k2": "key4"}
    ]
    assert missing_keys["optional_field"] == [{"k1": "key3", "k2": "key4"}]
    assert missing_keys["deeply_nested_struct"] == [{"k1": "key3", "k2": "key4"}]
    assert missing_keys["deeply_nested_struct.outer_field.inner_field2"] == [
        {"k1": "key1", "k2": "key2"}
    ]
    assert missing_keys["array_of_structs.inner_array_of_structs.inner_n"] == [
        {"k1": "key1", "k2": "key2"}
    ]
    assert missing_keys[
        "array_of_structs.inner_array_of_structs.inner_s.another_field"
    ] == [{"k1": "key3", "k2": "key4"}]


def test_missingness_report_creation(missingness_report):
    assert isinstance(missingness_report, MissingnessReport)
    assert isinstance(missingness_report.df, pd.DataFrame)


def test_missingness_report_df_structure(missingness_report):
    expected_columns = ["field", "counts", "missing_keys", "missing_percent"]
    assert list(missingness_report.df.columns) == expected_columns
    assert len(missingness_report.df) > 0


def test_missingness_report_counts(missingness_report):
    report_dict = missingness_report.df.set_index("field").to_dict("index")

    assert report_dict["detailed_struct.long_field1"]["counts"] == 1
    assert (
        report_dict["nested_complex_struct.detailed_struct.long_field1"]["counts"] == 1
    )
    assert report_dict["nested_complex_struct.inner_struct.long_t"]["counts"] == 1
    assert report_dict["optional_field"]["counts"] == 1
    assert report_dict["deeply_nested_struct"]["counts"] == 1
    assert report_dict["deeply_nested_struct.outer_field.inner_field2"]["counts"] == 1
    assert report_dict["array_of_structs.inner_array_of_structs.inner_n"]["counts"] == 1
    assert (
        report_dict["array_of_structs.inner_array_of_structs.inner_s.another_field"][
            "counts"
        ]
        == 1
    )


def test_missingness_report_missing_keys(missingness_report):
    report_dict = missingness_report.df.set_index("field").to_dict("index")

    assert report_dict["detailed_struct.long_field1"]["missing_keys"] == [
        {"k1": "key3", "k2": "key4"}
    ]
    assert report_dict["nested_complex_struct.detailed_struct.long_field1"][
        "missing_keys"
    ] == [{"k1": "key3", "k2": "key4"}]
    assert report_dict["nested_complex_struct.inner_struct.long_t"]["missing_keys"] == [
        {"k1": "key3", "k2": "key4"}
    ]
    assert report_dict["optional_field"]["missing_keys"] == [
        {"k1": "key3", "k2": "key4"}
    ]
    assert report_dict["deeply_nested_struct"]["missing_keys"] == [
        {"k1": "key3", "k2": "key4"}
    ]
    assert report_dict["deeply_nested_struct.outer_field.inner_field2"][
        "missing_keys"
    ] == [{"k1": "key1", "k2": "key2"}]
    assert report_dict["array_of_structs.inner_array_of_structs.inner_n"][
        "missing_keys"
    ] == [{"k1": "key1", "k2": "key2"}]
    assert report_dict["array_of_structs.inner_array_of_structs.inner_s.another_field"][
        "missing_keys"
    ] == [{"k1": "key3", "k2": "key4"}]
