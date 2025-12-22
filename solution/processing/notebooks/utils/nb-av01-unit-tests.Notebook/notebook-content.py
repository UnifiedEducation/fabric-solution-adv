# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "8e42a676-c1b7-8c84-4def-63a50b9c5c90",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "environment": {
# MAGIC         "id": {"variableName": "$(/**/vl-av01-variables/ENVIRONMENT_ID)"},
# MAGIC         "name": {"variableName": "$(/**/vl-av01-variables/ENVIRONMENT_NAME)"}
# MAGIC     }
# MAGIC }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb-av01-unit-tests
# # **Purpose**: Unit tests for core functions in nb-av01-generic-functions.
# # **Usage**: Run manually to validate function behavior after changes.
# # **Dependencies**: nb-av01-generic-functions
# # **Test Coverage**:
# - Path construction and layer mapping (pure functions)
# - Transform functions (DataFrame operations)
# - Surrogate key generation (critical dimensional modeling logic)
# - GX expectation building (validation setup)
# - Transform pipeline execution (orchestration logic)
# # **Not Tested** (external dependencies):
# - Metadata queries (require SQL database)
# - Logging functions (require SQL database)
# - File operations (require OneLake access)

# MARKDOWN ********************

# ## Imports & Setup

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test Helper Functions

# CELL ********************

def run_test(test_func):
    """Run a test function and report results."""
    test_name = test_func.__name__
    try:
        test_func()
        print(f"  PASSED: {test_name}")
        return True
    except AssertionError as e:
        print(f"  FAILED: {test_name} - {str(e)}")
        return False
    except Exception as e:
        print(f"  ERROR: {test_name} - {type(e).__name__}: {str(e)}")
        return False


def create_test_df(data, schema=None):
    """Helper to create test DataFrames consistently."""
    if schema:
        return spark.createDataFrame(data, schema)
    return spark.createDataFrame(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Path Construction Tests

# CELL ********************

def test_construct_abfs_path_tables():
    """Test ABFS path construction for Tables area."""
    result = construct_abfs_path("my_workspace", "my_lakehouse", "Tables")
    expected = "abfss://my_workspace@onelake.dfs.fabric.microsoft.com/my_lakehouse.Lakehouse/Tables/"
    assert result == expected, f"Expected {expected}, got {result}"


def test_construct_abfs_path_files():
    """Test ABFS path construction for Files area."""
    result = construct_abfs_path("my_workspace", "my_lakehouse", "Files")
    expected = "abfss://my_workspace@onelake.dfs.fabric.microsoft.com/my_lakehouse.Lakehouse/Files/"
    assert result == expected, f"Expected {expected}, got {result}"


def test_construct_abfs_path_default_area():
    """Test ABFS path construction with default area (Tables)."""
    result = construct_abfs_path("ws", "lh")
    assert "/Tables/" in result, "Default area should be 'Tables'"


def test_construct_abfs_path_empty_workspace_raises():
    """Test that empty workspace raises ValueError."""
    try:
        construct_abfs_path("", "lakehouse")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "must not be empty" in str(e)


def test_construct_abfs_path_empty_lakehouse_raises():
    """Test that empty lakehouse raises ValueError."""
    try:
        construct_abfs_path("workspace", "")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "must not be empty" in str(e)


def test_construct_abfs_path_special_characters():
    """Test that workspace/lakehouse names with special chars are handled."""
    result = construct_abfs_path("ws-prod_01", "lh-gold_analytics")
    assert "ws-prod_01" in result
    assert "lh-gold_analytics" in result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Layer Mapping Tests

# CELL ********************

class MockVariables:
    """Mock Variable Library for testing."""
    BRONZE_LH_NAME = "bronze_lakehouse"
    SILVER_LH_NAME = "silver_lakehouse"
    GOLD_LH_NAME = "gold_lakehouse"


def test_get_layer_lakehouse_all_layers():
    """Test all valid layer mappings."""
    mock_vars = MockVariables()

    # Raw and bronze both map to bronze lakehouse
    assert get_layer_lakehouse("raw", mock_vars) == "bronze_lakehouse"
    assert get_layer_lakehouse("bronze", mock_vars) == "bronze_lakehouse"
    assert get_layer_lakehouse("silver", mock_vars) == "silver_lakehouse"
    assert get_layer_lakehouse("gold", mock_vars) == "gold_lakehouse"


def test_get_layer_lakehouse_invalid_raises():
    """Test that invalid layer raises ValueError with helpful message."""
    try:
        get_layer_lakehouse("platinum", MockVariables())
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unknown layer" in str(e)
        assert "platinum" in str(e)
        # Should list valid options
        assert "raw" in str(e) or "bronze" in str(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform Function Tests
# # Tests for core DataFrame transformation functions used in pipeline processing.

# CELL ********************

def test_filter_nulls_single_column():
    """Test filter_nulls removes rows with null in specified column."""
    data = [
        Row(id=1, name="Alice"),
        Row(id=2, name=None),
        Row(id=3, name="Charlie")
    ]
    df = create_test_df(data)

    result = filter_nulls(df, ["name"])

    assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
    names = [r["name"] for r in result.collect()]
    assert None not in names, "Should not contain any null names"


def test_filter_nulls_multiple_columns():
    """Test filter_nulls with multiple columns (AND logic)."""
    data = [
        Row(id=1, name="Alice", age=30),
        Row(id=2, name=None, age=25),
        Row(id=3, name="Charlie", age=None),
        Row(id=4, name="David", age=40)
    ]
    df = create_test_df(data)

    result = filter_nulls(df, ["name", "age"])

    assert result.count() == 2, f"Expected 2 rows (only fully populated), got {result.count()}"
    ids = [r["id"] for r in result.collect()]
    assert 1 in ids and 4 in ids, "Should keep rows 1 and 4"


def test_filter_nulls_preserves_all_when_no_nulls():
    """Test filter_nulls keeps all rows when no nulls present."""
    data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
    df = create_test_df(data)

    result = filter_nulls(df, ["name"])

    assert result.count() == 2, "Should keep all rows when no nulls"


def test_rename_columns_single_rename():
    """Test renaming a single column."""
    data = [Row(old_name="test", other_col=1)]
    df = create_test_df(data)

    result = rename_columns(df, {"old_name": "new_name"})

    assert "new_name" in result.columns
    assert "old_name" not in result.columns
    assert "other_col" in result.columns


def test_rename_columns_multiple_renames():
    """Test renaming multiple columns in one operation."""
    data = [Row(col_a="val1", col_b="val2", col_c="val3")]
    df = create_test_df(data)

    result = rename_columns(df, {"col_a": "renamed_a", "col_b": "renamed_b"})

    assert "renamed_a" in result.columns
    assert "renamed_b" in result.columns
    assert "col_c" in result.columns  # Unchanged
    row = result.collect()[0]
    assert row["renamed_a"] == "val1"
    assert row["renamed_b"] == "val2"


def test_rename_columns_empty_mapping():
    """Test rename_columns with empty mapping (no-op)."""
    data = [Row(col_a="test")]
    df = create_test_df(data)

    result = rename_columns(df, {})

    assert result.columns == df.columns
    assert result.count() == 1


def test_add_literal_columns_various_types():
    """Test adding literal columns with different data types."""
    data = [Row(id=1), Row(id=2)]
    df = create_test_df(data)

    result = add_literal_columns(df, {
        "str_col": "constant",
        "int_col": 42,
        "bool_col": True,
        "null_col": None
    })

    row = result.collect()[0]
    assert row["str_col"] == "constant"
    assert row["int_col"] == 42
    assert row["bool_col"] == True
    assert row["null_col"] is None


def test_add_literal_columns_applied_to_all_rows():
    """Test that literal columns are added to every row."""
    data = [Row(id=i) for i in range(5)]
    df = create_test_df(data)

    result = add_literal_columns(df, {"source": "test_source"})

    rows = result.collect()
    assert all(r["source"] == "test_source" for r in rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dedupe Function Tests
# # Tests for window-based deduplication logic.

# CELL ********************

def test_dedupe_by_window_keeps_most_recent():
    """Test that dedupe keeps the most recent row when order_desc=True."""
    data = [
        Row(id=1, value="old", ts=1),
        Row(id=1, value="new", ts=2),
        Row(id=2, value="only", ts=1)
    ]
    df = create_test_df(data)

    result = dedupe_by_window(df, partition_cols=["id"], order_col="ts", order_desc=True)

    assert result.count() == 2
    id1_row = result.filter("id = 1").collect()[0]
    assert id1_row["value"] == "new", "Should keep most recent (ts=2)"


def test_dedupe_by_window_keeps_oldest_when_asc():
    """Test that dedupe keeps oldest when order_desc=False."""
    data = [
        Row(id=1, value="old", ts=1),
        Row(id=1, value="new", ts=2)
    ]
    df = create_test_df(data)

    result = dedupe_by_window(df, partition_cols=["id"], order_col="ts", order_desc=False)

    row = result.collect()[0]
    assert row["value"] == "old", "Should keep oldest (ts=1)"


def test_dedupe_by_window_multiple_partition_cols():
    """Test dedupe with composite partition key."""
    data = [
        Row(cat="A", subcat="X", value=1, ts=1),
        Row(cat="A", subcat="X", value=2, ts=2),
        Row(cat="A", subcat="Y", value=3, ts=1)
    ]
    df = create_test_df(data)

    result = dedupe_by_window(df, partition_cols=["cat", "subcat"], order_col="ts", order_desc=True)

    assert result.count() == 2, "Should keep one row per (cat, subcat) combination"


def test_dedupe_by_window_removes_helper_column():
    """Test that internal _row_num column is not in output."""
    data = [Row(id=1, ts=1), Row(id=1, ts=2)]
    df = create_test_df(data)

    result = dedupe_by_window(df, partition_cols=["id"], order_col="ts", order_desc=True)

    assert "_row_num" not in result.columns, "Internal column should be dropped"


def test_dedupe_by_window_no_duplicates():
    """Test dedupe with no actual duplicates (passthrough)."""
    data = [Row(id=1, ts=1), Row(id=2, ts=2), Row(id=3, ts=3)]
    df = create_test_df(data)

    result = dedupe_by_window(df, partition_cols=["id"], order_col="ts", order_desc=True)

    assert result.count() == 3, "Should keep all rows when no duplicates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Surrogate Key Generation Tests
# # Critical tests for dimensional modeling. Tests the core logic without Delta table dependencies.

# CELL ********************

def test_generate_surrogate_key_basic():
    """Test basic surrogate key generation starting from 1."""
    data = [Row(name="Alice", ts=1), Row(name="Bob", ts=2), Row(name="Charlie", ts=3)]
    df = create_test_df(data)

    # Basic case: no existing table, generate from 1
    result = generate_surrogate_key(
        df,
        key_column_name="sk_id",
        order_by_col="ts",
        spark=spark
    )

    assert "sk_id" in result.columns, "Should add surrogate key column"
    keys = sorted([r["sk_id"] for r in result.collect()])
    assert keys == [1, 2, 3], f"Expected [1, 2, 3], got {keys}"


def test_generate_surrogate_key_order_matters():
    """Test that keys are assigned in order_by_col order."""
    data = [
        Row(name="First", ts=1),
        Row(name="Third", ts=3),
        Row(name="Second", ts=2)
    ]
    df = create_test_df(data)

    result = generate_surrogate_key(
        df,
        key_column_name="sk_id",
        order_by_col="ts",
        spark=spark
    )

    rows = result.orderBy("ts").collect()
    assert rows[0]["name"] == "First" and rows[0]["sk_id"] == 1
    assert rows[1]["name"] == "Second" and rows[1]["sk_id"] == 2
    assert rows[2]["name"] == "Third" and rows[2]["sk_id"] == 3


def test_generate_surrogate_key_preserves_existing_columns():
    """Test that all original columns are preserved."""
    data = [Row(id=100, name="Test", category="A", ts=1)]
    df = create_test_df(data)

    result = generate_surrogate_key(
        df,
        key_column_name="sk_id",
        order_by_col="ts",
        spark=spark
    )

    row = result.collect()[0]
    assert row["id"] == 100
    assert row["name"] == "Test"
    assert row["category"] == "A"
    assert row["sk_id"] == 1


def test_generate_surrogate_key_single_row():
    """Test surrogate key generation with single row."""
    data = [Row(name="Only", ts=1)]
    df = create_test_df(data)

    result = generate_surrogate_key(
        df,
        key_column_name="sk_id",
        order_by_col="ts",
        spark=spark
    )

    assert result.count() == 1
    assert result.collect()[0]["sk_id"] == 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Function Lookup Tests

# CELL ********************

def test_get_transform_function_known_functions():
    """Test that all registered transform functions can be resolved."""
    known_functions = [
        "filter_nulls",
        "dedupe_by_window",
        "rename_columns",
        "add_literal_columns",
        "generate_surrogate_key",
        "lookup_join"
    ]

    for func_name in known_functions:
        func = get_transform_function(func_name)
        assert func is not None, f"{func_name} should be resolvable"
        assert callable(func), f"{func_name} should be callable"


def test_get_transform_function_returns_none_for_unknown():
    """Test that unknown function names return None (not error)."""
    result = get_transform_function("definitely_not_a_real_function_xyz")
    assert result is None, "Should return None for unknown function"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform Pipeline Tests
# # Tests for the orchestration layer that chains transforms together.

# CELL ********************

def test_execute_transform_pipeline_single_transform():
    """Test pipeline execution with a single transform."""
    data = [Row(id=1, name="Alice"), Row(id=2, name=None)]
    df = create_test_df(data)

    transform_lookup = {1: {"function_name": "filter_nulls", "transform_id": 1}}

    result = execute_transform_pipeline(
        spark=spark,
        df=df,
        pipeline=[1],
        params={"1": {"columns": ["name"]}},
        transform_lookup=transform_lookup
    )

    assert result.count() == 1, "Should filter out null row"


def test_execute_transform_pipeline_chained_transforms():
    """Test pipeline with multiple transforms executed in sequence."""
    data = [
        Row(id=1, old_name="Alice"),
        Row(id=2, old_name=None),
        Row(id=3, old_name="Charlie")
    ]
    df = create_test_df(data)

    transform_lookup = {
        1: {"function_name": "filter_nulls", "transform_id": 1},
        2: {"function_name": "rename_columns", "transform_id": 2}
    }

    result = execute_transform_pipeline(
        spark=spark,
        df=df,
        pipeline=[1, 2],
        params={
            "1": {"columns": ["old_name"]},
            "2": {"column_mapping": {"old_name": "new_name"}}
        },
        transform_lookup=transform_lookup
    )

    assert result.count() == 2, "Should have 2 rows after filtering"
    assert "new_name" in result.columns, "Should have renamed column"
    assert "old_name" not in result.columns, "Old name should be gone"


def test_execute_transform_pipeline_unknown_transform_raises():
    """Test that unknown transform_id raises descriptive error."""
    df = create_test_df([Row(id=1)])

    try:
        execute_transform_pipeline(
            spark=spark,
            df=df,
            pipeline=[999],
            params={},
            transform_lookup={}
        )
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "999" in str(e) or "not found" in str(e).lower()


def test_execute_transform_pipeline_unknown_function_raises():
    """Test that unknown function_name raises descriptive error."""
    df = create_test_df([Row(id=1)])

    transform_lookup = {1: {"function_name": "not_a_real_function", "transform_id": 1}}

    try:
        execute_transform_pipeline(
            spark=spark,
            df=df,
            pipeline=[1],
            params={},
            transform_lookup=transform_lookup
        )
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "not_a_real_function" in str(e) or "not implemented" in str(e).lower()


def test_execute_transform_pipeline_empty_pipeline():
    """Test pipeline with empty transform list (passthrough)."""
    data = [Row(id=1, name="Test")]
    df = create_test_df(data)

    result = execute_transform_pipeline(
        spark=spark,
        df=df,
        pipeline=[],
        params={},
        transform_lookup={}
    )

    assert result.count() == 1
    assert result.columns == df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GX Expectation Tests
# # Tests for Great Expectations integration - building expectations from metadata.

# CELL ********************

def test_get_expectation_class_common_types():
    """Test that common GX expectation classes can be resolved."""
    common_expectations = [
        "ExpectColumnValuesToNotBeNull",
        "ExpectColumnValuesToBeUnique",
        "ExpectColumnValuesToBeInSet",
        "ExpectColumnValuesToBeBetween"
    ]

    for exp_name in common_expectations:
        exp_class = get_expectation_class(exp_name)
        assert exp_class is not None, f"Should resolve {exp_name}"
        assert exp_class.__name__ == exp_name


def test_build_expectation_not_null():
    """Test building a not-null expectation."""
    exp = build_expectation(
        gx_method="ExpectColumnValuesToNotBeNull",
        column_name="required_field"
    )

    assert exp is not None
    assert exp.column == "required_field"


def test_build_expectation_with_value_set():
    """Test building expectation with value_set parameter."""
    exp = build_expectation(
        gx_method="ExpectColumnValuesToBeInSet",
        column_name="status",
        validation_params={"value_set": ["active", "inactive", "pending"]}
    )

    assert exp.column == "status"
    assert exp.value_set == ["active", "inactive", "pending"]


def test_build_expectation_with_range():
    """Test building expectation with min/max parameters."""
    exp = build_expectation(
        gx_method="ExpectColumnValuesToBeBetween",
        column_name="age",
        validation_params={"min_value": 0, "max_value": 150}
    )

    assert exp.column == "age"
    assert exp.min_value == 0
    assert exp.max_value == 150


def test_build_expectation_no_column():
    """Test building table-level expectation (no column)."""
    exp = build_expectation(
        gx_method="ExpectTableRowCountToBeBetween",
        validation_params={"min_value": 1, "max_value": 1000000}
    )

    assert exp is not None
    assert exp.min_value == 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run All Tests

# CELL ********************

def run_all_tests():
    """Run all test functions and report summary by category."""
    print("=" * 60)
    print("Unit Tests for nb-av01-generic-functions")
    print("=" * 60)

    # Organize tests by category for clearer output
    test_categories = {
        "Path Construction": [
            test_construct_abfs_path_tables,
            test_construct_abfs_path_files,
            test_construct_abfs_path_default_area,
            test_construct_abfs_path_empty_workspace_raises,
            test_construct_abfs_path_empty_lakehouse_raises,
            test_construct_abfs_path_special_characters,
        ],
        "Layer Mapping": [
            test_get_layer_lakehouse_all_layers,
            test_get_layer_lakehouse_invalid_raises,
        ],
        "Transform Functions": [
            test_filter_nulls_single_column,
            test_filter_nulls_multiple_columns,
            test_filter_nulls_preserves_all_when_no_nulls,
            test_rename_columns_single_rename,
            test_rename_columns_multiple_renames,
            test_rename_columns_empty_mapping,
            test_add_literal_columns_various_types,
            test_add_literal_columns_applied_to_all_rows,
        ],
        "Dedupe Functions": [
            test_dedupe_by_window_keeps_most_recent,
            test_dedupe_by_window_keeps_oldest_when_asc,
            test_dedupe_by_window_multiple_partition_cols,
            test_dedupe_by_window_removes_helper_column,
            test_dedupe_by_window_no_duplicates,
        ],
        "Surrogate Key Generation": [
            test_generate_surrogate_key_basic,
            test_generate_surrogate_key_order_matters,
            test_generate_surrogate_key_preserves_existing_columns,
            test_generate_surrogate_key_single_row,
        ],
        "Function Lookup": [
            test_get_transform_function_known_functions,
            test_get_transform_function_returns_none_for_unknown,
        ],
        "Transform Pipeline": [
            test_execute_transform_pipeline_single_transform,
            test_execute_transform_pipeline_chained_transforms,
            test_execute_transform_pipeline_unknown_transform_raises,
            test_execute_transform_pipeline_unknown_function_raises,
            test_execute_transform_pipeline_empty_pipeline,
        ],
        "GX Expectations": [
            test_get_expectation_class_common_types,
            test_build_expectation_not_null,
            test_build_expectation_with_value_set,
            test_build_expectation_with_range,
            test_build_expectation_no_column,
        ],
    }

    total_passed = 0
    total_failed = 0

    for category, tests in test_categories.items():
        print(f"\n{category}:")
        for test_func in tests:
            if run_test(test_func):
                total_passed += 1
            else:
                total_failed += 1

    # Summary
    total = total_passed + total_failed
    print("\n" + "=" * 60)
    print(f"Results: {total_passed} passed, {total_failed} failed, {total} total")
    print("=" * 60)

    if total_failed > 0:
        print("\nSome tests FAILED. Review output above for details.")
    else:
        print("\nAll tests PASSED!")

    return total_failed == 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run all tests
all_passed = run_all_tests()

# Fail the notebook job if any tests failed (for CI/CD integration)
if not all_passed:
    raise RuntimeError("Unit tests failed. See output above for details.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
