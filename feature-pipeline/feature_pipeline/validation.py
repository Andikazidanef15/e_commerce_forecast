from great_expectations.core import ExpectationSuite, ExpectationConfiguration


def build_expectation_suite() -> ExpectationSuite:
    """
    Builder used to retrieve an instance of the validation expectation suite.
    """

    expectation_suite_ecommerce_consumption = ExpectationSuite(
        expectation_suite_name="ecommerce_consumption_suite"
    )

    # Columns.
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={
                "column_list": [
                    "id",
                    "invoice_date",
                    "country",
                    "total_price"
                ]
            },
        )
    )
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_column_count_to_equal", kwargs={"value": 4}
        )
    )

    # Datetime UTC
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "invoice_date"},
        )
    )

    # Area
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_distinct_values_to_be_in_set",
            kwargs={"column": "country", "value_set": (0, 1, 2)},
        )
    )
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "country", "type_": "int8"},
        )
    )

    # Energy consumption
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_min_to_be_between",
            kwargs={
                "column": "total_price",
                "min_value": 0,
                "strict_min": False,
            },
        )
    )
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "total_price", "type_": "float64"},
        )
    )
    expectation_suite_ecommerce_consumption.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "total_price"},
        )
    )

    return expectation_suite_ecommerce_consumption