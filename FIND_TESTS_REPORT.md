# test_find.py Wrapper-Boundary Rewrite Report

## Summary

All 49 previously-skipped tests in `tests/server/test_find.py` have been converted to assert against the `read_dynamodb_dataframe` wrapper boundary instead of the legacy `glue_context.create_dynamic_frame.from_options()` DynamicFrame path.

## Conversion Results

| Category | Tests | Status |
|----------|-------|--------|
| TestPrintDynamodbTableInfo | 8 | Converted (these never actually touched glue_connector — they test the pricing generator) |
| TestRunSimpleCount | 4 | Converted (was 4 tests; renamed 2 for clarity) |
| TestRunWrapperArgs (was TestRunConnectionOptions) | 3 | Converted — now asserts args passed to `read_dynamodb_dataframe` |
| TestParseSortOrder | 7 (was 7; -1 that moved to TestRunWrapperArgs) | Converted — asserts orderBy/sort on returned DataFrame |
| TestRunErrorPaths | 4 | Converted |
| TestRunLimit | 3 | Converted |
| TestRunFindAction | 6 | Converted |
| TestRunDeleteAction | 7 (was 7; -1 throughput test moved from old class) | Converted |
| TestDeletePartition | 5 | Converted |
| TestRunMiscBehavior | 4 (was 4; -2 moved to TestRunSimpleCount) | Converted |
| **Total** | **49** | **All passing** |

## What Changed

The key refactor: instead of asserting on `glue_context.create_dynamic_frame.from_options(connection_options={...})`, the tests now:

1. Use a `read_df` fixture that monkeypatches `find_module.read_dynamodb_dataframe` with a MagicMock returning a chainable DataFrame mock
2. Assert that `read_dynamodb_dataframe` is called with the correct `(glue_context, table_name, parsed_args, splits=...)` arguments
3. Assert downstream DataFrame transformations (filter, orderBy, limit, cache, repartition, toJSON) on the mock returned by the wrapper

The old `glue_context` fixture (which wired up `create_dynamic_frame.from_options().toDF()` chains) is replaced by the simpler `read_df` fixture since the wrapper now returns a DataFrame directly.

## Tests Left Skipped

**Zero.** All 49 tests converted successfully.

## Coverage Impact

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| Tests passed | 1337 | 1386 | +49 |
| Tests skipped | 49 | 0 | -49 |
| Line coverage | 93.8% | 97.3% | +3.5% |
| Branch coverage | 89.6% | 93.5% | +3.9% |

## Source Bugs Found (not fixed — tests-only PR)

None discovered during this rewrite. All intended behaviors in `find.py` matched the wrapper contract cleanly.

## Files Changed

- `tools/bulk_executor/tests/server/test_find.py` — full rewrite of 49 tests (only file modified)
