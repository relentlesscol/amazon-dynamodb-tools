from python_modules.shared.export.parsers.records import FullExportRecord, IncrementalExportRecord


def transform_full_record(record: FullExportRecord) -> list[FullExportRecord]:
    # export-replay only supports incremental exports since full exports lack operation metadata
    raise NotImplementedError("export-replay does not support full exports")


def transform_incremental_record(record: IncrementalExportRecord) -> list[IncrementalExportRecord]:
    """Default passthrough for incremental export records."""
    return [record]
