"""Thin wrapper: 'load-parquet' verb delegates to the load module with format='parquet'."""

from python_modules.load import run as _load_run


def run(job, spark_context, glue_context, parsed_args):
    parsed_args.setdefault('format', 'parquet')
    return _load_run(job, spark_context, glue_context, parsed_args)
