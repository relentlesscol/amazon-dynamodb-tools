"""Thin wrapper: 'load-json' verb delegates to the load module with format='json'."""

from python_modules.load import run as _load_run


def run(job, spark_context, glue_context, parsed_args):
    parsed_args.setdefault('format', 'json')
    return _load_run(job, spark_context, glue_context, parsed_args)
