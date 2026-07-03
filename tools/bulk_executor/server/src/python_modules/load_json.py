def run(job, spark_context, glue_context, parsed_args):
    from python_modules import load
    parsed_args['format'] = 'json'
    load.run(job, spark_context, glue_context, parsed_args)
