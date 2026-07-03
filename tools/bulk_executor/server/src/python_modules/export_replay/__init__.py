from ..shared.bulk_executor_error import BulkExecutorError
from ..shared.export.pipeline import run_export_pipeline
from ..shared.export.utils.enums import ExportLoadType

TRANSFORM_PACKAGE = 'python_modules.export_replay.transform'


def _post_validate(validation):
    """Fail fast if export is not incremental with NEW_AND_OLD_IMAGES."""
    manifest_data = validation['manifest_data']
    if manifest_data['export_type'] != ExportLoadType.INCREMENTAL.value:
        raise BulkExecutorError(
            f"export-replay requires an incremental export, but this export is: {manifest_data['export_type']}. "
            f"Only incremental exports can be replayed forward."
        )
    if manifest_data.get('output_view') != 'NEW_AND_OLD_IMAGES':
        raise BulkExecutorError(
            f"export-replay requires output view NEW_AND_OLD_IMAGES, but this export has: {manifest_data.get('output_view')}. "
            f"Replay needs both old and new images for full context."
        )


def run(job, spark_context, glue_context, parsed_args):
    run_export_pipeline(
        spark_context, parsed_args,
        transform_package=TRANSFORM_PACKAGE,
        post_validate=_post_validate,
        post_transform=None,
    )
