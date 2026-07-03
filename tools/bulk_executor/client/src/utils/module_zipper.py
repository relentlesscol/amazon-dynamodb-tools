import os
import zipfile

# project files
from infrastructure.constants import (
    PYTHON_MODULE_CLIENT_DIR_PATH,
    PYTHON_MODULE_CLIENT_ZIP_PATH
)
from utils.logger import log

# Directories to exclude entirely from the zip archive
_EXCLUDED_DIRS = {'__pycache__', 'data'}

# File patterns to exclude from the zip archive
_EXCLUDED_FILES = {'.DS_Store'}
_EXCLUDED_EXTENSIONS = {'.pyc'}


def _should_exclude_file(filename):
    """Return True if the file should be excluded from the archive."""
    if filename in _EXCLUDED_FILES:
        return True
    if os.path.splitext(filename)[1] in _EXCLUDED_EXTENSIONS:
        return True
    return False


def zip_module():
    return _zip_module(PYTHON_MODULE_CLIENT_DIR_PATH, PYTHON_MODULE_CLIENT_ZIP_PATH)

def _zip_module(source_path, zip_path):
    try:
        # Normalize and make absolute to avoid traversal
        source_path = os.path.abspath(os.path.normpath(source_path))
        zip_path = os.path.abspath(os.path.normpath(zip_path))

        # Guard to ensure we don't write the zip inside the tree being zipped
        if os.path.commonpath([source_path, zip_path]) == source_path:
            raise ValueError("zip_path must not be inside source_path")

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            parent_dir = os.path.basename(source_path.rstrip('/\\'))
            # Add an explicit entry for the parent directory
            zipf.writestr(parent_dir + '/', '')

            for root, dirs, files in os.walk(source_path):
                # Prune excluded directories in-place so os.walk skips them
                dirs[:] = [d for d in dirs if d not in _EXCLUDED_DIRS]

                for file in files:
                    # Skip excluded file patterns
                    if _should_exclude_file(file):
                        continue

                    file_path = os.path.join(root, file)

                    # Skip any symlinked files out of an abundance of caution
                    if os.path.islink(file_path):
                        continue

                    # Add parent directory to the archive name
                    arcname = os.path.join(parent_dir, os.path.relpath(file_path, source_path))
                    zipf.write(file_path, arcname)

                # Preserve empty directories (only non-excluded ones)
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    arcname = os.path.join(parent_dir, os.path.relpath(dir_path, source_path)) + '/'
                    zipf.writestr(arcname, '')

        log.info(f"Successfully zipped {source_path} to {zip_path} using Python zipfile")
        return True
    except Exception as e:
        log.error(f"Error zipping {source_path}: {e}")
        return False
