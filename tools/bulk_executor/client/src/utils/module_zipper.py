import os
import zipfile

# project files
from infrastructure.constants import (
    PYTHON_MODULE_CLIENT_DIR_PATH,
    PYTHON_MODULE_CLIENT_ZIP_PATH
)
from utils.logger import log

# Directories to exclude from the zip
_EXCLUDED_DIRS = {'__pycache__', 'data', '.git'}

# File patterns to exclude from the zip
_EXCLUDED_FILES = {'.DS_Store'}
_EXCLUDED_EXTENSIONS = {'.pyc'}


def _should_exclude_dir(dirname):
    """Check if a directory should be excluded from the zip."""
    return dirname in _EXCLUDED_DIRS


def _should_exclude_file(filename):
    """Check if a file should be excluded from the zip."""
    if filename in _EXCLUDED_FILES:
        return True
    _, ext = os.path.splitext(filename)
    if ext in _EXCLUDED_EXTENSIONS:
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
                # Filter out excluded directories in-place to prevent os.walk
                # from descending into them
                dirs[:] = [d for d in dirs if not _should_exclude_dir(d)]

                for file in files:
                    # Skip excluded files
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


def discover_verb_requirements(modules_dir, verb=None):
    """Discover requirements.txt files for command verbs.

    Args:
        modules_dir: Path to the python_modules directory.
        verb: Specific verb name to discover requirements for.
              If None, merge all verb requirements.

    Returns:
        List of requirement strings, or empty list if none found.
    """
    requirements = []

    if verb is not None:
        # Check for directory-based verb with requirements.txt
        verb_dir = os.path.join(modules_dir, verb)
        req_file = os.path.join(verb_dir, 'requirements.txt')
        if os.path.isdir(verb_dir) and os.path.isfile(req_file):
            requirements.extend(_read_requirements(req_file))
        else:
            # Check for single-file verb with sibling requirements
            sibling_req = os.path.join(modules_dir, f'{verb}.requirements.txt')
            if os.path.isfile(sibling_req):
                requirements.extend(_read_requirements(sibling_req))
    else:
        # Merge all requirements from all verbs
        for entry in os.listdir(modules_dir):
            entry_path = os.path.join(modules_dir, entry)
            if os.path.isdir(entry_path):
                req_file = os.path.join(entry_path, 'requirements.txt')
                if os.path.isfile(req_file):
                    requirements.extend(_read_requirements(req_file))
            elif entry.endswith('.requirements.txt'):
                requirements.extend(_read_requirements(entry_path))

    return requirements


def _read_requirements(path):
    """Read a requirements.txt file and return non-empty, non-comment lines."""
    lines = []
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                lines.append(line)
    return lines
