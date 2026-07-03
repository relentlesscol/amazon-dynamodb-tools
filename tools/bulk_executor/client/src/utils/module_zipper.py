import os
import zipfile

# project files
from infrastructure.constants import (
    PYTHON_MODULE_CLIENT_DIR_PATH,
    PYTHON_MODULE_CLIENT_ZIP_PATH
)
from utils.logger import log


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
                for file in files:
                    file_path = os.path.join(root, file)

                    # Skip any symlinked files out of an abundance of caution
                    if os.path.islink(file_path):
                        continue

                    # Add parent directory to the archive name
                    arcname = os.path.join(parent_dir, os.path.relpath(file_path, source_path))
                    zipf.write(file_path, arcname)

                # Preserve empty directories
                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    arcname = os.path.join(parent_dir, os.path.relpath(dir_path, source_path)) + '/'
                    zipf.writestr(arcname, '')

        log.info(f"Successfully zipped {source_path} to {zip_path} using Python zipfile")
        return True
    except Exception as e:
        log.error(f"Error zipping {source_path}: {e}")
        return False


def zip_commands(commands_dir, zip_path):
    """Discover server modules from commands/*/server/ and zip them under python_modules/.

    Each command folder's server/ contents are placed under python_modules/.
    If the server/ dir contains a file or directory matching the command name
    (e.g. commands/copy/server/copy.py), contents are merged flat.  Otherwise
    the command name becomes a package prefix (e.g. commands/shared/server/helper.py
    -> python_modules/shared/helper.py).
    """
    try:
        commands_dir = os.path.abspath(os.path.normpath(commands_dir))
        zip_path = os.path.abspath(os.path.normpath(zip_path))

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr('python_modules/', '')

            for entry in sorted(os.listdir(commands_dir)):
                server_dir = os.path.join(commands_dir, entry, 'server')
                if not os.path.isdir(server_dir):
                    continue

                # Determine if this command's server/ has a top-level entry
                # matching the command name (file or directory).  If so, merge
                # flat; otherwise namespace under the command name.
                top_entries = os.listdir(server_dir)
                has_canonical = (
                    (entry + '.py') in top_entries or
                    entry in top_entries
                )
                prefix = '' if has_canonical else entry + '/'

                if prefix:
                    zipf.writestr('python_modules/' + prefix, '')

                for root, dirs, files in os.walk(server_dir):
                    rel = os.path.relpath(root, server_dir)

                    # Add directory entries
                    for d in dirs:
                        dir_rel = os.path.join(rel, d) if rel != '.' else d
                        arcname = 'python_modules/' + prefix + dir_rel + '/'
                        zipf.writestr(arcname, '')

                    for f in files:
                        file_path = os.path.join(root, f)
                        if os.path.islink(file_path):
                            continue
                        file_rel = os.path.join(rel, f) if rel != '.' else f
                        arcname = 'python_modules/' + prefix + file_rel
                        zipf.write(file_path, arcname)

        log.info(f"Successfully zipped commands from {commands_dir} to {zip_path}")
        return True
    except Exception as e:
        log.error(f"Error zipping commands from {commands_dir}: {e}")
        return False
