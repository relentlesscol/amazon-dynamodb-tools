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
    """Discover server modules from commands/*/server/ and zip them under python_modules/."""
    try:
        commands_dir = os.path.abspath(os.path.normpath(commands_dir))
        zip_path = os.path.abspath(os.path.normpath(zip_path))

        prefix = "python_modules"

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(prefix + '/', '')

            for entry in sorted(os.listdir(commands_dir)):
                entry_path = os.path.join(commands_dir, entry)
                if not os.path.isdir(entry_path):
                    continue

                server_dir = os.path.join(entry_path, "server")
                if not os.path.isdir(server_dir):
                    log.info(f"Skipping {entry}: no server/ directory")
                    continue

                # Determine if the command's server content is already self-namespaced:
                # i.e., has a <command_name>.py or <command_name>/ at the top level.
                top_level = os.listdir(server_dir)
                self_namespaced = (
                    f"{entry}.py" in top_level or
                    entry in top_level and os.path.isdir(os.path.join(server_dir, entry))
                )

                for root, dirs, files in os.walk(server_dir):
                    for d in sorted(dirs):
                        dir_path = os.path.join(root, d)
                        rel = os.path.relpath(dir_path, server_dir)
                        if self_namespaced:
                            arcname = os.path.join(prefix, rel) + '/'
                        else:
                            arcname = os.path.join(prefix, entry, rel) + '/'
                        zipf.writestr(arcname, '')

                    for f in sorted(files):
                        file_path = os.path.join(root, f)
                        if os.path.islink(file_path):
                            continue
                        rel = os.path.relpath(file_path, server_dir)
                        if self_namespaced:
                            arcname = os.path.join(prefix, rel)
                        else:
                            arcname = os.path.join(prefix, entry, rel)
                        zipf.write(file_path, arcname)

        log.info(f"Successfully zipped commands from {commands_dir} to {zip_path}")
        return True
    except Exception as e:
        log.error(f"Error zipping commands from {commands_dir}: {e}")
        return False
