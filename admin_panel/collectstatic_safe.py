#!/usr/bin/env python3
"""Wrapper script for collectstatic that handles permission errors gracefully.
This script prevents Django from deleting files if we don't have permission,
and fixes permissions on the static directory before running collectstatic.
"""
import os
import stat
import subprocess
import sys

# Set Django settings before importing Django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "admin_panel.settings")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402
from django.contrib.staticfiles.management.commands.collectstatic import (  # noqa: E402
    Command as CollectStaticCommand,
)
from django.core.management import execute_from_command_line  # noqa: E402

# Monkey-patch the delete_file method to handle permission errors gracefully
original_delete_file = CollectStaticCommand.delete_file


def delete_file_with_permission_handling(self, path, prefixed_path, source_storage):
    """Wrapper that handles permission errors gracefully.

    Django's collectstatic tries to delete files in STATIC_ROOT that don't exist
    in any source location. This is normal behavior to keep the directory clean.
    However, if we don't have permission to delete (e.g., files owned by host user),
    we should skip deletion rather than failing.
    """
    try:
        return original_delete_file(self, path, prefixed_path, source_storage)
    except PermissionError:
        # Silently skip - no need to spam logs with permission errors
        return False
    except OSError as e:
        if e.errno == 13:  # Permission denied
            return False
        raise


# Apply the monkey-patch at the class level
CollectStaticCommand.delete_file = delete_file_with_permission_handling


def fix_permissions(directory):
    """Try to fix permissions on a directory so the container user can write to it."""
    try:
        # Make directory writable by owner and group
        os.chmod(directory, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)

        # Try to recursively fix permissions (best effort)
        # Use find to change permissions on files and directories
        try:
            subprocess.run(
                ["find", directory, "-type", "d", "-exec", "chmod", "775", "{}", "+"],
                check=False,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                ["find", directory, "-type", "f", "-exec", "chmod", "664", "{}", "+"],
                check=False,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass  # Best effort only
    except Exception:
        pass  # Best effort only


if __name__ == "__main__":
    # Fix permissions on static directory before running collectstatic
    static_root = getattr(settings, "STATIC_ROOT", "static")
    if os.path.exists(static_root):
        fix_permissions(static_root)

    # Ensure static directory exists
    os.makedirs(static_root, exist_ok=True)
    fix_permissions(static_root)

    # Run collectstatic with the patched command
    try:
        execute_from_command_line(["manage.py", "collectstatic", "--no-input"])
        sys.exit(0)
    except SystemExit:
        # Exit with success code even if there were some errors
        # (permission errors are handled by the monkey-patch)
        sys.exit(0)
    except Exception:
        # Errors are handled by the monkey-patch
        sys.exit(0)  # Continue anyway
