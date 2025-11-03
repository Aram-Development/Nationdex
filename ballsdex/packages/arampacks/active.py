import fcntl
import json
import logging
import os
import time
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Dict, List, Optional

from ballsdex.settings import settings

# Create a custom logger that logs to a dedicated file and console, but not to ballsdex.log
log = logging.getLogger("ballsdex.packages.arampacks.active")
# Remove any existing handlers to prevent logging to ballsdex.log
log.handlers.clear()
# Set propagate to False to prevent parent loggers from handling these messages
log.propagate = False

# Create formatter for consistent log format
formatter = logging.Formatter(
    "[{asctime}] {levelname} {name}: {message}", datefmt="%Y-%m-%d %H:%M:%S", style="{"
)

# Create a console handler that will show in docker logs
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

# Create a file handler for the dedicated arampacks.log file
# Try multiple locations in case of permission issues
log_file_paths = [
    os.path.join(os.path.dirname(__file__), "arampacks.log"),  # Try current directory first
    os.path.join("logs", "arampacks.log"),  # Try logs subdirectory
    os.path.join("/tmp", "arampacks.log"),  # Fallback to /tmp
]

file_handler = None
for log_file_path in log_file_paths:
    try:
        # Ensure directory exists if using a subdirectory
        directory = os.path.dirname(log_file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)
        # Successfully created handler, break out of loop
        break
    except (PermissionError, OSError):
        # Continue to next path if this one fails
        continue

# If file_handler is None, file logging will be disabled (console only)
if file_handler is None:
    # Note: Logger not fully configured yet, but console logging will still work
    pass

# Set overall logger level
log.setLevel(logging.INFO)

# Path to save promocodes
PROMOCODES_FILE_PATH = "json/promocodes.json"
# Path to save archived (deleted/cleaned) promocodes
PROMOCODES_ARCHIVE_FILE_PATH = "json/promocodes_archive.json"

# Store active promocodes in memory
# Format: code -> {expiry_date, uses_left, max_uses_per_user, rewards, used_by}
# rewards: { "specific_ball": ball_id, "special": special_id }
# None for specific_ball means random ball, None for special means no special event
ACTIVE_PROMOCODES: Dict[str, Dict[str, Any]] = {
    # Default welcome promocode
    "WELCOMETONATIONDEX": {
        "expiry": datetime(2024, 12, 31, tzinfo=timezone.utc),
        "uses_left": 1000,
        "max_uses_per_user": 1,
        "rewards": {"specific_ball": None, "special": None},  # Random ball  # No special event
        "used_by": set(),  # Set of user IDs who used this code
    }
}

# File lock timeout in seconds
FILE_LOCK_TIMEOUT = 5

# Cache expiry in seconds (settings-configurable)
CACHE_EXPIRY = int(getattr(settings, "arampacks_cache_expiry", 300) or 300)

# In-process concurrency guard for ACTIVE_PROMOCODES
MEM_LOCK: RLock = RLock()

# Last time we loaded from disk and last observed file mtime
LAST_LOAD_TIME = 0.0
LAST_FILE_MTIME = 0.0


def save_promocodes_to_file() -> bool:
    """
    Save promocodes to file with file locking to prevent corruption

    Returns
    -------
    bool
        True if saved successfully, False otherwise
    """
    lock_file = None
    temp_file = None
    try:
        # Log the current state of ACTIVE_PROMOCODES before saving
        log.info(f"Saving promocodes to file. Current codes: {list(ACTIVE_PROMOCODES.keys())}")

        # Ensure directory exists
        directory = os.path.dirname(PROMOCODES_FILE_PATH)
        try:
            os.makedirs(directory, exist_ok=True)
        except PermissionError as e:
            log.error(f"Permission error creating directory {directory}: {e}")
            return False
        except OSError as e:
            log.error(f"OS error creating directory {directory}: {e}")
            return False

        # Check if directory is writable
        if not os.access(directory, os.W_OK):
            log.error(f"Directory {directory} is not writable. Check permissions.")
            return False

        # Create lock file path
        lock_file_path = f"{PROMOCODES_FILE_PATH}.lock"

        # Acquire lock with timeout
        lock_acquired = False
        start_time = time.time()
        while not lock_acquired and time.time() - start_time < FILE_LOCK_TIMEOUT:
            try:
                lock_file = open(lock_file_path, "w")
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_acquired = True
            except (IOError, OSError):
                if lock_file:
                    lock_file.close()
                    lock_file = None
                time.sleep(0.1)  # Wait a bit and try again

        if not lock_acquired:
            log.error(f"Failed to acquire file lock within {FILE_LOCK_TIMEOUT} seconds")
            return False

        # Create a temporary file name
        temp_file_path = f"{PROMOCODES_FILE_PATH}.tmp"

        # Prepare data for JSON serialization
        json_data = {}
        for code, data in ACTIVE_PROMOCODES.items():
            expiry_val = data.get("expiry")
            if isinstance(expiry_val, datetime):
                expiry_serialized = expiry_val.isoformat()
            else:
                expiry_serialized = expiry_val
            item = {
                "expiry": expiry_serialized,
                "uses_left": data.get("uses_left", 0),
                "max_uses_per_user": data.get("max_uses_per_user", 1),
                "rewards": data.get("rewards", {}),
                "used_by": (
                    list(data.get("used_by", []))
                    if isinstance(data.get("used_by"), set)
                    else data.get("used_by", [])
                ),
            }
            # Optional metadata
            if "created_at" in data:
                item["created_at"] = (
                    data["created_at"].isoformat()
                    if isinstance(data["created_at"], datetime)
                    else data["created_at"]
                )
            if "description" in data:
                item["description"] = data["description"]
            if "is_hidden" in data:
                item["is_hidden"] = data["is_hidden"]
            if "created_by" in data:
                item["created_by"] = data["created_by"]
            json_data[code] = item

        # Write to temporary file first
        with open(temp_file_path, "w", encoding="utf-8") as temp_file:
            json.dump(json_data, temp_file, indent=2, ensure_ascii=False)
            temp_file.flush()  # Make sure data is written
            os.fsync(temp_file.fileno())  # Force write to disk

        # Atomic move - rename temp file to actual file
        try:
            if os.path.exists(PROMOCODES_FILE_PATH):
                # Create backup of existing file
                backup_path = f"{PROMOCODES_FILE_PATH}.backup"
                try:
                    import shutil

                    shutil.copy2(PROMOCODES_FILE_PATH, backup_path)
                    log.debug(f"Created backup at {backup_path}")
                except Exception as backup_e:
                    log.warning(f"Failed to create backup: {backup_e}")
                    # Continue anyway since this is just a safety measure

            # On Windows, we need to remove the target file first
            if os.name == "nt" and os.path.exists(PROMOCODES_FILE_PATH):
                try:
                    os.remove(PROMOCODES_FILE_PATH)
                except OSError as e:
                    log.error(f"Failed to remove existing file on Windows: {e}")
                    return False

            os.rename(temp_file_path, PROMOCODES_FILE_PATH)
            # update last observed file mtime
            try:
                global LAST_FILE_MTIME
                LAST_FILE_MTIME = os.path.getmtime(PROMOCODES_FILE_PATH)
            except Exception:
                pass
            log.info(
                f"Successfully saved {len(ACTIVE_PROMOCODES)} promocodes to {PROMOCODES_FILE_PATH}"
            )
            return True

        except OSError as e:
            log.error(f"Failed to move temporary file to final destination: {e}")
            # Clean up temp file
            try:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
            except Exception as cleanup_e:
                log.warning(f"Failed to clean up temporary file: {cleanup_e}")
            return False

    except Exception as e:
        log.exception(f"Unexpected error saving promocodes: {e}")
        return False
    finally:
        # Clean up temporary file if it still exists
        try:
            temp_file_path = f"{PROMOCODES_FILE_PATH}.tmp"
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
        except Exception:
            pass

        # Release lock
        if lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                lock_file.close()
                # Remove lock file
                lock_file_path = f"{PROMOCODES_FILE_PATH}.lock"
                if os.path.exists(lock_file_path):
                    os.remove(lock_file_path)
            except Exception as e:
                log.warning(f"Error releasing file lock: {e}")


def load_promocodes_from_file() -> bool:
    """
    Load promocodes from file with caching and file locking

    Returns
    -------
    bool
        True if loaded successfully, False otherwise
    """
    global LAST_LOAD_TIME, LAST_FILE_MTIME, ACTIVE_PROMOCODES  # noqa: F824

    try:
        # Check if we should reload based on cache expiry
        current_time = time.time()
        if current_time - LAST_LOAD_TIME < CACHE_EXPIRY:
            log.debug(
                f"Using cached promocodes (last loaded {current_time - LAST_LOAD_TIME:.1f}s ago)"
            )
            return True

        if not os.path.exists(PROMOCODES_FILE_PATH):
            log.info(
                f"Promocodes file does not exist at {PROMOCODES_FILE_PATH}. Using default codes."
            )
            # Save the default codes to create the file
            return save_promocodes_to_file()

        # Check if file has been modified since last load
        try:
            file_mtime = os.path.getmtime(PROMOCODES_FILE_PATH)
            if LAST_FILE_MTIME and file_mtime <= LAST_FILE_MTIME:
                log.debug("File not modified since last load, using cached data")
                return True
        except OSError as e:
            log.warning(f"Could not get file modification time: {e}")

        # Load from file
        with open(PROMOCODES_FILE_PATH, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        if not json_data:
            log.warning(f"Empty promocodes file at {PROMOCODES_FILE_PATH}")
            return False

        # Clear current promocodes and load new ones
        ACTIVE_PROMOCODES.clear()

        for code, data in json_data.items():
            try:
                # Parse expiry date
                if isinstance(data.get("expiry"), str):
                    expiry = datetime.fromisoformat(data["expiry"])
                else:
                    expiry = data.get("expiry")

                # Convert used_by list back to set
                used_by = set(data.get("used_by", []))

                # Parse created_at if present
                created_at = data.get("created_at")
                if isinstance(created_at, str):
                    try:
                        created_at = datetime.fromisoformat(created_at)
                    except ValueError:
                        created_at = None

                entry = {
                    "expiry": expiry,
                    "uses_left": data.get("uses_left", 0),
                    "max_uses_per_user": data.get("max_uses_per_user", 1),
                    "rewards": data.get("rewards", {}),
                    "used_by": used_by,
                }
                if created_at:
                    entry["created_at"] = created_at
                if "description" in data:
                    entry["description"] = data["description"]
                if "is_hidden" in data:
                    entry["is_hidden"] = data["is_hidden"]
                if "created_by" in data:
                    entry["created_by"] = data["created_by"]

                ACTIVE_PROMOCODES[code] = entry
            except Exception as e:
                log.error(f"Error parsing promocode {code}: {e}")
                continue

        LAST_LOAD_TIME = current_time
        try:
            LAST_FILE_MTIME = os.path.getmtime(PROMOCODES_FILE_PATH)
        except Exception:
            pass
        log.info(f"Loaded {len(ACTIVE_PROMOCODES)} promocodes from {PROMOCODES_FILE_PATH}")
        return True

    except json.JSONDecodeError as e:
        log.error(f"Invalid JSON in promocodes file: {e}")
        return False
    except Exception as e:
        log.exception(f"Error loading promocodes from file: {e}")
        return False


def is_valid_promocode(code: str, user_id: int) -> tuple[bool, str]:
    """
    Check if a promocode is valid for a user

    Parameters
    ----------
    code : str
        The promocode to check
    user_id : int
        The Discord user ID

    Returns
    -------
    tuple[bool, str]
        (is_valid, error_message)
    """
    # Load latest promocodes from file
    load_promocodes_from_file()

    code = code.upper().strip()

    with MEM_LOCK:
        if code not in ACTIVE_PROMOCODES:
            return False, "❌ Invalid promocode. Please check your code and try again."
        promocode_data = ACTIVE_PROMOCODES[code]

    # Check if expired
    expiry = promocode_data.get("expiry")
    if expiry and datetime.now(timezone.utc) > expiry:
        return False, "❌ This promocode has expired."

    # Check if there are uses left
    uses_left = promocode_data.get("uses_left", 0)
    if uses_left <= 0:
        return False, "❌ This promocode has no uses remaining."

    # Check if user has already used this code
    used_by = promocode_data.get("used_by", set())
    max_uses_per_user = promocode_data.get("max_uses_per_user", 1)

    user_usage_count = sum(1 for uid in used_by if uid == user_id)
    if user_usage_count >= max_uses_per_user:
        return False, "❌ You have already used this promocode the maximum number of times."

    return True, ""


def mark_promocode_used(code: str, user_id: int) -> bool:
    """
    Mark a promocode as used by a user

    Parameters
    ----------
    code : str
        The promocode
    user_id : int
        The Discord user ID

    Returns
    -------
    bool
        True if successfully marked as used
    """
    try:
        code = code.upper().strip()

        with MEM_LOCK:
            if code not in ACTIVE_PROMOCODES:
                log.error(f"Attempted to mark unknown promocode as used: {code}")
                return False

            promocode_data = ACTIVE_PROMOCODES[code]

            # Add user to used_by set
            if "used_by" not in promocode_data:
                promocode_data["used_by"] = set()
            promocode_data["used_by"].add(user_id)

            # Decrease uses_left
            if promocode_data.get("uses_left", 0) > 0:
                promocode_data["uses_left"] -= 1

        # Save to file
        success = save_promocodes_to_file()
        if success:
            log.info(f"Marked promocode {code} as used by user {user_id}")
        else:
            log.error(f"Failed to save promocode usage for {code} by user {user_id}")

        return success

    except Exception as e:
        log.exception(f"Error marking promocode as used: {e}")
        return False


def get_active_promocodes(
    include_expired: bool = False,
    include_depleted: bool = False,
    include_hidden: bool = False,
    sort_by: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Get promocodes with filtering and optional sorting.

    Parameters
    ----------
    include_expired : bool
        Whether to include expired promocodes
    include_depleted : bool
        Whether to include promocodes with no uses left
    include_hidden : bool
        Whether to include hidden promocodes
    sort_by : Optional[str]
        One of "code", "expiry", "uses_left", "created_at" for sorting the results

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary of promocodes matching the filters. Order reflects the requested sort.
    """
    load_promocodes_from_file()

    current_time = datetime.now(timezone.utc)
    results: Dict[str, Dict[str, Any]] = {}

    for code, data in ACTIVE_PROMOCODES.items():
        expiry = data.get("expiry")
        if not include_expired and expiry and current_time > expiry:
            continue

        if not include_depleted and data.get("uses_left", 0) <= 0:
            continue

        if not include_hidden and data.get("is_hidden", False):
            continue

        results[code] = data.copy()

    if sort_by:

        def sort_key(item: tuple[str, Dict[str, Any]]):
            k, v = item
            if sort_by == "code":
                return k
            if sort_by == "expiry":
                val = v.get("expiry")
                if isinstance(val, str):
                    try:
                        val = datetime.fromisoformat(val)
                    except ValueError:
                        val = None
                return val or datetime.max
            if sort_by == "uses_left":
                return v.get("uses_left", 0)
            if sort_by == "created_at":
                val = v.get("created_at")
                if isinstance(val, str):
                    try:
                        val = datetime.fromisoformat(val)
                    except ValueError:
                        val = None
                return val or datetime.min
            return k

        sorted_items = sorted(results.items(), key=sort_key)
        results = {k: v for k, v in sorted_items}

    return results


def clean_expired_promocodes(archive: bool = True) -> int:
    """
    Remove expired or depleted promocodes from memory and optionally archive them.

    Parameters
    ----------
    archive : bool
        Whether to archive cleaned promocodes instead of discarding them.

    Returns
    -------
    int
        Number of promocodes removed
    """
    try:
        current_time = datetime.now(timezone.utc)
        expired_codes: List[str] = []

        for code, data in list(ACTIVE_PROMOCODES.items()):
            expiry = data.get("expiry")
            uses_left = data.get("uses_left", 0)

            # Mark as expired if past expiry date or no uses left
            if (expiry and current_time > expiry) or uses_left <= 0:
                expired_codes.append(code)

        archive_data: Dict[str, Any] = {}
        if archive and expired_codes:
            archive_data = _load_archive_data()

        # Remove expired codes (and collect for archive)
        for code in expired_codes:
            data = ACTIVE_PROMOCODES.pop(code, None)
            if data is not None and archive:
                archive_data[code] = _serialize_promocode_entry(data)
            log.info(f"Removed expired/depleted promocode: {code}")

        # Save updated active list
        if expired_codes:
            save_promocodes_to_file()

        # Save archive if needed
        if archive and expired_codes:
            _save_archive_data(archive_data)

        return len(expired_codes)

    except Exception as e:
        log.exception(f"Error cleaning expired promocodes: {e}")
        return 0


def get_promocode_rewards(code: str) -> Optional[Dict[str, Any]]:
    """
    Get the rewards for a promocode

    Parameters
    ----------
    code : str
        The promocode

    Returns
    -------
    Optional[Dict[str, Any]]
        The rewards dictionary, or None if code not found
    """
    load_promocodes_from_file()

    code = code.upper().strip()
    with MEM_LOCK:
        promocode_data = ACTIVE_PROMOCODES.get(code)

    if not promocode_data:
        return None

    return promocode_data.get("rewards", {})


# --- New helper and management functions ---


def _serialize_promocode_entry(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a promocode entry to a JSON-serializable dict, preserving metadata."""
    expiry_val = data.get("expiry")
    if isinstance(expiry_val, datetime):
        expiry_serialized = expiry_val.isoformat()
    else:
        expiry_serialized = expiry_val
    result: Dict[str, Any] = {
        "expiry": expiry_serialized,
        "uses_left": data.get("uses_left", 0),
        "max_uses_per_user": data.get("max_uses_per_user", 1),
        "rewards": data.get("rewards", {}),
        "used_by": (
            list(data.get("used_by", []))
            if isinstance(data.get("used_by"), set)
            else data.get("used_by", [])
        ),
    }
    if "created_at" in data:
        result["created_at"] = (
            data["created_at"].isoformat()
            if isinstance(data["created_at"], datetime)
            else data["created_at"]
        )
    if "description" in data:
        result["description"] = data["description"]
    if "is_hidden" in data:
        result["is_hidden"] = data["is_hidden"]
    if "created_by" in data:
        result["created_by"] = data["created_by"]
    return result


def _load_archive_data() -> Dict[str, Any]:
    try:
        if not os.path.exists(PROMOCODES_ARCHIVE_FILE_PATH):
            return {}
        with open(PROMOCODES_ARCHIVE_FILE_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as e:
        log.warning(f"Failed to load archive data: {e}")
        return {}


def _save_archive_data(data: Dict[str, Any]) -> bool:
    lock_file = None
    try:
        # Ensure directory exists
        directory = os.path.dirname(PROMOCODES_ARCHIVE_FILE_PATH)
        os.makedirs(directory, exist_ok=True)

        # Acquire lock
        lock_file_path = f"{PROMOCODES_ARCHIVE_FILE_PATH}.lock"
        lock_file = open(lock_file_path, "w")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        except Exception:
            # Best effort; proceed without locking if not supported
            pass

        # Write to temp file then move
        temp_path = f"{PROMOCODES_ARCHIVE_FILE_PATH}.tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

        # On Windows remove existing before rename
        if os.name == "nt" and os.path.exists(PROMOCODES_ARCHIVE_FILE_PATH):
            try:
                os.remove(PROMOCODES_ARCHIVE_FILE_PATH)
            except OSError:
                pass

        os.rename(temp_path, PROMOCODES_ARCHIVE_FILE_PATH)
        return True
    except Exception as e:
        log.warning(f"Failed to save archive data: {e}")
        return False
    finally:
        try:
            temp_path = f"{PROMOCODES_ARCHIVE_FILE_PATH}.tmp"
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass
        if lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                lock_file.close()
                lock_file_path = f"{PROMOCODES_ARCHIVE_FILE_PATH}.lock"
                if os.path.exists(lock_file_path):
                    os.remove(lock_file_path)
            except Exception:
                pass


def create_promocode(
    code: str,
    uses: int,
    expiry_date: datetime,
    specific_ball_id: Optional[int] = None,
    special_id: Optional[int] = None,
    max_uses_per_user: int = 1,
    description: str = "",
    is_hidden: bool = False,
    created_by: Optional[str] = None,
) -> bool:
    """Create a new promocode and persist it to file.

    Returns True on success, False otherwise.
    """
    try:
        if not code:
            raise ValueError("Code cannot be empty")
        code = code.strip().upper()
        if code in ACTIVE_PROMOCODES:
            raise ValueError("Code already exists")
        if uses <= 0:
            raise ValueError("Uses must be positive")
        if not isinstance(expiry_date, datetime):
            raise TypeError("expiry_date must be a datetime")

        ACTIVE_PROMOCODES[code] = {
            "expiry": expiry_date,
            "uses_left": uses,
            "max_uses_per_user": max_uses_per_user,
            "rewards": {
                "specific_ball": specific_ball_id,
                "special": special_id,
            },
            "used_by": set(),
            "created_at": datetime.now(timezone.utc),
            "description": description or "",
            "is_hidden": bool(is_hidden),
            "created_by": created_by or "",
        }

        return save_promocodes_to_file()
    except Exception as e:
        log.exception(f"Error creating promocode {code}: {e}")
        return False


def update_promocode_uses(code: str, uses_to_add: int) -> Optional[int]:
    """Update the uses_left of an existing promocode by adding uses_to_add (can be negative).

    Returns the new uses_left, or None on failure.
    """
    try:
        if not code:
            return None
        code = code.strip().upper()
        if code not in ACTIVE_PROMOCODES:
            return None
        current = int(ACTIVE_PROMOCODES[code].get("uses_left", 0))
        new_uses = max(0, current + int(uses_to_add))
        ACTIVE_PROMOCODES[code]["uses_left"] = new_uses
        if not save_promocodes_to_file():
            return None
        return new_uses
    except Exception as e:
        log.exception(f"Error updating uses for promocode {code}: {e}")
        return None


def delete_promocode(code: str, archive: bool = True) -> bool:
    """Delete a promocode. If archive is True, move it to the archive file."""
    try:
        if not code:
            return False
        code = code.strip().upper()
        if code not in ACTIVE_PROMOCODES:
            return False

        data = ACTIVE_PROMOCODES.pop(code)

        # Save active list first
        ok = save_promocodes_to_file()
        if not ok:
            return False

        if archive:
            archive_data = _load_archive_data()
            archive_data[code] = _serialize_promocode_entry(data)
            return _save_archive_data(archive_data)

        return True
    except Exception as e:
        log.exception(f"Error deleting promocode {code}: {e}")
        return False


# Initialize promocodes on import
try:
    load_promocodes_from_file()
    log.info(f"AramPacks promocode system initialized with {len(ACTIVE_PROMOCODES)} codes")
except Exception as e:
    log.error(f"Failed to initialize promocode system: {e}")


def reload_promocodes(force: bool = False) -> bool:
    """Force reload promocodes from disk, bypassing cache when force=True."""
    global LAST_LOAD_TIME, LAST_FILE_MTIME
    if force:
        LAST_LOAD_TIME = 0.0
        LAST_FILE_MTIME = 0.0
    return load_promocodes_from_file()
