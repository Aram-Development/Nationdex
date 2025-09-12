import json
import logging
import os
import random
import time
import fcntl
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Set, List, Union

# Create a custom logger that logs to a dedicated file and console, but not to ballsdex.log
log = logging.getLogger("ballsdex.packages.arampacks.active")
# Remove any existing handlers to prevent logging to ballsdex.log
log.handlers.clear()
# Set propagate to False to prevent parent loggers from handling these messages
log.propagate = False

# Create formatter for consistent log format
formatter = logging.Formatter('[{asctime}] {levelname} {name}: {message}', datefmt='%Y-%m-%d %H:%M:%S', style='{')

# Create a console handler that will show in docker logs
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

# Create a file handler for the dedicated promocode.log file
log_file_path = os.path.join(os.path.dirname(__file__), "arampacks.log")
file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# Set overall logger level
log.setLevel(logging.INFO)

# Path to save promocodes
PROMOCODES_FILE_PATH = "json/promocodes.json"

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
        "rewards": {
            "specific_ball": None,  # Random ball
            "special": None  # No special event
        },
        "used_by": set()  # Set of user IDs who used this code
    }
}

# File lock timeout in seconds
FILE_LOCK_TIMEOUT = 5

# Cache expiry in seconds (5 minutes)
CACHE_EXPIRY = 300

# Last time the promocodes were loaded from file
LAST_LOAD_TIME = 0

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
                lock_file = open(lock_file_path, 'w')
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_acquired = True
            except (IOError, OSError) as e:
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
            json_data[code] = {
                "expiry": data["expiry"].isoformat() if isinstance(data["expiry"], datetime) else data["expiry"],
                "uses_left": data["uses_left"],
                "max_uses_per_user": data["max_uses_per_user"],
                "rewards": data["rewards"],
                "used_by": list(data["used_by"]) if isinstance(data["used_by"], set) else data["used_by"]
            }
        
        # Write to temporary file first
        with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
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
            if os.name == 'nt' and os.path.exists(PROMOCODES_FILE_PATH):
                try:
                    os.remove(PROMOCODES_FILE_PATH)
                except OSError as e:
                    log.error(f"Failed to remove existing file on Windows: {e}")
                    return False
            
            os.rename(temp_file_path, PROMOCODES_FILE_PATH)
            log.info(f"Successfully saved {len(ACTIVE_PROMOCODES)} promocodes to {PROMOCODES_FILE_PATH}")
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
    global LAST_LOAD_TIME, ACTIVE_PROMOCODES
    
    try:
        # Check if we should reload based on cache expiry
        current_time = time.time()
        if current_time - LAST_LOAD_TIME < CACHE_EXPIRY:
            log.debug(f"Using cached promocodes (last loaded {current_time - LAST_LOAD_TIME:.1f}s ago)")
            return True
        
        if not os.path.exists(PROMOCODES_FILE_PATH):
            log.info(f"Promocodes file does not exist at {PROMOCODES_FILE_PATH}. Using default codes.")
            # Save the default codes to create the file
            return save_promocodes_to_file()
        
        # Check if file has been modified since last load
        try:
            file_mtime = os.path.getmtime(PROMOCODES_FILE_PATH)
            if file_mtime <= LAST_LOAD_TIME:
                log.debug(f"File not modified since last load, using cached data")
                return True
        except OSError as e:
            log.warning(f"Could not get file modification time: {e}")
        
        # Load from file
        with open(PROMOCODES_FILE_PATH, 'r', encoding='utf-8') as f:
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
                
                ACTIVE_PROMOCODES[code] = {
                    "expiry": expiry,
                    "uses_left": data.get("uses_left", 0),
                    "max_uses_per_user": data.get("max_uses_per_user", 1),
                    "rewards": data.get("rewards", {}),
                    "used_by": used_by
                }
            except Exception as e:
                log.error(f"Error parsing promocode {code}: {e}")
                continue
        
        LAST_LOAD_TIME = current_time
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

def get_active_promocodes(include_expired: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Get all active promocodes
    
    Parameters
    ----------
    include_expired : bool
        Whether to include expired promocodes
        
    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary of active promocodes
    """
    load_promocodes_from_file()
    
    if include_expired:
        return ACTIVE_PROMOCODES.copy()
    
    # Filter out expired codes
    current_time = datetime.now(timezone.utc)
    active_codes = {}
    
    for code, data in ACTIVE_PROMOCODES.items():
        expiry = data.get("expiry")
        if not expiry or current_time <= expiry:
            # Also check if there are uses left
            uses_left = data.get("uses_left", 0)
            if uses_left > 0:
                active_codes[code] = data.copy()
    
    return active_codes

def clean_expired_promocodes() -> int:
    """
    Remove expired promocodes from memory and file
    
    Returns
    -------
    int
        Number of promocodes removed
    """
    try:
        current_time = datetime.now(timezone.utc)
        expired_codes = []
        
        for code, data in ACTIVE_PROMOCODES.items():
            expiry = data.get("expiry")
            uses_left = data.get("uses_left", 0)
            
            # Mark as expired if past expiry date or no uses left
            if (expiry and current_time > expiry) or uses_left <= 0:
                expired_codes.append(code)
        
        # Remove expired codes
        for code in expired_codes:
            del ACTIVE_PROMOCODES[code]
            log.info(f"Removed expired promocode: {code}")
        
        # Save to file if any were removed
        if expired_codes:
            save_promocodes_to_file()
        
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
    promocode_data = ACTIVE_PROMOCODES.get(code)
    
    if not promocode_data:
        return None
    
    return promocode_data.get("rewards", {})

# Initialize promocodes on import
try:
    load_promocodes_from_file()
    log.info(f"AramPacks promocode system initialized with {len(ACTIVE_PROMOCODES)} codes")
except Exception as e:
    log.error(f"Failed to initialize promocode system: {e}")
