import json
import logging
import os
import random
import time
import fcntl
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Set, List, Union

# Create a custom logger that logs to a dedicated file and console, but not to ballsdex.log
log = logging.getLogger("ballsdex.packages.promocode.active")
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
log_file_path = os.path.join(os.path.dirname(__file__), "promocode.log")
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
        lock_file = open(lock_file_path, 'w')
        start_time = time.time()
        
        # Try to acquire lock with timeout
        while True:
            try:
                if os.name == 'nt':  # Windows doesn't have fcntl
                    # On Windows, just having the file open is enough as a lock
                    break
                else:
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
            except (IOError, OSError) as e:
                if time.time() - start_time > FILE_LOCK_TIMEOUT:
                    log.error(f"Timeout acquiring lock for {PROMOCODES_FILE_PATH}: {e}")
                    return False
                time.sleep(0.1)  # Small delay before retrying
        
        # Convert data to JSON-serializable format
        serialized_data = {}
        try:
            for code, data in ACTIVE_PROMOCODES.items():
                # Convert datetime to string
                serialized_item = {**data}
                
                # Handle expiry date
                if "expiry" in data and isinstance(data["expiry"], datetime):
                    serialized_item["expiry"] = data["expiry"].isoformat()
                else:
                    log.warning(f"Invalid expiry date for promocode {code}. Using default.")
                    serialized_item["expiry"] = datetime(2024, 12, 31, tzinfo=timezone.utc).isoformat()
                
                # Handle last_used date
                if "last_used" in data and isinstance(data["last_used"], datetime):
                    serialized_item["last_used"] = data["last_used"].isoformat()
                    
                # Handle created_at date
                if "created_at" in data and isinstance(data["created_at"], datetime):
                    serialized_item["created_at"] = data["created_at"].isoformat()
                    
                # Handle last_modified date
                if "last_modified" in data and isinstance(data["last_modified"], datetime):
                    serialized_item["last_modified"] = data["last_modified"].isoformat()
                
                # Convert set to list
                if "used_by" in data:
                    if isinstance(data["used_by"], set):
                        serialized_item["used_by"] = list(data["used_by"])
                    else:
                        log.warning(f"Invalid used_by data for promocode {code}. Using empty list.")
                        serialized_item["used_by"] = []
                
                # Handle usage_history - ensure all datetime objects are converted to strings
                if "usage_history" in data and isinstance(data["usage_history"], dict):
                    # Make a deep copy to avoid modifying the original
                    usage_history_copy = {}
                    for user_id, history in data["usage_history"].items():
                        if isinstance(history, list):
                            # Convert each entry in the list
                            usage_history_copy[user_id] = []
                            for entry in history:
                                if isinstance(entry, dict):
                                    entry_copy = entry.copy()
                                    # Convert timestamp if it's a datetime
                                    if "timestamp" in entry:
                                        if isinstance(entry["timestamp"], datetime):
                                            entry_copy["timestamp"] = entry["timestamp"].isoformat()
                                        elif isinstance(entry["timestamp"], str):
                                            # Already a string, keep as is
                                            entry_copy["timestamp"] = entry["timestamp"]
                                        else:
                                            # Invalid format, use current time
                                            log.warning(f"Invalid timestamp format in usage_history for promocode {code}, converting to current time")
                                            entry_copy["timestamp"] = datetime.now(timezone.utc).isoformat()
                                    usage_history_copy[user_id].append(entry_copy)
                                else:
                                    # Just add as is if not a dict
                                    usage_history_copy[user_id].append(entry)
                        else:
                            # Single entry (old format) or unknown format
                            usage_history_copy[user_id] = history
                    serialized_item["usage_history"] = usage_history_copy
                
                # Ensure rewards is present
                if "rewards" not in data or not isinstance(data["rewards"], dict):
                    log.warning(f"Invalid rewards data for promocode {code}. Using default.")
                    serialized_item["rewards"] = { "specific_ball": None, "special": None }
                    
                serialized_data[code] = serialized_item
        except Exception as e:
            log.error(f"Error serializing promocode data: {e}")
            return False
            
        # Write to file
        try:
            # First write to a temporary file, then rename to avoid corruption
            temp_file = f"{PROMOCODES_FILE_PATH}.tmp"
            with open(temp_file, "w") as f:
                json.dump(serialized_data, f, indent=4, sort_keys=True)
                # Ensure the file is fully written to disk
                os.fsync(f.fileno())
                
            # Replace the original file with the temporary file
            if os.path.exists(PROMOCODES_FILE_PATH):
                # On Windows, we need to remove the destination file first
                if os.name == 'nt' and os.path.exists(PROMOCODES_FILE_PATH):
                    os.remove(PROMOCODES_FILE_PATH)
                os.replace(temp_file, PROMOCODES_FILE_PATH)
            else:
                os.rename(temp_file, PROMOCODES_FILE_PATH)
                
            log.info(f"Saved {len(ACTIVE_PROMOCODES)} promocodes to {PROMOCODES_FILE_PATH}")
            return True
        except PermissionError as e:
            log.error(f"Permission error writing promocode file: {e}")
            return False
        except OSError as e:
            log.error(f"OS error writing promocode file: {e}")
            return False
        except json.JSONDecodeError as e:
            log.error(f"JSON encoding error writing promocode file: {e}")
            return False
        except Exception as e:
            log.error(f"Error writing promocode file: {e}")
            return False
    except Exception as e:
        log.error(f"Unexpected error saving promocodes to file: {e}")
        return False
    finally:
        # Clean up resources
        if temp_file and os.path.exists(f"{PROMOCODES_FILE_PATH}.tmp"):
            try:
                os.remove(f"{PROMOCODES_FILE_PATH}.tmp")
            except Exception as e:
                log.warning(f"Failed to remove temporary file: {e}")
        
        # Release lock
        if lock_file:
            try:
                if os.name != 'nt':  # Not Windows
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()
            except Exception as e:
                log.warning(f"Error releasing lock: {e}")
            
            # Try to remove lock file
            try:
                if os.path.exists(lock_file_path):
                    os.remove(lock_file_path)
            except Exception as e:
                log.warning(f"Failed to remove lock file: {e}")
        
def load_promocodes_from_file(force: bool = False) -> bool:
    """
    Load promocodes from file, making the file the source of truth.
    
    Parameters
    ----------
    force: bool
        Force reload from file even if cache is valid
    
    Returns
    -------
    bool
        True if loaded successfully, False otherwise
    """
    global ACTIVE_PROMOCODES, LAST_LOAD_TIME
    
    # Check if cache is still valid and not forced reload
    current_time = time.time()
    if not force and ACTIVE_PROMOCODES and current_time - LAST_LOAD_TIME < CACHE_EXPIRY:
        log.debug(f"Using cached promocodes (age: {current_time - LAST_LOAD_TIME:.1f}s)")
        return True
    
    try:
        if not os.path.exists(PROMOCODES_FILE_PATH):
            log.info(f"Promocode file not found at {PROMOCODES_FILE_PATH}. Using default in-memory codes.")
            # On first run, the file may not exist. We rely on the initial ACTIVE_PROMOCODES
            # and the first save operation will create the file.
            return True

        # Check if file is readable
        if not os.access(PROMOCODES_FILE_PATH, os.R_OK):
            log.error(f"Promocode file at {PROMOCODES_FILE_PATH} is not readable. Check file permissions.")
            return False

        lock_file = None
        lock_file_path = f"{PROMOCODES_FILE_PATH}.lock"
        
        try:
            # Acquire shared lock with timeout for reading
            lock_file = open(lock_file_path, 'w')
            start_time = time.time()
            
            # Try to acquire lock with timeout
            while True:
                try:
                    if os.name == 'nt':  # Windows doesn't have fcntl
                        # On Windows, just having the file open is enough as a lock
                        break
                    else:
                        # Use shared lock for reading
                        fcntl.flock(lock_file, fcntl.LOCK_SH | fcntl.LOCK_NB)
                        break
                except (IOError, OSError) as e:
                    if time.time() - start_time > FILE_LOCK_TIMEOUT:
                        log.error(f"Timeout acquiring read lock for {PROMOCODES_FILE_PATH}: {e}")
                        return False
                    time.sleep(0.1)  # Small delay before retrying
            
            with open(PROMOCODES_FILE_PATH, "r") as f:
                content = f.read()
                if not content:
                    log.warning(f"Promocode file at {PROMOCODES_FILE_PATH} is empty.")
                    serialized_data = {}
                else:
                    try:
                        serialized_data = json.loads(content)
                        if not isinstance(serialized_data, dict):
                            log.error(f"Promocode file at {PROMOCODES_FILE_PATH} does not contain a valid JSON object.")
                            return False
                    except json.JSONDecodeError as e:
                        log.error(f"Error decoding JSON from {PROMOCODES_FILE_PATH}: {e}")
                        # Try to recover by backing up corrupted file
                        backup_path = f"{PROMOCODES_FILE_PATH}.corrupted.{int(time.time())}"  
                        try:
                            os.rename(PROMOCODES_FILE_PATH, backup_path)
                            log.warning(f"Backed up corrupted file to {backup_path}")
                            # Create a new empty file
                            with open(PROMOCODES_FILE_PATH, "w") as f:
                                f.write("{}")
                            log.info(f"Created new empty promocodes file")
                        except Exception as backup_error:
                            log.error(f"Failed to backup corrupted file: {backup_error}")
                        return False
        except PermissionError as e:
            log.error(f"Permission error reading promocode file: {e}")
            return False
        except OSError as e:
            log.error(f"OS error reading promocode file: {e}")
            return False
        finally:
            # Release lock
            if lock_file:
                try:
                    if os.name != 'nt':  # Not Windows
                        fcntl.flock(lock_file, fcntl.LOCK_UN)
                    lock_file.close()
                except Exception as e:
                    log.warning(f"Error releasing read lock: {e}")
                
                # Try to remove lock file
                try:
                    if os.path.exists(lock_file_path):
                        os.remove(lock_file_path)
                except Exception as e:
                    log.warning(f"Failed to remove lock file: {e}")
                
        # Convert serialized data back to proper format
        processed_data = {}
        try:
            for code, data in serialized_data.items():
                # Skip invalid codes
                if not code or not isinstance(code, str):
                    log.warning(f"Skipping invalid promocode key: {code}")
                    continue
                    
                # Create a copy to avoid modifying the original
                processed_item = data.copy() if isinstance(data, dict) else {}
                
                # Validate required fields
                if not all(key in data for key in ["expiry", "uses_left", "rewards"]):
                    log.warning(f"Promocode {code} is missing required fields. Skipping.")
                    continue
                    
                # Convert expiry date
                if "expiry" in data and isinstance(data["expiry"], str):
                    try:
                        processed_item["expiry"] = datetime.fromisoformat(data["expiry"])
                    except ValueError as e:
                        log.warning(f"Invalid expiry date format for promocode {code}: {e}. Using default expiry.")
                        processed_item["expiry"] = datetime(2024, 12, 31, tzinfo=timezone.utc)
                elif "expiry" in data and isinstance(data["expiry"], datetime):
                    processed_item["expiry"] = data["expiry"]
                else:
                    log.warning(f"Invalid or missing expiry for promocode {code}. Using default expiry.")
                    processed_item["expiry"] = datetime(2024, 12, 31, tzinfo=timezone.utc)
                    
                # Convert last_used date if present
                if "last_used" in data and isinstance(data["last_used"], str):
                    try:
                        processed_item["last_used"] = datetime.fromisoformat(data["last_used"])
                    except ValueError as e:
                        log.warning(f"Invalid last_used date format for promocode {code}: {e}. Removing last_used.")
                        # Don't include last_used if it's invalid
                elif "last_used" in data and isinstance(data["last_used"], datetime):
                    processed_item["last_used"] = data["last_used"]
                    
                # Convert created_at date if present
                if "created_at" in data and isinstance(data["created_at"], str):
                    try:
                        processed_item["created_at"] = datetime.fromisoformat(data["created_at"])
                    except ValueError as e:
                        log.warning(f"Invalid created_at date format for promocode {code}: {e}. Using current time.")
                        processed_item["created_at"] = datetime.now(timezone.utc)
                elif "created_at" in data and isinstance(data["created_at"], datetime):
                    processed_item["created_at"] = data["created_at"]
                    
                # Convert last_modified date if present
                if "last_modified" in data and isinstance(data["last_modified"], str):
                    try:
                        processed_item["last_modified"] = datetime.fromisoformat(data["last_modified"])
                    except ValueError as e:
                        log.warning(f"Invalid last_modified date format for promocode {code}: {e}. Using current time.")
                        processed_item["last_modified"] = datetime.now(timezone.utc)
                elif "last_modified" in data and isinstance(data["last_modified"], datetime):
                    processed_item["last_modified"] = data["last_modified"]
                        
                # Convert used_by list to set
                if "used_by" in data and isinstance(data["used_by"], list):
                    processed_item["used_by"] = set(data["used_by"])
                elif "used_by" in data and isinstance(data["used_by"], set):
                    processed_item["used_by"] = data["used_by"]
                else:
                    processed_item["used_by"] = set()
                
                # Process usage_history - convert timestamps from strings to datetime objects
                if "usage_history" in data and isinstance(data["usage_history"], dict):
                    processed_history = {}
                    for user_id, history in data["usage_history"].items():
                        if isinstance(history, list):
                            # Process each entry in the list
                            processed_entries = []
                            for entry in history:
                                if isinstance(entry, dict):
                                    entry_copy = entry.copy()
                                    # Convert timestamp string to datetime if needed
                                    if "timestamp" in entry and isinstance(entry["timestamp"], str):
                                        try:
                                            entry_copy["timestamp"] = datetime.fromisoformat(entry["timestamp"])
                                        except ValueError as e:
                                            log.warning(f"Invalid timestamp format in usage_history for promocode {code}, user {user_id}: {e}")
                                    processed_entries.append(entry_copy)
                                else:
                                    # Just add as is if not a dict
                                    processed_entries.append(entry)
                            processed_history[user_id] = processed_entries
                        else:
                            # Single entry (old format) or unknown format
                            processed_history[user_id] = history
                    processed_item["usage_history"] = processed_history
                    
                # Ensure rewards is a dictionary
                if "rewards" in data and isinstance(data["rewards"], dict):
                    processed_item["rewards"] = data["rewards"]
                else:
                    log.warning(f"Invalid rewards format for promocode {code}. Using default rewards.")
                    processed_item["rewards"] = { "specific_ball": None, "special": None }
                    
                # Ensure uses_left is an integer
                if "uses_left" in data and isinstance(data["uses_left"], (int, float)):
                    processed_item["uses_left"] = int(data["uses_left"])
                else:
                    log.warning(f"Invalid uses_left for promocode {code}. Setting to 0.")
                    processed_item["uses_left"] = 0
                    
                # Ensure max_uses_per_user is an integer
                if "max_uses_per_user" in data and isinstance(data["max_uses_per_user"], (int, float)):
                    processed_item["max_uses_per_user"] = int(data["max_uses_per_user"])
                else:
                    processed_item["max_uses_per_user"] = 1
                    
                processed_data[code] = processed_item
        except Exception as e:
            log.error(f"Error processing promocode data: {e}")
            log.exception("Detailed error information:")
            return False
        
        # The file is the source of truth. We replace the in-memory dict.
        # Clear existing promocodes and update with processed data
        ACTIVE_PROMOCODES.clear()
        ACTIVE_PROMOCODES.update(processed_data)
        
        # Update last load time
        LAST_LOAD_TIME = current_time
        
        # Log the state of ACTIVE_PROMOCODES after update
        log.info(f"ACTIVE_PROMOCODES after update: {list(ACTIVE_PROMOCODES.keys())}")
        
        # Ensure the default code is present if it was somehow removed from the file
        if "WELCOMETONATIONDEX" not in ACTIVE_PROMOCODES:
            log.info(f"Adding default WELCOMETONATIONDEX promocode")
            ACTIVE_PROMOCODES["WELCOMETONATIONDEX"] = {
                "expiry": datetime(2024, 12, 31, tzinfo=timezone.utc),
                "uses_left": 1000,
                "max_uses_per_user": 1,
                "rewards": { "specific_ball": None, "special": None },
                "used_by": set()
            }
        
        # Ensure WELCOMETOPLEASUREDOME is present if it exists in the file
        if "WELCOMETOPLEASUREDOME" in processed_data and "WELCOMETOPLEASUREDOME" not in ACTIVE_PROMOCODES:
            log.warning(f"WELCOMETOPLEASUREDOME was in processed data but not in ACTIVE_PROMOCODES. Adding it.")
            ACTIVE_PROMOCODES["WELCOMETOPLEASUREDOME"] = processed_data["WELCOMETOPLEASUREDOME"]

        log.info(f"Loaded {len(ACTIVE_PROMOCODES)} promocodes from {PROMOCODES_FILE_PATH}")
        return True
    except json.JSONDecodeError as e:
        log.error(f"Error decoding JSON from {PROMOCODES_FILE_PATH}: {e}")
        return False
    except Exception as e:
        log.error(f"Error loading promocodes from file: {e}")
        log.exception("Detailed error information:")
        return False

def create_promocode(
    code: str, 
    uses: int, 
    expiry_date: Optional[datetime] = None,
    expiry_days: int = 30, 
    specific_ball_id: Optional[int] = None, 
    special_id: Optional[int] = None, 
    max_uses_per_user: int = 1,
    description: Optional[str] = None,
    is_hidden: bool = False,
    created_by: Optional[str] = None,
    custom_rewards: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Create a new promocode with enhanced features
    
    Parameters
    ----------
    code: str
        The promocode to create
    uses: int
        Number of times this code can be used
    expiry_date: Optional[datetime]
        Specific expiry date (overrides expiry_days if provided)
    expiry_days: int
        Days until the code expires (default: 30)
    specific_ball_id: Optional[int]
        The specific ball ID to reward (optional, random if not provided)
    special_id: Optional[int]
        Special event to apply to the ball (optional)
    max_uses_per_user: int
        Maximum uses per user (default: 1)
    description: Optional[str]
        Description of the promocode for admin reference
    is_hidden: bool
        Whether the promocode should be hidden from public listings
    created_by: Optional[str]
        Username or ID of the admin who created the promocode (stored in JSON for reference)
    custom_rewards: Optional[Dict[str, Any]]
        Additional custom rewards to include with this promocode
    
    Returns
    -------
    bool
        True if created successfully, False otherwise
    """
    try:
        # Normalize the code
        code = code.strip().upper()
        
        # Validate code format (alphanumeric and underscore only)
        if not code.replace("_", "").isalnum():
            log.error(f"Invalid promocode format: {code} (must be alphanumeric with optional underscores)")
            return False
        
        # Check if code already exists
        if code in ACTIVE_PROMOCODES:
            log.error(f"Promocode {code} already exists")
            return False
        
        # Validate uses
        if uses <= 0:
            log.error(f"Invalid uses count for promocode {code}: {uses}")
            return False
        
        # Set expiry date
        if expiry_date is None:
            expiry = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)
            expiry += timedelta(days=expiry_days)
        else:
            expiry = expiry_date
        
        # Validate max_uses_per_user
        if max_uses_per_user <= 0:
            log.warning(f"Invalid max_uses_per_user for promocode {code}, setting to 1")
            max_uses_per_user = 1
        
        # Prepare rewards dictionary
        rewards = {
            "specific_ball": specific_ball_id,  # None means random ball
            "special": special_id               # None means no special event
        }
        
        # Add custom rewards if provided
        if custom_rewards and isinstance(custom_rewards, dict):
            rewards.update(custom_rewards)
        
        # Create promocode with enhanced metadata
        ACTIVE_PROMOCODES[code] = {
            "expiry": expiry,
            "uses_left": uses,
            "max_uses_per_user": max_uses_per_user,
            "rewards": rewards,
            "used_by": set(),
            "created_at": datetime.now(timezone.utc),
            "is_hidden": is_hidden
        }
        
        # Add optional fields if provided
        if description:
            ACTIVE_PROMOCODES[code]["description"] = description
            
        if created_by:
            ACTIVE_PROMOCODES[code]["created_by"] = created_by
        
        # Save changes
        if save_promocodes_to_file():
            log.info(f"Created promocode {code} with {uses} uses, expires on {expiry.strftime('%Y-%m-%d')}")
            return True
        else:
            # If save failed, remove from memory
            if code in ACTIVE_PROMOCODES:
                del ACTIVE_PROMOCODES[code]
            log.error(f"Failed to save promocode {code} to file")
            return False
    except Exception as e:
        log.error(f"Error creating promocode {code}: {e}")
        return False

def update_promocode_uses(code: str, uses_to_add: int) -> Optional[int]:
    """
    Update promocode uses
    
    Parameters
    ----------
    code: str
        The promocode to update
    uses_to_add: int
        Number of uses to add (can be negative to decrease)
    
    Returns
    -------
    Optional[int]
        New uses count if updated successfully, None otherwise
    """
    try:
        # Ensure promocodes are loaded
        if not ACTIVE_PROMOCODES:
            if not load_promocodes_from_file():
                log.error(f"Failed to load promocodes before updating {code}")
                return None
        
        # Normalize the code
        code = code.strip().upper()
        
        if code not in ACTIVE_PROMOCODES:
            log.warning(f"Promocode {code} not found for update")
            return None
        
        # Update uses
        ACTIVE_PROMOCODES[code]["uses_left"] += uses_to_add
        
        # Ensure we don't go below 0
        if ACTIVE_PROMOCODES[code]["uses_left"] < 0:
            ACTIVE_PROMOCODES[code]["uses_left"] = 0
            log.warning(f"Adjusted uses for promocode {code} to 0 (was negative)")
        
        # Update last_modified timestamp
        ACTIVE_PROMOCODES[code]["last_modified"] = datetime.now(timezone.utc)
            
        # Save changes
        if save_promocodes_to_file():
            log.info(f"Updated promocode {code} uses to {ACTIVE_PROMOCODES[code]['uses_left']}")
            return ACTIVE_PROMOCODES[code]["uses_left"]
        
        log.error(f"Failed to save promocode {code} after updating uses")
        return None
    except KeyError as e:
        log.error(f"Key error updating promocode {code}: {e}")
        return None
    except TypeError as e:
        log.error(f"Type error updating promocode {code}: {e}")
        return None
    except Exception as e:
        log.error(f"Error updating promocode {code}: {e}")
        return None

def delete_promocode(code: str, archive: bool = True) -> bool:
    """
    Delete a promocode with optional archiving
    
    Parameters
    ----------
    code: str
        The promocode to delete
    archive: bool
        Whether to archive the promocode instead of permanently deleting
    
    Returns
    -------
    bool
        True if deleted successfully, False otherwise
    """
    try:
        # Ensure promocodes are loaded
        if not ACTIVE_PROMOCODES:
            if not load_promocodes_from_file():
                log.error(f"Failed to load promocodes before deleting {code}")
                return False
        
        # Normalize the code
        code = code.strip().upper()
        
        if code not in ACTIVE_PROMOCODES:
            log.warning(f"Promocode {code} not found for deletion")
            return False
        
        # Get promocode data for archiving
        promocode_data = ACTIVE_PROMOCODES[code]
        
        # Delete promocode from active dictionary
        del ACTIVE_PROMOCODES[code]
        
        # Archive the promocode if requested
        if archive:
            try:
                # Create archive directory if it doesn't exist
                archive_dir = os.path.join(os.path.dirname(PROMOCODES_FILE_PATH), "archived_promocodes")
                os.makedirs(archive_dir, exist_ok=True)
                
                # Create archive file if it doesn't exist
                archive_file = os.path.join(archive_dir, "archived_promocodes.json")
                archived_promocodes = {}
                
                # Load existing archived promocodes if file exists
                if os.path.exists(archive_file):
                    try:
                        with open(archive_file, "r", encoding="utf-8") as f:
                            content = f.read().strip()
                            if content:
                                archived_promocodes = json.loads(content)
                    except Exception as e:
                        log.warning(f"Error loading archived promocodes: {e}")
                
                # Add deletion timestamp
                promocode_data["deleted_at"] = datetime.now(timezone.utc).isoformat()
                
                # Convert sets to lists for JSON serialization
                if "used_by" in promocode_data and isinstance(promocode_data["used_by"], set):
                    promocode_data["used_by"] = list(promocode_data["used_by"])
                
                # Convert datetime objects to ISO format strings
                for key, value in list(promocode_data.items()):
                    if isinstance(value, datetime):
                        promocode_data[key] = value.isoformat()
                
                # Add to archived promocodes
                archived_promocodes[code] = promocode_data
                
                # Save archived promocodes
                with open(archive_file, "w", encoding="utf-8") as f:
                    json.dump(archived_promocodes, f, indent=4, sort_keys=True)
                
                log.info(f"Archived promocode {code} to {archive_file}")
            except Exception as e:
                log.warning(f"Error archiving promocode {code}: {e}")
        
        # Save changes to active promocodes
        if save_promocodes_to_file():
            log.info(f"Deleted promocode {code}")
            return True
        else:
            # If save failed, restore the promocode
            ACTIVE_PROMOCODES[code] = promocode_data
            log.error(f"Failed to save after deleting promocode {code}, restored to memory")
            return False
    except KeyError as e:
        log.error(f"Key error deleting promocode {code}: {e}")
        return False
    except Exception as e:
        log.error(f"Error deleting promocode {code}: {e}")
        return False

def mark_promocode_used(code: str, user_id: int, username: Optional[str] = None) -> bool:
    """
    Mark a promocode as used by a user with usage tracking
    
    Parameters
    ----------
    code: str
        The promocode to update
    user_id: int
        The user ID who used the code
    username: Optional[str]
        The username of the user who used the code (for tracking)
    
    Returns
    -------
    bool
        True if updated successfully, False otherwise
    """
    try:
        # Ensure promocodes are loaded
        if not ACTIVE_PROMOCODES:
            if not load_promocodes_from_file():
                log.error(f"Failed to load promocodes before marking {code} as used")
                return False
        
        # Normalize the code
        code = code.strip().upper()
        
        if code not in ACTIVE_PROMOCODES:
            log.warning(f"Promocode {code} not found when marking as used")
            return False
        
        # Get max_uses_per_user for this promocode
        max_uses_per_user = ACTIVE_PROMOCODES[code].get("max_uses_per_user", 1)
        
        # Check current usage count for this user
        current_usage = 0
        user_id_str = str(user_id)
        
        # Check usage_history if it exists
        if "usage_history" in ACTIVE_PROMOCODES[code] and user_id_str in ACTIVE_PROMOCODES[code]["usage_history"]:
            user_history = ACTIVE_PROMOCODES[code]["usage_history"][user_id_str]
            if isinstance(user_history, list):
                current_usage = len(user_history)
            else:
                # Single entry (old format)
                current_usage = 1
        # Also check used_by set
        elif "used_by" in ACTIVE_PROMOCODES[code] and user_id in ACTIVE_PROMOCODES[code]["used_by"]:
            current_usage = 1
        
        # Log detailed usage information
        log.info(f"Current usage for user {user_id} with promocode {code}: {current_usage}/{max_uses_per_user}")
        
        # Check if user has already reached max uses
        if current_usage >= max_uses_per_user:
            log.warning(f"User {user_id} has already used promocode {code} {current_usage} times (max: {max_uses_per_user})")
            return False
        
        # Update uses and mark as used by user
        ACTIVE_PROMOCODES[code]["uses_left"] -= 1
        
        # Ensure used_by is a set
        if "used_by" not in ACTIVE_PROMOCODES[code]:
            ACTIVE_PROMOCODES[code]["used_by"] = set()
        
        # Add user to used_by set
        ACTIVE_PROMOCODES[code]["used_by"].add(user_id)
        
        # Track usage with timestamp and username if provided
        usage_entry = {"timestamp": datetime.now(timezone.utc).isoformat()}
        if username:
            usage_entry["username"] = username
        
        # Initialize usage_history if it doesn't exist
        if "usage_history" not in ACTIVE_PROMOCODES[code]:
            ACTIVE_PROMOCODES[code]["usage_history"] = {}
        
        # Add usage entry to history (support multiple uses per user)
        user_id_str = str(user_id)
        if user_id_str not in ACTIVE_PROMOCODES[code]["usage_history"]:
            ACTIVE_PROMOCODES[code]["usage_history"][user_id_str] = []
        elif not isinstance(ACTIVE_PROMOCODES[code]["usage_history"][user_id_str], list):
            # Convert old single-entry format to list format
            old_entry = ACTIVE_PROMOCODES[code]["usage_history"][user_id_str]
            ACTIVE_PROMOCODES[code]["usage_history"][user_id_str] = [old_entry]
        
        ACTIVE_PROMOCODES[code]["usage_history"][user_id_str].append(usage_entry)
        
        # Update last_used timestamp
        ACTIVE_PROMOCODES[code]["last_used"] = datetime.now(timezone.utc)
        
        # Save changes
        if save_promocodes_to_file():
            log.info(f"Marked promocode {code} as used by user {user_id}, {ACTIVE_PROMOCODES[code]['uses_left']} uses left")
            return True
        else:
            # If save failed, revert the changes
            ACTIVE_PROMOCODES[code]["uses_left"] += 1
            ACTIVE_PROMOCODES[code]["used_by"].remove(user_id)
            if "usage_history" in ACTIVE_PROMOCODES[code] and str(user_id) in ACTIVE_PROMOCODES[code]["usage_history"]:
                user_history = ACTIVE_PROMOCODES[code]["usage_history"][str(user_id)]
                if isinstance(user_history, list) and len(user_history) > 0:
                    # Remove the last entry we just added
                    user_history.pop()
                    # If list is now empty, remove the user entry entirely
                    if len(user_history) == 0:
                        del ACTIVE_PROMOCODES[code]["usage_history"][str(user_id)]
                else:
                    # Old format or empty, just delete
                    del ACTIVE_PROMOCODES[code]["usage_history"][str(user_id)]
            if "last_used" in ACTIVE_PROMOCODES[code]:
                del ACTIVE_PROMOCODES[code]["last_used"]
            log.error(f"Failed to save after marking promocode {code} as used, reverted changes")
            return False
    except KeyError as e:
        log.error(f"Key error marking promocode {code} as used: {e}")
        return False
    except Exception as e:
        log.error(f"Error marking promocode {code} as used: {e}")
        return False

def get_active_promocodes(include_expired: bool = False, include_depleted: bool = False, 
                         include_hidden: bool = False, sort_by: Optional[str] = None,
                         filter_by_reward: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get active promocodes with filtering and sorting options
    
    Parameters
    ----------
    include_expired: bool
        Whether to include expired promocodes
    include_depleted: bool
        Whether to include promocodes with no uses left
    include_hidden: bool
        Whether to include hidden promocodes
    sort_by: Optional[str]
        Field to sort by ("expiry", "uses_left", "created_at", "code")
    filter_by_reward: Optional[Dict[str, Any]]
        Filter promocodes by reward type (e.g. {"specific_ball": 123} or {"special": 456})
    
    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary of promocodes
    """
    try:
        # Ensure promocodes are loaded
        if not ACTIVE_PROMOCODES:
            if not load_promocodes_from_file():
                log.error("Failed to load promocodes for listing")
                return {}
        
        now = datetime.now(timezone.utc)
        result = {}
        
        # Filter promocodes based on criteria
        for code, data in ACTIVE_PROMOCODES.items():
            try:
                # Skip if expired and not including expired
                if not include_expired:
                    # Handle invalid expiry format
                    if not isinstance(data.get("expiry"), datetime):
                        try:
                            if isinstance(data.get("expiry"), str):
                                data["expiry"] = datetime.fromisoformat(data["expiry"])
                                # Update in memory
                                ACTIVE_PROMOCODES[code]["expiry"] = data["expiry"]
                            else:
                                log.warning(f"Skipping promocode {code} with invalid expiry format")
                                continue
                        except (ValueError, TypeError):
                            log.warning(f"Skipping promocode {code} with invalid expiry format")
                            continue
                    
                    if now > data["expiry"]:
                        continue
                
                # Skip if depleted and not including depleted
                if not include_depleted:
                    # Handle invalid uses_left format
                    if not isinstance(data.get("uses_left"), int):
                        try:
                            data["uses_left"] = int(data["uses_left"])
                            # Update in memory
                            ACTIVE_PROMOCODES[code]["uses_left"] = data["uses_left"]
                        except (ValueError, TypeError):
                            log.warning(f"Skipping promocode {code} with invalid uses_left format")
                            continue
                    
                    if data["uses_left"] <= 0:
                        continue
                
                # Skip if hidden and not including hidden
                if not include_hidden and data.get("is_hidden", False):
                    continue
                
                # Filter by reward if specified
                if filter_by_reward:
                    rewards = data.get("rewards", {})
                    if not isinstance(rewards, dict):
                        log.warning(f"Skipping promocode {code} with invalid rewards format")
                        continue
                    
                    # Check if all filter criteria match
                    match = True
                    for key, value in filter_by_reward.items():
                        if key not in rewards or rewards[key] != value:
                            match = False
                            break
                    
                    if not match:
                        continue
                
                # Add to result if passed all filters
                result[code] = data
            except Exception as e:
                log.warning(f"Error processing promocode {code} during filtering: {e}")
                continue
        
        # Sort result if requested
        if sort_by and result:
            try:
                if sort_by == "code":
                    # Sort by code (dictionary key)
                    result = dict(sorted(result.items()))
                elif sort_by in ["expiry", "uses_left", "created_at"]:
                    # Sort by specified field
                    sorted_items = sorted(
                        result.items(),
                        key=lambda x: x[1].get(sort_by, datetime.max.replace(tzinfo=timezone.utc) if sort_by == "expiry" else 0)
                    )
                    result = {k: v for k, v in sorted_items}
            except Exception as e:
                log.warning(f"Error sorting promocodes by {sort_by}: {e}")
        
        return result
    except Exception as e:
        log.error(f"Error getting active promocodes: {e}")
        return {}

def is_valid_promocode(code: str, user_id: Optional[int] = None) -> Union[Dict[str, Any], bool]:
    """
    Check if a promocode is valid for a user with enhanced validation
    
    Parameters
    ----------
    code: str
        The promocode to check
    user_id: Optional[int]
        The user ID to check for, if None just checks if code exists
    
    Returns
    -------
    Union[Dict[str, Any], bool]
        Promocode data if valid, False otherwise
    """
    try:
        # Ensure promocodes are loaded
        if not ACTIVE_PROMOCODES:
            if not load_promocodes_from_file():
                log.error("Failed to load promocodes for validation")
                return False
        
        # Handle empty or None code
        if not code:
            log.warning("Empty promocode provided for validation")
            return False
        
        # Normalize the code
        original_code = code
        code = code.strip().upper() if code else ""
        
        log.info(f"Checking promocode validity: '{code}' (original: '{original_code}')")
        
        # Debug: Log all available promocodes
        log.debug(f"Available promocodes: {list(ACTIVE_PROMOCODES.keys())}")
        
        # Check if code exists in ACTIVE_PROMOCODES
        if code not in ACTIVE_PROMOCODES:
            log.warning(f"Promocode '{code}' not found in active promocodes dictionary")
            
            # Check for case sensitivity issues or whitespace
            for active_code in ACTIVE_PROMOCODES.keys():
                if active_code.lower() == code.lower():
                    log.warning(f"Found case-insensitive match: '{active_code}' vs '{code}'")
                    code = active_code  # Use the correctly cased version
                    break
                    
            # If still not found after case-insensitive check
            if code not in ACTIVE_PROMOCODES:
                # Find similar codes for debugging
                similar_codes = [c for c in ACTIVE_PROMOCODES.keys() 
                               if c.startswith(code[:1]) or 
                               any(char in c for char in code)]
                if similar_codes:
                    log.info(f"Similar promocodes found: {similar_codes}")
                return False
        
        # Get the promocode data
        data = ACTIVE_PROMOCODES[code]
        log.debug(f"Found promocode data for '{code}': {data}")
        
        # Validate data structure
        if not isinstance(data, dict):
            log.error(f"Promocode '{code}' has invalid data format: {type(data)}")
            return False
            
        # Check for required fields
        for field in ["expiry", "uses_left"]:
            if field not in data:
                log.error(f"Promocode '{code}' is missing required field: {field}")
                return False
        
        # Validate expiry date
        if not isinstance(data["expiry"], datetime):
            try:
                # Try to convert string to datetime if needed
                if isinstance(data["expiry"], str):
                    data["expiry"] = datetime.fromisoformat(data["expiry"])
                    # Update in memory
                    ACTIVE_PROMOCODES[code]["expiry"] = data["expiry"]
                    log.warning(f"Converted expiry string to datetime for promocode '{code}'")
                else:
                    log.error(f"Promocode '{code}' has invalid expiry format: {type(data['expiry'])}")
                    return False
            except (ValueError, TypeError) as e:
                log.error(f"Failed to convert expiry date for promocode '{code}': {e}")
                return False
            
        now = datetime.now(timezone.utc)
        
        # Check if promocode has expired
        if now > data["expiry"]:
            log.info(f"Promocode '{code}' has expired. Current time: {now}, Expiry: {data['expiry']}")
            return False
        
        # Check if promocode has reached its maximum uses
        if not isinstance(data["uses_left"], int):
            try:
                data["uses_left"] = int(data["uses_left"])
                # Update in memory
                ACTIVE_PROMOCODES[code]["uses_left"] = data["uses_left"]
                log.warning(f"Converted uses_left to integer for promocode '{code}'")
            except (ValueError, TypeError):
                log.error(f"Promocode '{code}' has invalid uses_left format: {type(data['uses_left'])}")
                return False
            
        if data["uses_left"] <= 0:
            log.info(f"Promocode '{code}' has no uses left: {data['uses_left']}")
            return False
        
        # Check if promocode is hidden
        if data.get("is_hidden", False) and not user_id:
            log.info(f"Promocode '{code}' is hidden and requires a user ID for validation")
            return False
        
        # If user_id is provided, check if user has already used this promocode
        if user_id is not None:
            max_uses_per_user = data.get("max_uses_per_user", 1)  # Default to 1 if not specified
            
            if not isinstance(max_uses_per_user, int):
                try:
                    max_uses_per_user = int(max_uses_per_user)
                    # Update in memory
                    ACTIVE_PROMOCODES[code]["max_uses_per_user"] = max_uses_per_user
                    log.warning(f"Converted max_uses_per_user to integer for promocode '{code}'")
                except (ValueError, TypeError):
                    log.error(f"Promocode '{code}' has invalid max_uses_per_user format: {type(max_uses_per_user)}")
                    return False
                
            if max_uses_per_user > 0:
                used_by = data.get("used_by", set())
                
                # Ensure used_by is a set
                if not isinstance(used_by, set):
                    try:
                        used_by = set(used_by) if used_by else set()
                        log.warning(f"Converted used_by to set for promocode '{code}'")
                        # Update the promocode data with the converted set
                        ACTIVE_PROMOCODES[code]["used_by"] = used_by
                    except Exception as e:
                        log.error(f"Failed to convert used_by to set for promocode '{code}': {e}")
                        return False
                
                # Check how many times the user has used this promocode
                usage_history = data.get("usage_history", {})
                user_usage_count = 0
                
                # Count usage from history if available
                if usage_history and str(user_id) in usage_history:
                    # If usage_history contains a list of usage entries, count them
                    user_history = usage_history[str(user_id)]
                    if isinstance(user_history, list):
                        user_usage_count = len(user_history)
                    else:
                        # Single usage entry (old format)
                        user_usage_count = 1
                elif user_id in used_by:
                    # Fallback: if user is in used_by but no detailed history, assume 1 use
                    user_usage_count = 1
                
                if user_usage_count >= max_uses_per_user:
                    log.info(f"User {user_id} has already used promocode '{code}' {user_usage_count} times (max: {max_uses_per_user})")
                    return False
        
        # Check if rewards are valid
        if "rewards" not in data or not isinstance(data["rewards"], dict):
            log.warning(f"Promocode '{code}' has invalid rewards format, using default")
            # Set default rewards
            data["rewards"] = {"specific_ball": None, "special": None}
            # Update in memory
            ACTIVE_PROMOCODES[code]["rewards"] = data["rewards"]
        
        log.info(f"Promocode '{code}' is valid for user {user_id if user_id else 'None'}")
        return data
    except KeyError as e:
        log.error(f"Key error checking promocode '{code}': {e}")
        return False
    except Exception as e:
        log.exception(f"Error checking promocode '{code}': {e}")
        return False

def clean_expired_promocodes(archive: bool = True) -> int:
    """
    Remove expired promocodes from memory and optionally archive them
    
    Parameters
    ----------
    archive: bool
        Whether to archive expired promocodes instead of permanently deleting them
    
    Returns
    -------
    int
        Number of removed promocodes
    
    Raises
    ------
    PermissionError
        If there's a permission issue with the promocode file
    OSError
        If there's a file system error
    json.JSONDecodeError
        If there's an issue with JSON formatting
    Exception
        For any other unexpected errors
    """
    # Ensure promocodes are loaded
    if not ACTIVE_PROMOCODES:
        if not load_promocodes_from_file():
            log.error("Failed to load promocodes for cleaning")
            raise OSError("Failed to load promocodes from file")
    
    now = datetime.now(timezone.utc)
    expired_codes = []
    archived_data = {}
    
    # Check each promocode with proper error handling
    for code, data in list(ACTIVE_PROMOCODES.items()):
        try:
            # Skip default promocodes
            if code in ["WELCOMETONATIONDEX", "WELCOMETOPLEASUREDOME"]:
                continue
                
            # Validate expiry date
            if not isinstance(data, dict):
                log.warning(f"Invalid data format for promocode {code}, removing")
                expired_codes.append(code)
                continue
                
            if "expiry" not in data:
                log.warning(f"Missing expiry date for promocode {code}, removing")
                expired_codes.append(code)
                continue
                
            expiry = data["expiry"]
            if not isinstance(expiry, datetime):
                log.warning(f"Invalid expiry date format for promocode {code}, removing")
                expired_codes.append(code)
                continue
                
            # Check if expired
            if now > expiry:
                log.info(f"Promocode {code} expired on {expiry.strftime('%Y-%m-%d')}, removing")
                expired_codes.append(code)
                
                # Store for archiving if requested
                if archive:
                    archived_copy = data.copy()
                    archived_copy["deleted_at"] = now.isoformat()
                    archived_copy["deletion_reason"] = "expired"
                    
                    # Convert sets to lists for JSON serialization
                    if "used_by" in archived_copy and isinstance(archived_copy["used_by"], set):
                        archived_copy["used_by"] = list(archived_copy["used_by"])
                    
                    archived_data[code] = archived_copy
                continue
                
            # Check uses left
            if "uses_left" not in data:
                log.warning(f"Missing uses_left for promocode {code}, removing")
                expired_codes.append(code)
                continue
                
            uses_left = data["uses_left"]
            if not isinstance(uses_left, int):
                log.warning(f"Invalid uses_left format for promocode {code}, removing")
                expired_codes.append(code)
                continue
                
            if uses_left <= 0:
                log.info(f"Promocode {code} has no uses left, removing")
                expired_codes.append(code)
                
                # Store for archiving if requested
                if archive:
                    archived_copy = data.copy()
                    archived_copy["deleted_at"] = now.isoformat()
                    archived_copy["deletion_reason"] = "depleted"
                    
                    # Convert sets to lists for JSON serialization
                    if "used_by" in archived_copy and isinstance(archived_copy["used_by"], set):
                        archived_copy["used_by"] = list(archived_copy["used_by"])
                    
                    archived_data[code] = archived_copy
                continue
        except Exception as e:
            log.error(f"Error checking promocode {code}: {e}")
            # Add to expired codes to remove problematic entries
            expired_codes.append(code)
    
    # If no expired codes, we're done
    if not expired_codes:
        log.info("No expired promocodes to clean")
        return 0
        
    # Archive expired codes if requested
    if archive and archived_data:
        try:
            # Create archive directory if it doesn't exist
            archive_dir = os.path.join(os.path.dirname(PROMOCODES_FILE_PATH), "archived_promocodes")
            os.makedirs(archive_dir, exist_ok=True)
            
            # Archive file path
            archive_file = os.path.join(archive_dir, f"archived_promocodes_{now.strftime('%Y%m%d')}.json")
            
            # Load existing archived promocodes if file exists
            existing_archived = {}
            if os.path.exists(archive_file):
                try:
                    with open(archive_file, "r", encoding="utf-8") as f:
                        existing_archived = json.load(f)
                except json.JSONDecodeError as e:
                    log.warning(f"Archive file exists but is not valid JSON, creating new archive: {e}")
                except PermissionError as e:
                    log.error(f"Permission error reading archive file: {e}")
                    raise
                except OSError as e:
                    log.error(f"OS error reading archive file: {e}")
                    raise
                except Exception as e:
                    log.warning(f"Error reading archive file: {e}, creating new archive")
            
            # Merge with new archived data
            existing_archived.update(archived_data)
            
            # Save to archive file
            try:
                with open(archive_file, "w", encoding="utf-8") as f:
                    json.dump(existing_archived, f, indent=4)
                
                log.info(f"Archived {len(archived_data)} expired promocodes to {archive_file}")
            except PermissionError as e:
                log.error(f"Permission error writing to archive file: {e}")
                raise
            except OSError as e:
                log.error(f"OS error writing to archive file: {e}")
                raise
        except (PermissionError, OSError) as e:
            log.error(f"Error archiving expired promocodes: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error archiving expired promocodes: {e}")
            # Continue with deletion even if archiving fails
    
    # Remove expired codes
    removed_count = 0
    for code in expired_codes:
        try:
            del ACTIVE_PROMOCODES[code]
            removed_count += 1
            log.debug(f"Removed expired promocode: {code}")
        except Exception as e:
            log.error(f"Error removing promocode {code}: {e}")
    
    # Save changes to file if any codes were removed
    if removed_count > 0:
        try:
            if not save_promocodes_to_file():
                log.error("Failed to save promocodes after cleaning expired codes")
                raise OSError("Failed to save promocodes to file after cleaning")
        except (PermissionError, OSError) as e:
            log.error(f"File error saving promocodes after cleaning: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error saving promocodes after cleaning: {e}")
            raise
    
    return removed_count
