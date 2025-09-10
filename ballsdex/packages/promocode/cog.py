import discord
import logging
import random
import asyncio
import time
from datetime import datetime, timezone, timedelta
from discord import app_commands
from discord.ext import commands
from ballsdex.settings import settings
from ballsdex.core.utils.paginator import FieldPageSource, Pages
from ballsdex.core.models import Ball, BallInstance, Player, Special, balls, specials
from ballsdex.packages.promocode.active import (
    load_promocodes_from_file, is_valid_promocode, mark_promocode_used,
    get_active_promocodes, ACTIVE_PROMOCODES, clean_expired_promocodes
)

from typing import TYPE_CHECKING, Optional, Dict, List, Union, Set, Tuple, Any, cast

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot

log = logging.getLogger("ballsdex.packages.promocode")

class PromocodeModal(discord.ui.Modal):
    code = discord.ui.TextInput(
        label="Enter your promocode",
        placeholder="Enter promocode here...",
        required=True,
        min_length=3,  # Require at least 3 characters
        max_length=50
    )

    def __init__(self, cog):
        super().__init__(title=f"Redeem {settings.bot_name} Promocode")
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Basic validation
            promocode = self.code.value.strip() if self.code.value else ""
            
            if not promocode:
                await interaction.response.send_message(
                    "❌ Promocode cannot be empty. Please try again.",
                    ephemeral=True
                )
                return
                
            # Convert to uppercase for consistency
            promocode = promocode.upper()
            
            # Check for invalid characters (only allow alphanumeric and some special chars)
            valid_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
            if not all(c in valid_chars for c in promocode):
                await interaction.response.send_message(
                    "❌ Promocode contains invalid characters. Please use only letters, numbers, underscores, and hyphens.",
                    ephemeral=True
                )
                return
                
            # Process the promocode
            await self.cog.process_promocode(interaction, promocode)
        except Exception as e:
            log.exception(f"Error in promocode modal submission: {e}")
            try:
                await interaction.response.send_message(
                    "❌ An error occurred while processing your promocode. Please try again later.",
                    ephemeral=True
                )
            except Exception:
                # If we can't respond, the interaction might have already been responded to
                pass

class Promocode(commands.Cog):
    """
    Promocode management commands.
    """

    def __init__(self, bot: "BallsDexBot"):
        self.bot = bot
        self.ball_loaded = asyncio.Event()
        # Schedule task to initialize promocodes once balls are loaded
        self.bot.loop.create_task(self.initialize_promocodes())
        
    async def initialize_promocodes(self):
        """Initialize promocodes once collectibles are loaded"""
        await self.bot.wait_until_ready()
        
        log.info(f"Initializing promocode system...")
        
        # Try to load promocodes from file first
        try:
            log.info(f"Attempting to load promocodes from file...")
            success = load_promocodes_from_file()
            
            if success:
                log.info(f"Successfully loaded promocodes from file")
                # Log the loaded promocodes for debugging
                log.info(f"Loaded promocodes: {list(ACTIVE_PROMOCODES.keys())}")
            else:
                log.warning(f"Failed to load promocodes from file. Using default in-memory codes.")
                # We'll continue with default codes in memory
        except Exception as e:
            log.exception(f"Error loading promocodes from file: {e}")
            # We'll continue with default codes in memory
        
        # Verify ACTIVE_PROMOCODES has expected content
        if not ACTIVE_PROMOCODES:
            log.error(f"ACTIVE_PROMOCODES dictionary is empty after initialization!")
        else:
            log.info(f"ACTIVE_PROMOCODES contains {len(ACTIVE_PROMOCODES)} codes after initialization")
            
        # Check for specific promocodes
        for expected_code in ["WELCOMETONATIONDEX", "WELCOMETOPLEASUREDOME"]:
            if expected_code in ACTIVE_PROMOCODES:
                log.info(f"Found expected promocode: {expected_code}")
            else:
                log.warning(f"Expected promocode not found: {expected_code}")
        
        log.info(f"Waiting for {settings.collectible_name} data to load...")
        
        # Make sure balls are loaded
        retry_count = 0
        max_retries = 30  # Wait longer to ensure balls are loaded
        
        while not balls and retry_count < max_retries:
            await asyncio.sleep(1)
            retry_count += 1
            log.debug(f"Waiting for {settings.collectible_name} data, attempt {retry_count}/{max_retries}")
            
        if not balls:
            log.warning(f"{settings.collectible_name.capitalize()} data not loaded after waiting, promocodes may not work correctly")
            # Set the ball_loaded event anyway to avoid blocking the bot
            self.ball_loaded.set()
            return
        else:
            log.info(f"Successfully loaded {len(balls)} {settings.collectible_name} items")
            
        # Set the ball_loaded event
        self.ball_loaded.set()
        
        # Get counts for logging
        try:
            all_promocodes = get_active_promocodes(include_expired=True)
            active_promocodes = get_active_promocodes(include_expired=False)
            log.info(f"Promocode system initialized with {len(active_promocodes)} active codes out of {len(all_promocodes)} total")
            
            # Clean expired promocodes
            cleaned = clean_expired_promocodes()
            if cleaned:
                log.info(f"Cleaned {cleaned} expired promocodes")
        except Exception as e:
            log.exception(f"Error getting promocode counts: {e}")
            log.info(f"Promocode system initialized with errors. Some features may not work correctly.")
            
        # Force reload one more time to ensure we have the latest data
        try:
            log.info(f"Performing final promocode reload to ensure latest data...")
            load_promocodes_from_file()
            log.info(f"Final promocode list: {list(ACTIVE_PROMOCODES.keys())}")
        except Exception as e:
            log.error(f"Error in final promocode reload: {e}")

    @app_commands.command(name="promocode_redeem")
    @app_commands.checks.cooldown(1, 10, key=lambda i: str(i.user.id))  # Standard cooldown for consistency
    async def promocode_redeem(self, interaction: discord.Interaction):
        """
        Redeem a promocode to get special rewards!

        Parameters
        ----------
        interaction : discord.Interaction
            The interaction object representing the user's command invocation.

        Behavior
        --------
        Presents a modal for the user to enter a promocode, checks if the collectible data is loaded,
        and handles the redemption process. Responds with an ephemeral message if the system is not ready.
        """
        if not self.ball_loaded.is_set():
            await interaction.response.send_message(
                f"❌ {settings.bot_name} is still initializing {settings.collectible_name} data. Please try again in a few moments.",
                ephemeral=True
            )
            return
            
        # Create a dynamic modal with the correct collectible name
        modal = PromocodeModal(self)
        await interaction.response.send_modal(modal)

    async def process_promocode(self, interaction: discord.Interaction, code: str):
        """Process the submitted promocode and grant rewards if valid"""
        user_id = interaction.user.id
        log.info(f"User {user_id} attempting to redeem promocode")
        
        # Normalize the code
        try:
            code = code.strip().upper() if code else ""
            if not code:
                await interaction.response.send_message("❌ Promocode cannot be empty. Please enter a valid code.", ephemeral=True)
                log.warning(f"User {user_id} attempted to use empty promocode")
                return
        except Exception as e:
            await interaction.response.send_message("❌ Invalid promocode format. Please try again.", ephemeral=True)
            log.error(f"Error normalizing promocode for user {user_id}: {e}")
            return
        
        # Check if system is initialized
        if not self.ball_loaded.is_set():
            await interaction.response.send_message(
                f"❌ {settings.bot_name} is still initializing {settings.collectible_name} data. Please try again in a few moments.",
                ephemeral=True
            )
            log.warning(f"User {user_id} attempted to use promocode but {settings.collectible_name} data not loaded")
            return
            
        if not balls:
            await interaction.response.send_message(
                f"❌ {settings.collectible_name.capitalize()} data is not available. Please try again later or contact an administrator.",
                ephemeral=True
            )
            log.error(f"User {user_id} attempted to use promocode but {settings.collectible_name} data is empty")
            return
        
        # Get data for this promocode
        if code not in ACTIVE_PROMOCODES:
            await interaction.response.send_message(
                f"❌ Invalid promocode '{code}'. Please check and try again.", 
                ephemeral=True
            )
            log.info(f"User {user_id} attempted to use invalid promocode: {code}")
            return
            
        try:
            code_data = ACTIVE_PROMOCODES[code]
            
            # Validate code data structure
            if not isinstance(code_data, dict):
                await interaction.response.send_message("❌ Invalid promocode data format. Please contact an administrator.", ephemeral=True)
                log.error(f"Promocode {code} has invalid data format: {type(code_data)}")
                return
                
            # Ensure required fields exist
            for field in ["expiry", "uses_left"]:
                if field not in code_data:
                    await interaction.response.send_message("❌ Invalid promocode data. Please contact an administrator.", ephemeral=True)
                    log.error(f"Promocode {code} is missing required field: {field}")
                    return
                    
            # Ensure used_by is a set
            if "used_by" in code_data and not isinstance(code_data["used_by"], set):
                try:
                    code_data["used_by"] = set(code_data["used_by"])
                    log.warning(f"Converted used_by to set for promocode {code}")
                except Exception as e:
                    log.error(f"Failed to convert used_by to set for promocode {code}: {e}")
                    # Continue with original value
        except Exception as e:
            await interaction.response.send_message("❌ Error accessing promocode data. Please try again later.", ephemeral=True)
            log.exception(f"Error accessing data for promocode {code}: {e}")
            return
        
        # Check if promocode is valid using the is_valid_promocode function
        log.info(f"Calling is_valid_promocode for code '{code}' and user {user_id}")
        
        # Force reload promocodes from file to ensure we have the latest data
        try:
            load_promocodes_from_file()
            log.info(f"Reloaded promocodes from file before validation")
            log.info(f"After reload, available promocodes: {list(ACTIVE_PROMOCODES.keys())}")
        except Exception as e:
            log.error(f"Failed to reload promocodes from file: {e}")
            # Continue with current data
        
        # Double-check if the code exists in ACTIVE_PROMOCODES after reload
        if code not in ACTIVE_PROMOCODES:
            log.warning(f"After reload, promocode '{code}' still not found in ACTIVE_PROMOCODES")
            # Try case-insensitive search
            for active_code in ACTIVE_PROMOCODES.keys():
                if active_code.lower() == code.lower():
                    log.info(f"Found case-insensitive match after reload: '{active_code}' vs '{code}'")
                    code = active_code  # Use the correctly cased version
                    code_data = ACTIVE_PROMOCODES[code]  # Update code_data with the correct case
                    break
        
        # Validate promocode
        valid_code_data = is_valid_promocode(code, user_id)
        if not valid_code_data:
            try:
                log.warning(f"Promocode '{code}' validation failed for user {user_id}")
                
                # Check if code exists in ACTIVE_PROMOCODES
                if code not in ACTIVE_PROMOCODES:
                    log.warning(f"Promocode '{code}' not found in ACTIVE_PROMOCODES during validation")
                    await interaction.response.send_message(
                        f"❌ Invalid promocode '{code}'. Please check and try again.", 
                        ephemeral=True
                    )
                    log.info(f"User {user_id} attempted to use invalid promocode: {code}")
                    return
                
                # Check if code has expired
                current_time = datetime.now(timezone.utc)
                
                # Ensure expiry is a datetime object
                if not isinstance(code_data["expiry"], datetime):
                    log.error(f"Expiry date for promocode '{code}' is not a datetime object: {type(code_data['expiry'])}")
                    await interaction.response.send_message("❌ Invalid promocode expiry format. Please contact an administrator.", ephemeral=True)
                    return
                    
                if current_time > code_data["expiry"]:
                    # Calculate how long ago it expired
                    expired_ago = current_time - code_data["expiry"]
                    days_expired = expired_ago.days
                    
                    if days_expired < 1:
                        expire_text = "today"
                    elif days_expired == 1:
                        expire_text = "yesterday"
                    else:
                        expire_text = f"{days_expired} days ago"
                        
                    await interaction.response.send_message(
                        f"❌ This promocode expired {expire_text}.", 
                        ephemeral=True
                    )
                    log.info(f"User {user_id} attempted to use expired promocode '{code}' (expired {expire_text})")
                    return
                    
                # Check if code has no uses left
                if code_data["uses_left"] <= 0:
                    await interaction.response.send_message(
                        "❌ This promocode has reached its usage limit.", 
                        ephemeral=True
                    )
                    log.info(f"User {user_id} attempted to use promocode '{code}' with no uses left")
                    return
                    
                # Check if user has already used this promocode
                max_uses_per_user = code_data.get("max_uses_per_user", 0)
                used_by = code_data.get("used_by", set())
                
                # Ensure used_by is a set
                if not isinstance(used_by, set):
                    try:
                        used_by = set(used_by) if used_by else set()
                        log.warning(f"Converted used_by to set for promocode '{code}' in process_promocode")
                        # Update the promocode data with the converted set
                        ACTIVE_PROMOCODES[code]["used_by"] = used_by
                        code_data["used_by"] = used_by
                    except Exception as e:
                        log.error(f"Failed to convert used_by to set for promocode '{code}' in process_promocode: {e}")
                
                if max_uses_per_user > 0 and user_id in used_by:
                    await interaction.response.send_message(
                        "❌ You have already used this promocode.", 
                        ephemeral=True
                    )
                    log.info(f"User {user_id} attempted to reuse promocode '{code}'")
                    return
                    
                # Generic error if we can't determine the specific issue
                log.warning(f"Promocode '{code}' failed validation for user {user_id} for unknown reason")
                await interaction.response.send_message("❌ Invalid promocode. Please check and try again.", ephemeral=True)
                return
            except Exception as e:
                log.exception(f"Error validating promocode '{code}' for user {user_id}: {e}")
                await interaction.response.send_message("❌ Error validating promocode. Please try again later.", ephemeral=True)
                return
        
        # Defer response while we process the database operations
        try:
            await interaction.response.defer(ephemeral=True)
        except Exception as e:
            log.error(f"Failed to defer response for user {user_id}: {e}")
            # Continue anyway, we'll try to use followup
        
        try:
            # Get the player instance
            try:
                player, _created = await Player.get_or_create(discord_id=user_id)
                if _created:
                    log.info(f"Created new player record for user {user_id}")
            except Exception as e:
                log.exception(f"Failed to create or get player for user {user_id}: {e}")
                raise ValueError(f"Could not create or retrieve player record: {e}")
            
            # Process rewards
            if "rewards" not in code_data or not isinstance(code_data["rewards"], dict):
                log.error(f"Promocode {code} has invalid or missing rewards data: {code_data.get('rewards', 'MISSING')}")
                raise ValueError(f"Invalid rewards data for promocode {code}")
                
            rewards = code_data["rewards"]
            rewards_text = []
            
            # Handle collectible reward
            if "specific_ball" in rewards:
                specific_ball_id = rewards["specific_ball"]
                ball_info = None
                special_event = None
                
                # Check for special event
                if "special" in rewards and rewards["special"] is not None:
                    try:
                        special_id = rewards["special"]
                        if special_id in specials:
                            special_event = specials[special_id]
                            log.info(f"Using special event ID {special_id} for promocode {code}")
                        else:
                            log.warning(f"Special event ID {special_id} not found for promocode {code}")
                    except Exception as e:
                        log.error(f"Error processing special event for promocode {code}: {e}")
                        # Continue without special event
                
                # Give specific collectible if specified, otherwise random one
                if specific_ball_id is not None:
                    # Specific collectible
                    try:
                        if specific_ball_id in balls:
                            ball_info = balls[specific_ball_id]
                            log.info(f"Using specific {settings.collectible_name} ID {specific_ball_id} for promocode {code}")
                        else:
                            # Fallback to random if specific collectible not found
                            log.warning(f"{settings.collectible_name.capitalize()} ID {specific_ball_id} not found for promocode {code}, using random {settings.collectible_name}")
                            enabled_balls = [b for b in balls.values() if getattr(b, 'enabled', True)]
                            if enabled_balls:
                                ball_info = random.choice(enabled_balls)
                                log.info(f"Fallback to random {settings.collectible_name} for promocode {code}")
                            else:
                                log.error(f"No enabled {settings.plural_collectible_name} found for promocode {code}")
                                raise ValueError(f"No enabled {settings.plural_collectible_name} available")
                    except Exception as e:
                        if isinstance(e, ValueError) and "No enabled" in str(e):
                            raise  # Re-raise our specific error
                        log.exception(f"Error processing specific {settings.collectible_name} ID {specific_ball_id} for promocode {code}: {e}")
                        # Try fallback to random
                        try:
                            enabled_balls = [b for b in balls.values() if getattr(b, 'enabled', True)]
                            if enabled_balls:
                                ball_info = random.choice(enabled_balls)
                                log.info(f"Fallback to random {settings.collectible_name} after error for promocode {code}")
                            else:
                                log.error(f"No enabled {settings.plural_collectible_name} found for promocode {code}")
                                raise ValueError(f"No enabled {settings.plural_collectible_name} available")
                        except Exception as fallback_error:
                            log.exception(f"Error in fallback random selection: {fallback_error}")
                            raise ValueError(f"Could not select a {settings.collectible_name} for the reward")
                else:
                    # Random collectible
                    try:
                        enabled_balls = [b for b in balls.values() if getattr(b, 'enabled', True)]
                        if enabled_balls:
                            ball_info = random.choice(enabled_balls)
                            log.info(f"Using random {settings.collectible_name} for promocode {code}")
                        else:
                            log.error(f"No enabled {settings.plural_collectible_name} found for promocode {code}")
                            raise ValueError(f"No enabled {settings.plural_collectible_name} available")
                    except Exception as e:
                        log.exception(f"Error selecting random {settings.collectible_name} for promocode {code}: {e}")
                        raise ValueError(f"Could not select a random {settings.collectible_name}")
                
                if ball_info:
                    try:
                        # Create collectible instance for player
                        ball_instance = await BallInstance.create(
                            ball=ball_info,
                            player=player,
                            special=special_event,
                            catch_date=datetime.now(timezone.utc),
                        )
                        
                        # Format reward text with proper collectible name
                        ball_name = f"**{ball_info.country}**"
                        if special_event:
                            ball_name += f" ({special_event.name})"
                            
                        rewards_text.append(f"1x {ball_name} {settings.collectible_name}")
                        log.info(f"User {user_id} received {ball_info.country} {settings.collectible_name} from promocode {code}")
                    except Exception as e:
                        log.exception(f"Failed to create {settings.collectible_name} instance for user {user_id}: {e}")
                        rewards_text.append(f"Error giving {settings.collectible_name}! Please contact an administrator.")
                        raise ValueError(f"Failed to create {settings.collectible_name} instance: {e}")
                else:
                    log.error(f"No valid {settings.collectible_name} found for promocode {code}")
                    rewards_text.append(f"Error: No valid {settings.plural_collectible_name} found!")
                    raise ValueError(f"No valid {settings.collectible_name} found for reward")
            else:
                log.warning(f"Promocode {code} has no specific_ball reward defined")
                rewards_text.append(f"Error: No {settings.collectible_name} reward defined for this promocode!")
                raise ValueError(f"No {settings.collectible_name} reward defined for this promocode")
            
            # Mark code as used
            try:
                if not mark_promocode_used(code, user_id):
                    log.warning(f"Failed to mark promocode {code} as used by user {user_id}")
                    # Continue anyway, user already got the reward
            except Exception as e:
                log.error(f"Error marking promocode {code} as used: {e}")
                # Continue anyway, user already got the reward
            
            if not rewards_text:
                rewards_text.append(f"Error: No valid {settings.plural_collectible_name} found!")
            
            # Calculate uses left for informational purposes
            try:
                uses_left = code_data["uses_left"]
                max_uses_per_user = code_data.get("max_uses_per_user", 0)
                
                # Format expiry date
                try:
                    expiry_date = code_data["expiry"].strftime('%Y-%m-%d')
                except (AttributeError, ValueError) as e:
                    log.error(f"Error formatting expiry date for promocode {code}: {e}")
                    expiry_date = "Unknown"
            except Exception as e:
                log.error(f"Error getting promocode details for display: {e}")
                uses_left = "Unknown"
                max_uses_per_user = 0
                expiry_date = "Unknown"
            
            # Create embed for better visual presentation
            embed = discord.Embed(
                title=f"✅ {settings.bot_name} Promocode Redeemed!",
                description=f"Successfully redeemed promocode **{code}**!",
                color=discord.Color.green()
            )
            
            # Add reward info
            embed.add_field(name="You received", value=', '.join(rewards_text), inline=False)
            
            # Add promocode details
            if max_uses_per_user > 0:
                embed.add_field(name="Note", value=f"This promocode can only be used once per user.", inline=False)
            
            # Footer with expiry info
            embed.set_footer(text=f"Code expires on {expiry_date} • {uses_left} uses remaining")
            
            # Send success message
            try:
                await interaction.followup.send(embed=embed, ephemeral=True)
                log.info(f"Successfully sent promocode redemption confirmation to user {user_id}")
            except Exception as e:
                log.error(f"Failed to send success message to user {user_id}: {e}")
                # Try a simpler message as fallback
                try:
                    await interaction.followup.send(f"✅ Successfully redeemed promocode {code}!", ephemeral=True)
                except Exception as followup_error:
                    log.exception(f"Failed to send fallback success message: {followup_error}")
        except Exception as e:
            log.exception(f"Error processing promocode {code} for user {user_id}: {e}")
            
            # Create error embed with more specific information
            error_message = "An error occurred while processing your promocode."
            if isinstance(e, ValueError):
                # More specific error message for ValueError
                error_message = str(e)
            
            error_embed = discord.Embed(
                title="❌ Error Processing Promocode",
                description=f"{error_message} Please try again later or contact an administrator.",
                color=discord.Color.red()
            )
            
            # Add error ID for tracking
            error_id = datetime.now().strftime('%Y%m%d%H%M%S')
            error_embed.add_field(name="Error Details", value=f"Error ID: {error_id}", inline=False)
            log.error(f"Promocode error ID {error_id} for user {user_id}: {e}")
            
            # Try to send error message, but handle any further errors
            try:
                await interaction.followup.send(embed=error_embed, ephemeral=True)
            except Exception as followup_error:
                log.exception(f"Failed to send error message to user {user_id}: {followup_error}")
                # Try a simpler message as fallback
                try:
                    await interaction.followup.send(
                        "❌ An error occurred while processing your promocode. Please try again later.",
                        ephemeral=True
                    )
                except Exception as simple_error:
                    log.exception(f"Failed to send fallback error message: {simple_error}")
                    # At this point, we've tried everything we can to notify the user
