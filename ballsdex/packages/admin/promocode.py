import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Literal, Optional, cast

import discord
from discord import app_commands

from ballsdex.core.models import balls, specials
from ballsdex.core.utils.paginator import FieldPageSource, Pages

# Import the promocode active module
from ballsdex.packages.arampacks.active import (
    ACTIVE_PROMOCODES,
    PROMOCODES_FILE_PATH,
    clean_expired_promocodes,
    create_promocode,
    delete_promocode,
    get_active_promocodes,
    load_promocodes_from_file,
    save_promocodes_to_file,
    update_promocode_uses,
)
from ballsdex.settings import settings

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot
else:
    BallsDexBot = None

log = logging.getLogger("ballsdex.packages.admin.promocode")


# Define autocomplete functions outside the class
async def ball_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[int]]:
    """Autocomplete for ball/collectible selection"""
    # If balls aren't loaded yet, return empty list
    if not balls:
        return []

    ball_list = list(balls.values())
    # Try to filter by ball attributes that match the search
    return [
        app_commands.Choice(name=f"{ball.country} ({ball.pk})", value=ball.pk)  # type: ignore
        for ball in ball_list
        if current.lower() in ball.country.lower()
        or (hasattr(ball, "emoji_id") and current.lower() in str(ball.emoji_id).lower())
        or current in str(ball.pk)  # type: ignore
    ][:25]


async def special_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[int]]:
    """Autocomplete for special event selection"""
    # If specials aren't loaded yet, return empty list
    if not specials:
        return []

    special_list = list(specials.values())
    return [
        app_commands.Choice(
            name=f"{special.name} ({special.pk})", value=special.pk  # type: ignore
        )
        for special in special_list
        if current.lower() in special.name.lower() or current in str(special.pk)  # type: ignore
    ][:25]


class Promocode(app_commands.Group):
    """
    Promocode management commands
    """

    def __init__(self):
        # Use explicit name without any suffix - ensure it matches what Discord expects
        super().__init__(name="promocode", description="Admin commands for promocode management")

    @app_commands.command(name="sync")
    @app_commands.default_permissions(administrator=True)
    async def promocode_sync(self, interaction: discord.Interaction):
        """
        Syncs the promocode database with the file, ensuring no data loss.
        """
        if (
            not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command.", ephemeral=True
            )
            return

        # Defer response while we load the database
        await interaction.response.defer(ephemeral=True)

        try:
            # Check if the file exists first
            if not os.path.exists(PROMOCODES_FILE_PATH):
                # Create a default file if it doesn't exist
                log.warning(
                    f"Promocode file not found at {PROMOCODES_FILE_PATH}. Creating a new file."
                )
                if save_promocodes_to_file():
                    await interaction.followup.send(
                        f"✅ Created new promocode file at {PROMOCODES_FILE_PATH}.", ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        (
                            f"❌ Failed to create promocode file at "
                            f"{PROMOCODES_FILE_PATH}. Check permissions."
                        ),
                        ephemeral=True,
                    )
                    return

            # Check if the file is readable
            if not os.access(PROMOCODES_FILE_PATH, os.R_OK):
                await interaction.followup.send(
                    (
                        f"❌ Cannot read promocode file at "
                        f"{PROMOCODES_FILE_PATH}. Check file permissions."
                    ),
                    ephemeral=True,
                )
                return

            # Load promocodes from file
            if load_promocodes_from_file():
                # Get counts for informational purposes
                all_promocodes = get_active_promocodes(include_expired=True)
                active_promocodes = get_active_promocodes(include_expired=False)

                # Create embed for better visual presentation
                embed = discord.Embed(
                    title="✅ Promocode Database Synced",
                    description="Successfully synced promocode database with file.",
                    color=discord.Color.green(),
                )
                embed.add_field(
                    name="Active Promocodes", value=str(len(active_promocodes)), inline=True
                )
                embed.add_field(
                    name="Total Promocodes", value=str(len(all_promocodes)), inline=True
                )
                embed.add_field(
                    name="File Location", value=f"`{PROMOCODES_FILE_PATH}`", inline=False
                )

                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                # Create error embed
                embed = discord.Embed(
                    title="❌ Sync Failed",
                    description="Failed to sync promocode database with file.",
                    color=discord.Color.red(),
                )
                embed.add_field(
                    name="File Location", value=f"`{PROMOCODES_FILE_PATH}`", inline=False
                )
                embed.add_field(
                    name="Troubleshooting",
                    value="Check the logs for detailed error information.",
                    inline=False,
                )

                await interaction.followup.send(embed=embed, ephemeral=True)
        except FileNotFoundError:
            log.error("Promocode file not found")
            await interaction.followup.send(
                "❌ Promocode file not found. The system will use default in-memory codes.",
                ephemeral=True,
            )
        except json.JSONDecodeError as e:
            log.error(f"Error decoding JSON from promocode file: {e}")
            await interaction.followup.send(
                f"❌ Invalid promocode file format. Error: {str(e)[:100]}", ephemeral=True
            )
        except PermissionError as e:
            log.error(f"Permission error accessing promocode file: {e}")
            await interaction.followup.send(
                "❌ Permission error accessing promocode file. " "Check file permissions.",
                ephemeral=True,
            )
        except Exception as e:
            log.exception(f"Error syncing promocode database: {e}")
            await interaction.followup.send(
                f"❌ An unexpected error occurred while syncing the database: {str(e)[:100]}",
                ephemeral=True,
            )

    @app_commands.command(name="create")
    @app_commands.default_permissions(administrator=True)
    @app_commands.autocomplete(ball_id=ball_autocomplete, special_id=special_autocomplete)
    async def promocode_create(
        self,
        interaction: discord.Interaction,
        code: str,
        uses: int,
        expiry_days: int = 30,
        ball_id: Optional[int] = None,
        special_id: Optional[int] = None,
        max_uses_per_user: int = 1,
        description: str = "",
        is_hidden: bool = False,
    ):
        """
        Create a new promocode that rewards a collectible.

        Parameters
        ----------
        code: The promocode to create
        uses: Number of times this code can be used
        expiry_days: Days until the code expires (default: 30)
        ball_id: The specific collectible ID to reward (optional, random if not provided)
        special_id: Special event to apply to the collectible (optional)
        max_uses_per_user: Maximum uses per user (default: 1)
        description: Optional description for the promocode
        is_hidden: Whether the promocode should be hidden from public listings
        """
        if (
            not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command.", ephemeral=True
            )
            return

        # Normalize the code to uppercase and remove whitespace
        code = code.strip().upper()
        if not code:
            await interaction.response.send_message(
                "❌ Promocode cannot be empty.", ephemeral=True
            )
            return

        # Check if code contains only valid characters
        if not all(c.isalnum() or c == "_" or c == "-" for c in code):
            await interaction.response.send_message(
                "❌ Promocode can only contain letters, numbers, underscores, and hyphens.",
                ephemeral=True,
            )
            return

        if code in ACTIVE_PROMOCODES:
            await interaction.response.send_message(
                f"❌ Promocode '{code}' already exists.", ephemeral=True
            )
            return

        if uses <= 0 or expiry_days <= 0:
            await interaction.response.send_message(
                "❌ Invalid parameters. Uses and expiry days must be positive.", ephemeral=True
            )
            return

        # Check if balls dictionary is populated
        if not balls:
            await interaction.response.send_message(
                f"❌ Cannot create promocode: {settings.collectible_name} data not loaded yet.",
                ephemeral=True,
            )
            return

        # Validate ball_id if provided
        ball_name = f"Random {settings.collectible_name.capitalize()}"
        if ball_id is not None:
            if ball_id not in balls:
                await interaction.response.send_message(
                    f"❌ Invalid {settings.collectible_name} ID: {ball_id}", ephemeral=True
                )
                return
            ball_name = balls[ball_id].country

        # Validate special_id if provided
        special_name = "None"
        if special_id is not None:
            # Check if specials dictionary is populated
            if not specials:
                await interaction.response.send_message(
                    (
                        "❌ Cannot create promocode with special event: "
                        "special events data not loaded yet."
                    ),
                    ephemeral=True,
                )
                return

            if special_id not in specials:
                await interaction.response.send_message(
                    f"❌ Invalid special event ID: {special_id}", ephemeral=True
                )
                return
            special_name = specials[special_id].name

        try:
            # Create promocode with specified parameters
            expiry = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)
            expiry += timedelta(days=expiry_days)

            if not create_promocode(
                code,
                uses,
                expiry_date=expiry,
                specific_ball_id=ball_id,
                special_id=special_id,
                max_uses_per_user=max_uses_per_user,
                description=description,
                is_hidden=is_hidden,
                created_by=f"{interaction.user.name} ({interaction.user.id})",
            ):
                log.warning(f"Failed to create promocode {code}")
                await interaction.response.send_message(
                    "❌ Failed to create promocode. Check logs for details.", ephemeral=True
                )
                return

            # Build success message
            reward_description = ball_name
            if special_id is not None:
                reward_description += f" ({special_name})"

            # Create embed for better visual presentation
            embed = discord.Embed(
                title=f"✅ Promocode '{code}' Created",
                description=f"Successfully created new promocode for {settings.bot_name}!",
                color=discord.Color.green(),
            )
            embed.add_field(
                name="Reward",
                value=f"{reward_description} {settings.collectible_name}",
                inline=False,
            )
            embed.add_field(
                name="Uses", value=f"{uses} (Max {max_uses_per_user} per user)", inline=True
            )
            embed.add_field(name="Expires", value=expiry.strftime("%Y-%m-%d"), inline=True)

            # Add visibility status
            visibility = "🔒 Hidden" if is_hidden else "🔓 Public"
            embed.add_field(name="Visibility", value=visibility, inline=True)

            # Add description if provided
            if description:
                embed.add_field(name="Description", value=description, inline=False)

            # Add creator info
            embed.set_footer(text=f"Created by {interaction.user.name}")

            await interaction.response.send_message(embed=embed, ephemeral=True)
        except ValueError as e:
            log.error(f"ValueError occurred while creating promocode {code}: {e}")
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        except TypeError as e:
            log.error(f"TypeError occurred while creating promocode {code}: {e}")
            await interaction.response.send_message(f"❌ {e}", ephemeral=True)
        except Exception as e:
            log.exception(f"Unexpected error occurred while creating promocode {code}: {e}")
            await interaction.response.send_message(
                "❌ An unexpected error occurred. Check logs for details.", ephemeral=True
            )

    @app_commands.command(name="update")
    @app_commands.default_permissions(administrator=True)
    async def promocode_update(
        self, interaction: discord.Interaction, code: str, uses_to_add: int
    ):
        """
        Update an existing promocode by adding more uses.

        Parameters
        ----------
        code: The promocode to update
        uses_to_add: Number of uses to add (can be negative to decrease)
        """
        if (
            not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command.", ephemeral=True
            )
            return

        # Normalize code format
        code = code.strip().upper()

        if not code:
            await interaction.response.send_message(
                "❌ Promocode cannot be empty.", ephemeral=True
            )
            return

        if code not in ACTIVE_PROMOCODES:
            await interaction.response.send_message(
                f"❌ Promocode '{code}' does not exist.", ephemeral=True
            )
            return

        try:
            # Get current uses
            old_uses = ACTIVE_PROMOCODES[code]["uses_left"]

            # Update uses with the function from active.py
            new_uses = update_promocode_uses(code, uses_to_add)

            if new_uses is None:
                log.warning(f"Failed to update promocode {code}")
                await interaction.response.send_message(
                    "❌ Failed to update promocode. Check logs for details.", ephemeral=True
                )
                return

            # Get reward info for the updated promocode
            promocode_data = ACTIVE_PROMOCODES[code]
            ball_id = promocode_data["rewards"].get("specific_ball")
            reward_info = f"Random {settings.collectible_name.capitalize()}"

            if ball_id is not None and ball_id in balls:
                reward_info = f"{balls[ball_id].country} {settings.collectible_name}"

            # Create embed for better visual presentation
            embed = discord.Embed(
                title=f"✅ Promocode '{code}' Updated",
                description=f"Successfully updated promocode in {settings.bot_name}!",
                color=discord.Color.blue(),
            )
            embed.add_field(name="Uses", value=f"{old_uses} → {new_uses}", inline=True)
            embed.add_field(name="Reward", value=reward_info, inline=True)
            embed.add_field(
                name="Expires", value=promocode_data["expiry"].strftime("%Y-%m-%d"), inline=True
            )

            await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as e:
            log.exception(f"Error updating promocode {code}: {e}")
            await interaction.response.send_message(
                f"❌ An error occurred while updating the promocode: {str(e)}", ephemeral=True
            )

    @app_commands.command(name="delete")
    @app_commands.default_permissions(administrator=True)
    async def promocode_delete(
        self, interaction: discord.Interaction, code: str, archive: bool = True
    ):
        """
        Delete an existing promocode.

        Parameters
        ----------
        code: The promocode to delete
        archive: Whether to archive the promocode instead of permanently
                 deleting it (default: True)
        """
        if (
            not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command.", ephemeral=True
            )
            return

        # Defer response while we process
        await interaction.response.defer(ephemeral=True)

        # Normalize code format
        code = code.strip().upper()

        if not code:
            await interaction.followup.send("❌ Promocode cannot be empty.", ephemeral=True)
            return

        if code not in ACTIVE_PROMOCODES:
            # Check if there are any promocodes at all
            if not ACTIVE_PROMOCODES:
                await interaction.followup.send(
                    "❌ No promocodes exist in the system. Use `/promocode create` to create one.",
                    ephemeral=True,
                )
            else:
                # Suggest similar codes if any exist
                similar_codes = [c for c in ACTIVE_PROMOCODES.keys() if code in c or c in code]
                suggestion = ""
                if similar_codes:
                    suggestion = f"\n\nDid you mean one of these?\n{', '.join(similar_codes[:5])}"
                    if len(similar_codes) > 5:
                        suggestion += f" (and {len(similar_codes) - 5} more...)"

                await interaction.followup.send(
                    f"❌ Promocode '{code}' does not exist.{suggestion}", ephemeral=True
                )
            return

        try:
            # Get promocode data before deleting
            promocode_data = ACTIVE_PROMOCODES[code]

            # Extract reward information safely
            reward_info = f"Random {settings.collectible_name.capitalize()}"
            ball_id = promocode_data["rewards"].get("specific_ball")
            if ball_id is not None:
                if balls and ball_id in balls:
                    reward_info = f"{balls[ball_id].country} {settings.collectible_name}"
                else:
                    reward_info = f"Unknown {settings.collectible_name} (ID: {ball_id})"

            # Get special info if any
            special_id = promocode_data["rewards"].get("special")
            if special_id is not None:
                if specials and special_id in specials:
                    reward_info += f" ({specials[special_id].name})"
                else:
                    reward_info += f" (Unknown Special ID: {special_id})"

            # Delete the promocode using the function from active.py
            if not delete_promocode(code, archive=archive):
                log.warning(f"Failed to delete promocode {code}")
                await interaction.followup.send(
                    "❌ Failed to delete promocode. Check logs for details.", ephemeral=True
                )
                return

            # Create confirmation embed with more details
            action = "Archived" if archive else "Deleted"
            embed = discord.Embed(
                title=f"✅ Promocode '{code}' {action}",
                description=f"Successfully {action.lower()} promocode from {settings.bot_name}!",
                color=discord.Color.orange() if archive else discord.Color.red(),
            )
            embed.add_field(name="Reward was", value=reward_info, inline=True)
            embed.add_field(
                name="Remaining uses", value=f"{promocode_data['uses_left']}", inline=True
            )

            # Add archive info if archived
            if archive:
                embed.add_field(name="Archive Status", value="✅ Saved to archive", inline=True)

            # Add expiry information if available
            if "expiry" in promocode_data:
                try:
                    expiry = promocode_data["expiry"]
                    embed.add_field(
                        name="Expiry date", value=expiry.strftime("%Y-%m-%d"), inline=True
                    )
                except (ValueError, TypeError, AttributeError):
                    pass

            # Add file save status
            if save_promocodes_to_file():
                embed.set_footer(text="Changes saved to file successfully.")
            else:
                embed.set_footer(
                    text="Warning: Failed to save changes to file. Changes are only in memory."
                )

            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            log.exception(f"Error deleting promocode {code}: {e}")
            await interaction.followup.send(
                f"❌ An unexpected error occurred while deleting the promocode: {str(e)[:100]}",
                ephemeral=True,
            )

    @app_commands.command(name="clean")
    @app_commands.default_permissions(administrator=True)
    async def promocode_clean(self, interaction: discord.Interaction, archive: bool = True):
        """
        Clean expired and depleted promocodes.

        Parameters
        ----------
        archive: Whether to archive cleaned promocodes instead of permanently
                 deleting them (default: True)
        """
        if (
            not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command.", ephemeral=True
            )
            return

        # Defer response while we process
        await interaction.response.defer(ephemeral=True)

        try:
            # Ensure promocodes are loaded first
            if not ACTIVE_PROMOCODES and not load_promocodes_from_file():
                log.error("Failed to load promocodes before cleaning")
                await interaction.followup.send(
                    "❌ Failed to load promocodes. Check logs for details.", ephemeral=True
                )
                return

            try:
                # Clean expired promocodes
                cleaned_count = clean_expired_promocodes(archive=archive)

                # Get count after cleaning
                after_count = len(ACTIVE_PROMOCODES)

                if cleaned_count > 0:
                    # Create success embed
                    embed = discord.Embed(
                        title="✅ Expired Promocodes Cleaned",
                        description=(
                            f"Successfully cleaned {cleaned_count} "
                            f"expired or depleted promocodes."
                        ),
                        color=discord.Color.green(),
                    )
                    embed.add_field(
                        name="Archive Status",
                        value="✅ Saved to archive" if archive else "❌ Permanently deleted",
                        inline=True,
                    )
                    embed.add_field(
                        name="Remaining Promocodes", value=str(after_count), inline=True
                    )

                    await interaction.followup.send(embed=embed, ephemeral=True)
                else:
                    await interaction.followup.send(
                        "✅ No expired or depleted promocodes to clean.", ephemeral=True
                    )
            except PermissionError as e:
                log.error(f"Permission error while cleaning promocodes: {e}")
                await interaction.followup.send(
                    "❌ Permission error: Cannot access promocode file. "
                    "Check file permissions.",
                    ephemeral=True,
                )
            except OSError as e:
                log.error(f"OS error while cleaning promocodes: {e}")
                await interaction.followup.send(
                    f"❌ File system error: {str(e)[:100]}", ephemeral=True
                )
            except json.JSONDecodeError as e:
                log.error(f"JSON decode error while cleaning promocodes: {e}")
                await interaction.followup.send(
                    f"❌ Invalid promocode file format. Error: {str(e)[:100]}", ephemeral=True
                )
            except Exception as e:
                log.error(f"Error in clean_expired_promocodes function: {e}")
                await interaction.followup.send(
                    f"❌ Failed to clean expired promocodes: {str(e)[:100]}", ephemeral=True
                )
        except Exception as e:
            log.exception(f"Unexpected error cleaning expired promocodes: {e}")
            await interaction.followup.send(
                f"❌ An unexpected error occurred while cleaning promocodes: {str(e)[:100]}",
                ephemeral=True,
            )

    @app_commands.command(name="list")
    @app_commands.default_permissions(administrator=True)
    async def promocode_list(
        self,
        interaction: discord.Interaction,
        show_expired: bool = False,
        show_depleted: bool = False,
        show_hidden: bool = False,
        sort_by: Optional[Literal["code", "expiry", "uses_left", "created_at"]] = None,
    ):
        """
        List promocodes with various filtering and sorting options.

        Parameters
        ----------
        show_expired: Whether to show expired promocodes (default: False)
        show_depleted: Whether to show promocodes with no uses left (default: False)
        show_hidden: Whether to show hidden promocodes (default: False)
        sort_by: Field to sort by ("code", "expiry", "uses_left", "created_at") (default: None)
        """
        if (
            not isinstance(interaction.user, discord.Member)
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command.", ephemeral=True
            )
            return

        # Defer response while we process
        await interaction.response.defer(ephemeral=True)

        try:
            # Get promocodes with filtering options
            promocodes = get_active_promocodes(
                include_expired=show_expired,
                include_depleted=show_depleted,
                include_hidden=show_hidden,
                sort_by=sort_by,
            )

            if not promocodes:
                # Check if there are any promocodes at all
                if not ACTIVE_PROMOCODES:
                    await interaction.followup.send(
                        (
                            "No promocodes found in the system. "
                            "Use `/promocode create` to create one."
                        ),
                        ephemeral=True,
                    )
                else:
                    # Build status message based on filters
                    filters = []
                    if show_expired:
                        filters.append("expired")
                    if show_depleted:
                        filters.append("depleted")
                    if show_hidden:
                        filters.append("hidden")

                    if filters:
                        status = " or ".join(filters)
                    else:
                        status = "active"

                    await interaction.followup.send(
                        (
                            f"No {status} promocodes found with the current filters. "
                            f"There are {len(ACTIVE_PROMOCODES)} total promocodes "
                            f"in the system."
                        ),
                        ephemeral=True,
                    )
                return

            now = datetime.now(timezone.utc)
            entries = []

            for code, data in promocodes.items():
                try:
                    # Get ball info if specific ball
                    ball_info = "Random Ball"
                    specific_ball_id = data["rewards"].get("specific_ball")
                    if specific_ball_id is not None:
                        if balls and specific_ball_id in balls:
                            ball_info = (
                                f"{balls[specific_ball_id].country} (ID: {specific_ball_id})"
                            )
                        else:
                            ball_info = (
                                f"Unknown {settings.collectible_name} (ID: {specific_ball_id})"
                            )

                    # Get special event info if any
                    special_info = ""
                    special_id = data["rewards"].get("special")
                    if special_id is not None:
                        if specials and special_id in specials:
                            special_info = f" ({specials[special_id].name})"
                        else:
                            special_info = f" (Unknown Special ID: {special_id})"

                    # Calculate stats
                    max_per_user = data.get("max_uses_per_user", 1)
                    used_by = data.get("used_by", set())
                    used_count = len(used_by)
                    status = "🟢 Active"

                    if now > data["expiry"]:
                        status = "🔴 Expired"
                    elif data["uses_left"] <= 0:
                        status = "🟠 Depleted"

                    # Check if hidden
                    if data.get("is_hidden", False):
                        status += " 🔒"

                    # Get creation info if available
                    created_info = ""
                    if "created_at" in data:
                        try:
                            created_at = data["created_at"]
                            if isinstance(created_at, str):
                                created_at = datetime.fromisoformat(created_at)
                            created_info = f"\n**Created:** {created_at.strftime('%Y-%m-%d')}"
                        except (ValueError, TypeError, AttributeError):
                            pass

                    # Get description if available
                    description_info = ""
                    if "description" in data and data["description"]:
                        description_info = f"\n**Description:** {data['description']}"

                    # Add hidden status if applicable
                    hidden_info = ""
                    if "is_hidden" in data and data["is_hidden"]:
                        hidden_info = "\n**Status:** 🔒 Hidden"

                    entries.append(
                        (
                            f"{code} ({status})",
                            f"**Expires:** {data['expiry'].strftime('%Y-%m-%d')}\n"
                            f"**Uses left:** {data['uses_left']} ({used_count} used)\n"
                            f"**Max per user:** {max_per_user}\n"
                            f"**Reward:** {ball_info}{special_info} {settings.collectible_name}"
                            f"{hidden_info}{created_info}{description_info}",
                        )
                    )
                except Exception as e:
                    # Handle errors for individual promocodes
                    log.error(f"Error processing promocode {code}: {e}")
                    entries.append(
                        (f"{code} - Error", f"Could not process this promocode: {str(e)[:100]}")
                    )

            if not entries:
                # Build status message based on filters
                filters = []
                if show_expired:
                    filters.append("expired")
                if show_depleted:
                    filters.append("depleted")
                if show_hidden:
                    filters.append("hidden")

                if filters:
                    filter_text = ", ".join(filters)
                    await interaction.followup.send(
                        f"No promocodes found matching filters: {filter_text}", ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        (
                            "No active promocodes found. Try with different filter "
                            "options to see more promocodes."
                        ),
                        ephemeral=True,
                    )
                return

            source = FieldPageSource(entries, per_page=5)

            # Build title based on filters
            title_parts = []
            if show_expired:
                title_parts.append("Expired")
            if show_depleted:
                title_parts.append("Depleted")
            if show_hidden:
                title_parts.append("Hidden")

            if title_parts:
                title = f"{', '.join(title_parts)} & Active Promocodes"
            else:
                title = "Active Promocodes"

            source.embed.title = f"{title} for {settings.bot_name}"

            # Add sorting info if applicable
            sort_info = ""
            if sort_by:
                sort_info = f" (Sorted by {sort_by})"

            source.embed.description = (
                f"Showing promocodes that reward {settings.plural_collectible_name}.{sort_info}"
            )
            pages = Pages(
                source=source,
                interaction=cast("discord.Interaction[BallsDexBot]", interaction),
                compact=True,
            )
            await pages.start()
        except Exception as e:
            log.exception(f"Error listing promocodes: {e}")
            await interaction.followup.send(
                f"❌ An error occurred while listing promocodes: {str(e)[:100]}", ephemeral=True
            )
