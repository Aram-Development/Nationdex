import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import TYPE_CHECKING, cast

import discord
from discord import app_commands
from discord.ext import commands

from ballsdex.core.models import BallInstance, Player, balls, specials
from ballsdex.core.utils.paginator import FieldPageSource, Pages
from ballsdex.packages.arampacks.active import (
    ACTIVE_PROMOCODES,
    clean_expired_promocodes,
    get_active_promocodes,
    get_promocode_rewards,
    is_valid_promocode,
    load_promocodes_from_file,
    mark_promocode_used,
)
from ballsdex.packages.arampacks.rarity import rarity_tiers as global_rarity_tiers
from ballsdex.settings import settings

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot
else:
    BallsDexBot = None

log = logging.getLogger("ballsdex.packages.arampacks")


class PromocodeModal(discord.ui.Modal):
    code = discord.ui.TextInput(
        label="Enter your promocode",
        placeholder="Enter promocode here...",
        required=True,
        min_length=3,  # Require at least 3 characters
        max_length=50,
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
                    "❌ Promocode cannot be empty. Please try again.", ephemeral=True
                )
                return

            # Convert to uppercase for consistency
            promocode = promocode.upper()

            # Check for invalid characters (only allow alphanumeric and some special chars)
            valid_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
            if not all(c in valid_chars for c in promocode):
                await interaction.response.send_message(
                    (
                        "❌ Promocode contains invalid characters. "
                        "Please use only letters, numbers, underscores, and hyphens."
                    ),
                    ephemeral=True,
                )
                return

            # Process the promocode
            await self.cog.process_promocode(interaction, promocode)
        except Exception as e:
            log.exception(f"Error in promocode modal submission: {e}")
            try:
                await interaction.response.send_message(
                    "❌ An error occurred while processing your promocode. Please try again later.",
                    ephemeral=True,
                )
            except Exception:
                # If we can't respond, the interaction might have already been responded to
                pass


class AramPacks(commands.Cog):
    """
    AramPacks - Combined rarity display and promocode management commands.
    """

    TIER_NA = "Tier: N/A"
    rarity_tiers = global_rarity_tiers

    def __init__(self, bot: "BallsDexBot"):
        self.bot = bot
        self.ball_loaded = asyncio.Event()
        # Schedule task to initialize promocodes once balls are loaded
        self.bot.loop.create_task(self.initialize_promocodes())

    async def initialize_promocodes(self):
        """Initialize promocodes once collectibles are loaded"""
        await self.bot.wait_until_ready()

        log.info("Initializing AramPacks system...")

        # Try to load promocodes from file first
        try:
            log.info("Attempting to load promocodes from file...")
            success = load_promocodes_from_file()

            if success:
                log.info("Successfully loaded promocodes from file")
                # Log the loaded promocodes for debugging
                log.info(f"Loaded promocodes: {list(ACTIVE_PROMOCODES.keys())}")
            else:
                log.warning("Failed to load promocodes from file. Using default in-memory codes.")
                # We'll continue with default codes in memory
        except Exception as e:
            log.exception(f"Error loading promocodes from file: {e}")
            # We'll continue with default codes in memory

        # Verify ACTIVE_PROMOCODES has expected content
        if not ACTIVE_PROMOCODES:
            log.error("ACTIVE_PROMOCODES dictionary is empty after initialization!")
        else:
            log.info(
                f"ACTIVE_PROMOCODES contains {len(ACTIVE_PROMOCODES)} codes after initialization"
            )

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
            log.debug(
                (
                    f"Waiting for {settings.collectible_name} data, "
                    f"attempt {retry_count}/{max_retries}"
                )
            )

        if not balls:
            log.warning(
                f"{settings.collectible_name.capitalize()} data not loaded after waiting, "
                "promocodes may not work correctly"
            )
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
            log.info(
                (
                    f"AramPacks system initialized with {len(active_promocodes)} "
                    f"active codes out of {len(all_promocodes)} total"
                )
            )

            # Clean expired promocodes
            cleaned = clean_expired_promocodes()
            if cleaned:
                log.info(f"Cleaned {cleaned} expired promocodes")
        except Exception as e:
            log.exception(f"Error getting promocode counts: {e}")
            log.info(
                "AramPacks system initialized with errors. "
                "Some features may not work correctly."
            )

        # Force reload one more time to ensure we have the latest data
        try:
            log.info("Performing final promocode reload to ensure latest data...")
            load_promocodes_from_file()
            log.info(f"Final promocode list: {list(ACTIVE_PROMOCODES.keys())}")
        except Exception as e:
            log.error(f"Error in final promocode reload: {e}")

    @app_commands.command()
    @app_commands.checks.cooldown(1, 60, key=lambda i: i.user.id)
    async def rarity(self, interaction: discord.Interaction):
        # DO NOT CHANGE THE CREDITS TO THE AUTHOR HERE!
        """
        Show the rarity list of the dex
        """
        # Filter enabled collectibles
        enabled_collectibles = [x for x in balls.values() if x.enabled]

        if not enabled_collectibles:
            await interaction.response.send_message(
                f"There are no collectibles registered in {settings.bot_name} yet.",
                ephemeral=True,
            )
            return

        # Sort collectibles by rarity in ascending order
        sorted_collectibles = sorted(enabled_collectibles, key=lambda x: x.rarity)

        entries = []

        for collectible in sorted_collectibles:
            country_name = f"{collectible.country}"
            emoji = self.bot.get_emoji(collectible.emoji_id)

            if emoji:
                emote = str(emoji)
            else:
                emote = "N/A"
            # if you want the Rarity to only show full numbers like 1 or 12 use the code part here:
            # rarity = int(collectible.rarity)
            # otherwise you want to display numbers like 1.5, 5.3, 76.9 use the normal part.
            rarity = collectible.rarity

            tier_name = None
            for (low, high), t_name in self.rarity_tiers:
                if low <= collectible.rarity < high:
                    tier_name = t_name
                    break
            if tier_name:
                entry = (country_name, f"{emote} Rarity: {rarity} | Tier: `{tier_name}`")
            else:
                entry = (country_name, f"{emote} Rarity: {rarity} | {self.TIER_NA}")
            entries.append(entry)
        # This is the number of countryballs who are displayed at one page,
        # you can change this, but keep in mind: discord has an embed size limit.
        per_page = 5

        source = FieldPageSource(entries, per_page=per_page, inline=False, clear_description=False)
        source.embed.description = f"__**{settings.bot_name} rarity**__"
        source.embed.colour = discord.Colour.blurple()
        source.embed.set_author(
            name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url
        )

        pages = Pages(
            source=source,
            interaction=cast("discord.Interaction[BallsDexBot]", interaction),
            compact=True,
        )
        await pages.start()

    @app_commands.command()
    @app_commands.checks.cooldown(1, 60, key=lambda i: i.user.id)
    async def events_rarity(self, interaction: discord.Interaction):
        # DO NOT CHANGE THE CREDITS TO THE AUTHOR HERE!
        """
        Show the rarity list of the dex
        """
        # Filter enabled collectibles
        events = [x for x in specials.values()]

        if not events:
            await interaction.response.send_message(
                f"There are no events registered in {settings.bot_name} yet.",
                ephemeral=True,
            )
            return

        # Sort collectibles by rarity in ascending order

        entries = []

        for special in events:
            name = f"{special.name}"
            emoji = special.emoji

            if emoji:
                emote = str(emoji)
            else:
                emote = "N/A"

            filters = {}
            filters["special"] = special

            count = await BallInstance.filter(**filters)
            countNum = len(count)
            # sorted_collectibles = sorted(enabled_collectibles.values(), key=lambda x: x.rarity)
            # if you want the Rarity to only show full numbers like 1 or 12 use the code part here:
            # rarity = int(collectible.rarity)
            # otherwise you want to display numbers like 1.5, 5.3, 76.9 use the normal part.

            entry = (name, f"{emote} Count: {countNum}")
            entries.append(entry)
        # This is the number of countryballs who are displayed at one page,
        # you can change this, but keep in mind: discord has an embed size limit.
        per_page = 5

        source = FieldPageSource(entries, per_page=per_page, inline=False, clear_description=False)
        source.embed.description = f"__**{settings.bot_name} events rarity**__"
        source.embed.colour = discord.Colour.blurple()
        source.embed.set_author(
            name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url
        )

        pages = Pages(
            source=source,
            interaction=cast("discord.Interaction[BallsDexBot]", interaction),
            compact=True,
        )
        await pages.start()

    @app_commands.command(name="promocode_redeem")
    @app_commands.checks.cooldown(
        1, 10, key=lambda i: str(i.user.id)
    )  # Standard cooldown for consistency
    async def promocode_redeem(self, interaction: discord.Interaction):
        """
        Redeem a promocode to get special rewards!

        Parameters
        ----------
        interaction : discord.Interaction
            The interaction object representing the user's command invocation.

        Behavior
        --------
        Presents a modal for the user to enter a promocode, checks if the
        collectible data is loaded, and handles the redemption process.
        Responds with an ephemeral message if the system is not ready.
        """
        if not self.ball_loaded.is_set():
            await interaction.response.send_message(
                (
                    f"❌ {settings.bot_name} is still initializing "
                    f"{settings.collectible_name} data. "
                    "Please try again in a few moments."
                ),
                ephemeral=True,
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
                await interaction.response.send_message(
                    "❌ Promocode cannot be empty. Please enter a valid code.", ephemeral=True
                )
                return
        except Exception as e:
            log.error(f"Error normalizing promocode: {e}")
            await interaction.response.send_message("❌ Invalid promocode format.", ephemeral=True)
            return

        # Check if promocode is valid
        is_valid, error_message = is_valid_promocode(code, user_id)
        if not is_valid:
            await interaction.response.send_message(error_message, ephemeral=True)
            return

        # Get or create player
        try:
            player, created = await Player.get_or_create(discord_id=user_id)
            if created:
                log.info(f"Created new player for user {user_id}")
        except Exception as e:
            log.exception(f"Error getting or creating player for user {user_id}: {e}")
            await interaction.response.send_message(
                "❌ Database error occurred. Please try again later.", ephemeral=True
            )
            return

        # Get promocode rewards
        try:
            rewards = get_promocode_rewards(code)
            if not rewards:
                await interaction.response.send_message(
                    "❌ This promocode has no rewards configured.", ephemeral=True
                )
                return
        except Exception as e:
            log.exception(f"Error getting promocode rewards for {code}: {e}")
            await interaction.response.send_message(
                "❌ Error retrieving promocode rewards.", ephemeral=True
            )
            return

        # Process rewards
        try:
            reward_text = []

            # Handle ball rewards
            specific_ball_id = rewards.get("specific_ball")
            special_id = rewards.get("special")

            if specific_ball_id:
                # Give specific ball
                if specific_ball_id not in balls:
                    log.error(
                        f"Promocode {code} references non-existent ball ID: {specific_ball_id}"
                    )
                    await interaction.response.send_message(
                        "❌ This promocode is misconfigured. Please contact support.",
                        ephemeral=True,
                    )
                    return

                ball = balls[specific_ball_id]
            else:
                # Give random ball
                enabled_balls = [x for x in balls.values() if x.enabled]
                if not enabled_balls:
                    await interaction.response.send_message(
                        "❌ No collectibles available for rewards.", ephemeral=True
                    )
                    return
                ball = random.choice(enabled_balls)

            # Create ball instance
            special_obj = None
            if special_id and special_id in specials:
                special_obj = specials[special_id]

            # Calculate bonuses (similar to normal spawning)
            health_bonus = random.randint(-settings.max_health_bonus, settings.max_health_bonus)
            attack_bonus = random.randint(-settings.max_attack_bonus, settings.max_attack_bonus)

            ball_instance = await BallInstance.create(
                ball=ball,
                player=player,
                attack_bonus=attack_bonus,
                health_bonus=health_bonus,
                special=special_obj,
                spawned_time=datetime.now(timezone.utc),
                server_id=interaction.guild_id if interaction.guild else None,
            )

            # Format reward text
            emoji = self.bot.get_emoji(ball.emoji_id)
            emoji_str = str(emoji) if emoji else "🎯"

            reward_text.append(f"{emoji_str} **{ball.country}**")
            reward_text.append(f"ATK: {ball_instance.attack} ({attack_bonus:+d}%)")
            reward_text.append(f"HP: {ball_instance.health} ({health_bonus:+d}%)")

            if special_obj:
                reward_text.append(f"🌟 Special: **{special_obj.name}**")
                if special_obj.catch_phrase:
                    reward_text.append(f"*{special_obj.catch_phrase}*")

        except Exception as e:
            log.exception(f"Error processing promocode rewards for {code}: {e}")
            await interaction.response.send_message(
                "❌ Error processing rewards. Please try again later.", ephemeral=True
            )
            return

        # Mark promocode as used
        try:
            success = mark_promocode_used(code, user_id)
            if not success:
                log.error(f"Failed to mark promocode {code} as used for user {user_id}")
                # We'll continue anyway since the reward was already given
        except Exception as e:
            log.exception(f"Error marking promocode as used: {e}")
            # Continue anyway

        # Send success message
        try:
            embed = discord.Embed(
                title="🎉 Promocode Redeemed Successfully!",
                description="You received:\n" + "\n".join(reward_text),
                color=discord.Color.green(),
            )
            embed.set_footer(text=f"Ball ID: #{ball_instance.pk:0X}")

            await interaction.response.send_message(embed=embed, ephemeral=True)
            log.info(f"Successfully processed promocode {code} for user {user_id}")

        except Exception as e:
            log.exception(f"Error sending success message for promocode redemption: {e}")
            # Try a simple text message as fallback
            try:
                simple_message = (
                    "🎉 Promocode redeemed! You received: " f"{' | '.join(reward_text)}"
                )
                await interaction.response.send_message(simple_message, ephemeral=True)
            except Exception:
                # If we can't send any message, just log it
                log.error("Could not send any response message for promocode redemption")
