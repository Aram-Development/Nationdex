from typing import TYPE_CHECKING
import logging

from ballsdex.packages.arampacks.cog import AramPacks
from ballsdex.settings import settings

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot

log = logging.getLogger("ballsdex.packages.arampacks")


async def setup(bot: "BallsDexBot"):
    if not getattr(settings, "arampacks_enabled", True):
        log.info("AramPacks is disabled via settings; skipping cog load.")
        return
    await bot.add_cog(AramPacks(bot))
