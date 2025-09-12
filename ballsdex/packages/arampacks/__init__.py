from typing import TYPE_CHECKING

from ballsdex.packages.arampacks.cog import AramPacks

if TYPE_CHECKING:
    from ballsdex.core.bot import BallsDexBot


async def setup(bot: "BallsDexBot"):
    await bot.add_cog(AramPacks(bot))
