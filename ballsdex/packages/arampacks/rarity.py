from typing import List, Tuple

# Define rarity tiers that can be imported by both AramPacks and BallSpawnView
rarity_tiers: List[Tuple[Tuple[float, float], str]] = [
    ((0.5, 1.0), "Common 🟩"),
    ((0.1, 0.5), "Uncommon 🟦"),
    ((0.01, 0.1), "Rare 🟪"),
    ((0.001, 0.01), "Epic 🟧"),
    ((0.00001, 0.001), "Legendary 🟨"),
    ((0.00000001, 0.00001), "Exotic 🟥"),
    ((0.00000000000001, 0.00000001), "Mythic 🟫"),
    ((0.00000000000000000000001, 0.00000000000001), "Hellas I 🟫"),
    ((0.00000000000000000000000000001, 0.00000000000000000000001), "Hellas II 🟫"),
    ((0.00000000000000000000000000000001, 0.0000000000000000000000001), "Hellas III 🟫"),
    ((0.0000000000000000000000000000000001, 0.0000000000000000000000000001), "Hellas IV 🟫"),
]
