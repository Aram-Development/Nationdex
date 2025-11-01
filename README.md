# Nationdex Discord Bot

![NationDex Banner](admin_panel/media/nationdexbanner.jpeg)

**Nationdex** is a unique fork of Ballsdex, soon to become a independent bot with the definitive codebase we all wanted, designed to deliver the best user experience with less limitations and over 100 nations to catch!

[**Invite the bot to your server!**](https://discord.com/application-directory/1207017704096141312)

---

## 🌐 Community & Support

Join our official Discord community for installation help, questions, suggestions, or to contribute!

### Nationdex HQ

[![Nationdex HQ](https://discord.com/api/guilds/1118965941221466194/embed.png?style=banner2)](https://discord.gg/tKh3n5uVnm)

---

## 🤝 Ballsdex Community

Nationdex is proudly based on BallsDex. For broader support and community, check out these servers:

- **Ballsdex Developers:**  
  [![Ballsdex Devs](https://discord.com/api/guilds/1255250024741212262/embed.png?style=banner3)](https://discord.gg/PKKhee4fvy)

- **Ballsdex Official Server:**  
  [![Ballsdex Official](https://discord.com/api/guilds/1049118743101452329/embed.png?style=banner2)](https://discord.gg/tKh3n5uVnm)

> **Note:** Ariel Aram is banned from the Ballsdex server by decision of **thebicfish** due to involvement in the AsiaDex drama (September 2024). However, we continue to provide the official Ballsdex server link due to our explicit association.

---

## 🐞 Suggestions, Issues & Bug Reports

Found a bug or have a suggestion?  
[Open an issue on GitHub](../../issues) to let us know!

---

## 📚 Documentation

Learn how to set up Nationdex and use all its features in the  
[**BallsDex Wiki**](https://github.com/laggron42/BallsDex-Discordbot/wiki/).  
More sections are added progressively.

---

## 💖 Supporting the Project

If you appreciate Laggron's work, consider supporting him on [Patreon](https://patreon.com/retke)!

---

## 🛠️ Contributing

Want to help improve Nationdex?  
Check out our [Contribution Guide](CONTRIBUTING.md) to get started!

---

## 🚀 Development with uv

This project now uses [uv](https://docs.astral.sh/uv/) for fast dependency management and Docker builds.

- Local development
  - Install uv (see official docs) and then:
    - `uv sync` to create a virtual environment and install dependencies.
    - `uv run python -m ballsdex` to start the bot locally.
    - `uv run uvicorn admin_panel.asgi:application --host 0.0.0.0 --port 8000` to run the admin panel.

- Docker
  - The Docker image uses the uv base image and installs dependencies via uv for speed and reproducibility.
  - Use `docker compose up --build` to build and run the full stack.

Notes:

- Configuration uses `config.toml` at the repository root.
- The admin panel reads TOML via `ballsdex.settings.read_settings`.
- For production, set `DJANGO_SECRET_KEY` and configure `DJANGO_ALLOWED_HOSTS`.

---

## ⚖️ License

This project is released under the [MIT License](https://opensource.org/licenses/MIT).

> **Please retain credits to the original authors if distributing this bot.**
