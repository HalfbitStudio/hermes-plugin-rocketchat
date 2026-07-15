"""Interactive ``hermes gateway setup`` wizard for Rocket.Chat."""

from __future__ import annotations


def interactive_setup() -> None:
    """Interactive ``hermes gateway setup`` flow for the Rocket.Chat platform."""
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("Rocket.Chat")
    existing_url = get_env_value("ROCKETCHAT_URL")
    if existing_url:
        print_info(f"Rocket.Chat: already configured (server: {existing_url})")
        if not prompt_yes_no("Reconfigure Rocket.Chat?", False):
            return

    print_info("Connect Hermes to a self-hosted Rocket.Chat instance.")
    print_info("   Uses REST API v1 for outbound and DDP WebSocket for inbound messages.")
    print()

    url = prompt("Rocket.Chat server URL (e.g. https://rc.example.com)", default=existing_url or "")
    if not url:
        print_warning("Server URL is required — skipping Rocket.Chat setup")
        return
    save_env_value("ROCKETCHAT_URL", url.strip())

    print()
    print_info("🔑 Authentication")
    print_info("   Generate a Personal Access Token in your Rocket.Chat profile")
    print_info("   (My Account → Security → Personal Access Tokens)")
    print_info("   Make sure 'Ignore Two Factor' is checked.")

    token = prompt("Personal Access Token", password=True)
    if not token:
        print_warning("Token is required — skipping Rocket.Chat setup")
        return
    save_env_value("ROCKETCHAT_TOKEN", token.strip())

    user_id = prompt("Bot user _id (shown at PAT creation)")
    if not user_id:
        print_warning("User ID is required — skipping Rocket.Chat setup")
        return
    save_env_value("ROCKETCHAT_USER_ID", user_id.strip())

    print()
    print_info("⚙️  Options")

    reply_mode = prompt_yes_no(
        "Use threaded replies in channels/private groups?", False
    )
    if reply_mode:
        save_env_value("ROCKETCHAT_REPLY_MODE", "thread")

    require_mention = prompt_yes_no("Require @mention in channels?", True)
    save_env_value("ROCKETCHAT_REQUIRE_MENTION", "true" if require_mention else "false")

    home = prompt("Home channel room ID (for cron/notification delivery)", default="")
    if home:
        save_env_value("ROCKETCHAT_HOME_CHANNEL", home.strip())

    print()
    print_info("🔒 Access control")
    if prompt_yes_no("Allow all users to talk to the bot?", False):
        save_env_value("ROCKETCHAT_ALLOW_ALL_USERS", "true")
        print_warning("⚠️  Open access — any user on this instance can command the bot.")
    else:
        save_env_value("ROCKETCHAT_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed user IDs (comma-separated, leave empty to deny everyone)",
            default=get_env_value("ROCKETCHAT_ALLOWED_USERS") or "",
        )
        if allowed:
            save_env_value("ROCKETCHAT_ALLOWED_USERS", allowed.replace(" ", ""))
            print_success("Allowlist configured")
        else:
            save_env_value("ROCKETCHAT_ALLOWED_USERS", "")
            print_info("No users allowed — bot will ignore all messages until you add IDs.")

    print()
    print_success("Rocket.Chat configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

