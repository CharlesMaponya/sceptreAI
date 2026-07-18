from __future__ import annotations

import smtplib
from email.message import EmailMessage
from urllib.parse import urlencode

from automl_api.core.config import get_settings


def send_password_reset_email(email: str, reset_token: str) -> bool:
    """Deliver a local-account reset link when SMTP is configured."""
    settings = get_settings()
    if not settings.smtp_host or not settings.smtp_from_email:
        return False

    query = urlencode({"mode": "reset", "token": reset_token})
    reset_url = f"{settings.public_app_url.rstrip('/')}/auth?{query}"
    message = EmailMessage()
    message["Subject"] = "Reset your Sceptre AI password"
    message["From"] = settings.smtp_from_email
    message["To"] = email
    message.set_content(
        "A password reset was requested for your Sceptre AI account.\n\n"
        f"Reset your password: {reset_url}\n\n"
        "This link expires in 30 minutes. If you did not request it, you can ignore this email."
    )

    smtp_class = smtplib.SMTP_SSL if settings.smtp_use_ssl else smtplib.SMTP
    with smtp_class(settings.smtp_host, settings.smtp_port, timeout=10) as client:
        if settings.smtp_starttls and not settings.smtp_use_ssl:
            client.starttls()
        if settings.smtp_username:
            client.login(settings.smtp_username, settings.smtp_password or "")
        client.send_message(message)
    return True
