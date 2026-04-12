from __future__ import annotations

from textwrap import dedent


def app_shell_html() -> str:
    return dedent(
        """\
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Mafia Chatroom</title>
          <link rel="stylesheet" href="/assets/react-app.css">
        </head>
        <body>
          <div id="root" data-app-shell="mafia-react"></div>
          <script type="module" src="/assets/react-app.js"></script>
        </body>
        </html>
        """
    )


__all__ = ["app_shell_html"]
