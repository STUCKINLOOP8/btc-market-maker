import os
import sys
from pathlib import Path

# Ensure project root is in sys.path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_maker.playback import app as base_app


class VercelPathMiddleware:
    """Normalizes URL paths forwarded by Vercel serverless routing."""

    def __init__(self, asgi_app):
        self.asgi_app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "websocket"):
            path = scope.get("path", "")
            # Strip Vercel function file prefixes if present
            for prefix in ("/api/index.py", "/api/index"):
                if path.startswith(prefix):
                    path = path[len(prefix):]
                    break
            # If path is empty, or specifically /api or /api/, route to the terminal root /
            if not path or path in ("/api", "/api/"):
                path = "/"
            scope["path"] = path
        await self.asgi_app(scope, receive, send)


app = VercelPathMiddleware(base_app)
