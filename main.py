"""
LVAY - API Server
=================
Serves the JSON API for WordPress / LVAY.
Do NOT run scraper or scheduler inside the web service.
"""

import os
from server import app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[SERVER] Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
