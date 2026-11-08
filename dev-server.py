"""Local dev server that reloads the browser when the page or data changes. Start it with `make dev`."""
import sys
from pathlib import Path
from livereload import Server

ROOT = Path(__file__).resolve().parent
port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000

server = Server()
for pattern in ("*.html", "*.json", "*.svg", "*.png"):
    server.watch(str(ROOT / pattern))
server.serve(root=str(ROOT), host="localhost", port=port)
