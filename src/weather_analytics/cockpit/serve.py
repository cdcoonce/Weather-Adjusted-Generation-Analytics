"""Minimal loopback static server for local preview of dist/."""

from __future__ import annotations

import functools
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def make_server(dist_dir: Path, port: int) -> ThreadingHTTPServer:
    """Build a ThreadingHTTPServer rooted at dist_dir, bound to loopback."""
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(dist_dir))
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


def serve(dist_dir: Path, port: int = 8420) -> None:
    """Serve dist_dir until interrupted."""
    httpd = make_server(Path(dist_dir), port)
    bound = httpd.server_address[1]
    print(f"serving {dist_dir} at http://127.0.0.1:{bound}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
