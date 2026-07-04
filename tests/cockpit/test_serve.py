import urllib.request
from pathlib import Path

from weather_analytics.cockpit.serve import make_server


def test_make_server_serves_index(tmp_path: Path):
    (tmp_path / "index.html").write_text("<h1>hi cockpit</h1>", encoding="utf-8")
    httpd = make_server(tmp_path, port=0)  # port 0 -> OS-assigned
    import threading
    t = threading.Thread(target=httpd.handle_request)
    t.start()
    try:
        port = httpd.server_address[1]
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/index.html", timeout=5).read()
        assert b"hi cockpit" in body
    finally:
        t.join(timeout=5)
        httpd.server_close()
