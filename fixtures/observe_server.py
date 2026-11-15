#!/usr/bin/env python3
"""Deterministic demo feed for the observability collector (docs example).

Serves a fixed three-page generation over the links of
``fixtures/reconcile_network.json`` (L1, L2, L3), including one cross-page
duplicate (E2 repeats with the same instant written in another timezone) to
demonstrate canonical-value deduplication.  Standard library only.

Usage: ``python3 fixtures/observe_server.py [port]`` (default 8765; port 0
picks a free port).  The bound URL is printed on the first stdout line.
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

TOKEN = "demo-gen-1"
PAGES = [
    {
        "snapshot": TOKEN,
        "next": "demo-c1",
        "events": [
            {"id": "E1", "link_id": "L1", "time": "2024-03-01T00:00:00Z", "state": "up"},
            {"id": "E2", "link_id": "L2", "time": "2024-03-01T08:00:00+08:00", "state": "down"},
            {"id": "E3", "link_id": "L3", "time": "2024-03-01T00:00:05Z", "state": "up"},
        ],
    },
    {
        "snapshot": TOKEN,
        "next": "demo-c2",
        "events": [
            # Same id, same instant (00:00Z), same state as page 0: deduplicated.
            {"id": "E2", "link_id": "L2", "time": "2024-03-01T00:00:00Z", "state": "down"},
            {"id": "E4", "link_id": "L1", "time": "2024-03-01T00:01:00Z", "state": "down"},
        ],
    },
    {
        "snapshot": TOKEN,
        "next": None,
        "events": [
            {"id": "E5", "link_id": "L2", "time": "2024-03-01T00:02:00Z", "state": "up"},
        ],
    },
]
CURSORS = {"demo-c1": 1, "demo-c2": 2}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path != "/feed":
            self.send_error(404)
            return
        if not parsed.query:
            index = 0
        else:
            params = parse_qs(parsed.query)
            if params.get("snapshot", [None])[0] != TOKEN:
                self.send_error(410)
                return
            index = CURSORS.get(params.get("cursor", [None])[0])
            if index is None:
                self.send_error(400)
                return
        body = json.dumps(PAGES[index]).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("serving http://127.0.0.1:%d/feed" % server.server_address[1], flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
