"""Entry point: python -m catalog_app [--port 8000] [--data data]"""

from __future__ import annotations

import argparse

from .web import serve


def main() -> None:
    parser = argparse.ArgumentParser(description="Catalog collection console")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data", default="data")
    args = parser.parse_args()
    httpd = serve(args.host, args.port, args.data)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
