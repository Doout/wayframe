#!/usr/bin/env python3
"""Manage a running gateway. The admin credential is read only from the environment."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def call(url, token, method, path, payload=None, revision=None):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    if revision is not None:
        headers["If-Match"] = f'"{revision}"'
    request = urllib.request.Request(url + path, data=body, headers=headers, method=method)

    # Ignore ambient proxies; redirecting privileged credentials is forbidden.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"Management request failed: HTTP {error.code}") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("show")
    sub.add_parser("deployment")
    sub.add_parser("reload")
    for command in ("create-user", "update-user", "apply-rules"):
        action = sub.add_parser(command)
        action.add_argument("file", type=Path, help="JSON profile or rules file")
    for command in ("delete-user", "rotate-token"):
        action = sub.add_parser(command)
        action.add_argument("id")
    args = parser.parse_args()
    endpoint = urllib.parse.urlsplit(args.url)
    if (
        endpoint.scheme not in {"http", "https"}
        or not endpoint.hostname
        or endpoint.username
        or endpoint.password
        or endpoint.path not in {"", "/"}
        or endpoint.query
        or endpoint.fragment
    ):
        parser.error("--url must be an HTTP(S) origin without credentials or a path")
    if endpoint.scheme == "http" and endpoint.hostname not in {"localhost", "127.0.0.1", "::1"}:
        parser.error("remote management requires HTTPS")
    token = os.environ.get("WAYFRAME_ADMIN_TOKEN")
    if not token:
        parser.error("set WAYFRAME_ADMIN_TOKEN")
    url = args.url.rstrip("/")
    try:
        if args.command in {"deployment", "reload"}:
            state = call(url, token, "GET", "/admin/deployment")
            result = (
                state
                if args.command == "deployment"
                else call(url, token, "POST", "/admin/reload", revision=state["generation"])
            )
            print(json.dumps(result, indent=2))
            return 0
        state = call(url, token, "GET", "/admin/state")
        if args.command == "show":
            result = state
        else:
            payload = None
            if hasattr(args, "file"):
                payload = json.loads(args.file.read_text())
            if args.command in {"create-user", "update-user"}:
                path = "/admin/users"
                method = "POST"
                if args.command == "update-user":
                    path += "/" + urllib.parse.quote(payload["id"], safe="")
                    method = "PUT"
            elif args.command == "apply-rules":
                method, path = "PUT", "/admin/rules"
            else:
                method, path = "DELETE", "/admin/users/" + urllib.parse.quote(args.id, safe="")
                if args.command == "rotate-token":
                    method, path = "POST", path + "/token"
            result = call(url, token, method, path, payload, state["revision"])
        # Creation/rotation deliberately return the new credential once; protect captured output.
        print(json.dumps(result, indent=2))
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
