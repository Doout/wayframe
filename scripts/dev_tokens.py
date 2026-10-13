#!/usr/bin/env python3
"""Print fresh development credentials as shell exports. Never commit the output."""

import argparse
import secrets

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--admin", action="store_true", help="include a distinct management credential")
args = parser.parse_args()
names = ["WAYFRAME_READER_TOKEN", "WAYFRAME_OPERATOR_TOKEN", "WAYFRAME_UPSTREAM_TOKEN"]
if args.admin:
    names.append("WAYFRAME_ADMIN_TOKEN")
for name in names:
    print(f"export {name}={secrets.token_urlsafe(48)}")
