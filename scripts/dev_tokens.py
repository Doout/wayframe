#!/usr/bin/env python3
"""Print fresh development credentials as shell exports. Never commit the output."""

import secrets

for name in ("WAYFRAME_READER_TOKEN", "WAYFRAME_OPERATOR_TOKEN", "WAYFRAME_UPSTREAM_TOKEN"):
    print(f"export {name}={secrets.token_urlsafe(48)}")
