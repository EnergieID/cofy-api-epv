"""
Cofy API - application entrypoint.

This file bootstraps the CofyApi app and registers the modules you need.
See https://github.com/EnergieID/cofy-api for all available modules & options.

Quick start:
  1. Copy .env.example → .env and fill in your values
  2. `uv sync` to install dependencies
  3. `poe dev` to start the dev server (auto-reloads, reads .env)
"""

from os import environ

from cofy import CofyAPI
from cofy.api import token_verifier
from cofy.modules.directive import DirectiveModule, DirectiveSource
from fastapi import Depends

from directive.dbsource import DBSource

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
# token_verifier protects all module endpoints with a simple bearer token.
# Map each token to a dict with at least a "name" key.
cofy = CofyAPI(dependencies=[Depends(token_verifier({environ.get("ENERGY_ID_COFY_API_TOKEN"): {"name": "EnergyID"}}))])

# ---------------------------------------------------------------------------
# Modules – uncomment / add the ones you need
# ---------------------------------------------------------------------------
# Each module exposes its own set of API routes under the name you choose.
# Browse the available modules:  https://github.com/EnergieID/cofy-api

cofy.register_module(
    DirectiveModule(
        source=DirectiveSource(
            source=DBSource(
                db_url=environ.get("DB_URL"),
                itemid=int(environ.get("DB_ITEMID", "42923")),
            ),
            boundaries=(-100000, 0, 100000, 500000),
        ),
    )
)
