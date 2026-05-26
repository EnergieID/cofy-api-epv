"""
Cofy API - application entrypoint.

This file bootstraps the CofyApi app and registers the modules you need.
See https://github.com/EnergieID/cofy-api for all available modules & options.

Quick start:
  1. Copy .env.example → .env and fill in your values
  2. `uv sync` to install dependencies
  3. `poe dev` to start the dev server (auto-reloads, reads .env)
"""

import json
from os import environ

from cofy import CofyAPI
from cofy.api import token_verifier
from cofy.modules.directive import DirectiveModule, DynamicBoundaryDirectiveSource
from fastapi import Depends

from directive.dbsource import BoundaryDBSource, SignalDBSource

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

forecasts = json.loads(environ.get("FORECASTS", "{}"))
for name, id in forecasts.items():
    cofy.register_module(
        DirectiveModule(
            source=DynamicBoundaryDirectiveSource(
                signal_source=SignalDBSource(
                    db_url=environ.get("DB_URL", "postgresql+asyncpg://cofy:cofy@localhost:5432/epv"),
                    itemid=id,
                ),
                boundary_source=BoundaryDBSource(
                    db_url=environ.get(
                        "BOUNDARY_DB_URL", environ.get("DB_URL", "postgresql+asyncpg://cofy:cofy@localhost:5432/epv")
                    ),
                    cohort_id=name,
                ),
            ),
            name=name,
            description=f"Directive for community {name}",
        )
    )
