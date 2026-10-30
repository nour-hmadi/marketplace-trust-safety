#!/usr/bin/env bash
# Stop every container safely (data stays in the Docker volumes). Run this before shutting down the laptop.
cd "$(dirname "$0")"
docker compose stop
