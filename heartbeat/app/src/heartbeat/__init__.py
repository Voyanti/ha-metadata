"""Network heartbeat checker.

Runs periodic connectivity checks (default-gateway ping, public-DNS ping, and
MQTT-broker DNS resolution), buffers every result in a SQLite outbox, and
publishes them to an MQTT broker with store-and-forward retry.

The package is intentionally Home Assistant agnostic: it is configured purely
through environment variables, with an optional ``/data/options.json`` overlay
when run as a Home Assistant add-on. The same image therefore runs unchanged as
a plain Docker container on a Linux host.
"""

__version__ = "1.0.0"
