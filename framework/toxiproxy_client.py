import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ToxiproxyClient:
    """A simple client for the Toxiproxy HTTP API."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.Client()

    def _request(self, method: str, endpoint: str, **kwargs) -> dict[str, Any] | None:
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.client.request(method, url, **kwargs)
            response.raise_for_status()
            if response.status_code == 204:  # No Content
                return None
            return response.json()
        except httpx.ConnectError as e:
            logger.error(f"Toxiproxy API request failed: {e}")
            raise RuntimeError(
                f"Could not connect to Toxiproxy at {self.base_url}. "
                "Please ensure Toxiproxy is running.\n\n"
                "Quick start:\n"
                "  docker run -d --name toxiproxy -p 8474:8474 ghcr.io/shopify/toxiproxy:latest\n\n"
                "For detailed setup instructions, see: experiments/FAILURE_RESILIENCE_SETUP.md"
            ) from e
        except httpx.HTTPStatusError as e:
            # 404 is expected when proxy doesn't exist - return None
            if e.response.status_code == 404:
                return None
            # Other HTTP errors are real problems
            logger.error(f"Toxiproxy API HTTP error: {e.response.status_code} {e}")
            raise RuntimeError(
                f"Toxiproxy API error: {e.response.status_code} - {e}"
            ) from e
        except httpx.RequestError as e:
            logger.error(f"Toxiproxy API request failed: {e}")
            raise RuntimeError(f"Toxiproxy API request failed: {e}") from e

    def get_proxy(self, name: str) -> dict[str, Any] | None:
        """Get a proxy by name. Returns None if proxy doesn't exist (404)."""
        return self._request("GET", f"/proxies/{name}")

    def create_proxy(
        self, name: str, listen: str, upstream: str, enabled: bool = True
    ) -> dict[str, Any] | None:
        """Create a new proxy."""
        payload = {
            "name": name,
            "listen": listen,
            "upstream": upstream,
            "enabled": enabled,
        }
        return self._request("POST", "/proxies", json=payload)

    def delete_proxy(self, name: str):
        """Delete a proxy."""
        self._request("DELETE", f"/proxies/{name}")

    def add_toxic(
        self,
        proxy_name: str,
        toxic_type: str,
        name: str,
        stream: str = "downstream",
        toxicity: float = 1.0,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Add a toxic to a proxy."""
        payload = {
            "name": name,
            "type": toxic_type,
            "stream": stream,
            "toxicity": toxicity,
            "attributes": attributes or {},
        }
        return self._request("POST", f"/proxies/{proxy_name}/toxics", json=payload)

    def remove_toxic(self, proxy_name: str, toxic_name: str):
        """Remove a toxic from a proxy."""
        self._request("DELETE", f"/proxies/{proxy_name}/toxics/{toxic_name}")

    def set_latency(
        self,
        proxy_name: str,
        latency_ms: int,
        jitter_ms: int = 0,
        toxic_name: str = "latency_toxic",
    ):
        """Adds or updates a latency toxic."""
        self.remove_toxic(proxy_name, toxic_name)
        if latency_ms > 0:
            attributes = {"latency": latency_ms, "jitter": jitter_ms}
            self.add_toxic(proxy_name, "latency", toxic_name, attributes=attributes)
            logger.info(
                f"Set latency on '{proxy_name}' to {latency_ms}ms (jitter: {jitter_ms}ms)"
            )
        else:
            logger.info(f"Latency on '{proxy_name}' removed (delay was 0).")

    def set_unavailable(self, proxy_name: str, toxic_name: str = "unavailable_toxic"):
        """Makes the proxy unavailable by adding a timeout toxic that never returns."""
        self.remove_toxic(proxy_name, toxic_name)
        attributes = {
            "timeout": 0
        }  # A timeout of 0 should effectively kill the connection
        self.add_toxic(proxy_name, "timeout", toxic_name, attributes=attributes)
        logger.info(f"Proxy '{proxy_name}' set to unavailable.")

    def set_available(self, proxy_name: str, toxic_name: str = "unavailable_toxic"):
        """Makes the proxy available again by removing the timeout toxic."""
        self.remove_toxic(proxy_name, toxic_name)
        logger.info(f"Proxy '{proxy_name}' set to available.")

    def ensure_proxy(self, name: str, listen_addr: str, upstream_addr: str):
        """Ensures a proxy exists with the correct configuration, creating or updating it if necessary."""
        proxy = self.get_proxy(name)

        if not proxy:
            logger.info(f"Proxy '{name}' not found. Creating it...")
            proxy = self.create_proxy(name, listen_addr, upstream_addr)

            if proxy:
                logger.info(f"Proxy '{name}' created: {listen_addr} -> {upstream_addr}")
            else:
                logger.error(f"Failed to create proxy '{name}'")
                raise RuntimeError(f"Could not create toxiproxy proxy '{name}'")
        else:
            # Check if proxy needs to be updated
            current_listen = proxy.get("listen", "")
            current_upstream = proxy.get("upstream", "")
            if current_listen != listen_addr or current_upstream != upstream_addr:
                logger.info(f"Proxy '{name}' configuration mismatch. Recreating...")
                logger.info(f"  Current: {current_listen} -> {current_upstream}")
                logger.info(f"  Desired: {listen_addr} -> {upstream_addr}")
                self.delete_proxy(name)
                proxy = self.create_proxy(name, listen_addr, upstream_addr)
                if proxy:
                    logger.info(
                        f"Proxy '{name}' recreated: {listen_addr} -> {upstream_addr}"
                    )
                else:
                    logger.error(f"Failed to recreate proxy '{name}'")
                    raise RuntimeError(f"Could not recreate toxiproxy proxy '{name}'")
            else:
                logger.debug(
                    f"Proxy '{name}' already exists with correct configuration."
                )
        # Ensure all toxics are cleared before starting
        self.reset_proxy(name)

    def reset_proxy(self, proxy_name: str):
        """Removes all toxics from a proxy."""
        proxy_details = self.get_proxy(proxy_name)
        if proxy_details and "toxics" in proxy_details:
            for toxic in proxy_details["toxics"]:
                self.remove_toxic(proxy_name, toxic["name"])
        logger.debug(f"Reset all toxics on proxy '{proxy_name}'.")
