"""Safe HTTP failure mapping for routes that use required credential storage."""

from fastapi import HTTPException, Request
from fastapi.routing import APIRoute

from omnigent.stores.credential_store.backend import CredentialBackendUnavailable


class CredentialBackendRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def credential_route(request: Request):
            try:
                return await handler(request)
            except CredentialBackendUnavailable as exc:
                raise HTTPException(503, str(exc), headers={"Retry-After": "5"}) from None

        return credential_route
