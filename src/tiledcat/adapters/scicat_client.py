import base64
import hashlib
import logging
import json
import re
from typing import Optional
from urllib.parse import urljoin, quote_plus

from httpx import AsyncClient, Timeout
from pydantic import BaseModel


logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


class ScicatCommError(Exception):
    """Represents an error encountered during communication with SciCat."""

    def __init__(self, message):
        self.message = message


class AsyncScicatClient():
    """ Async version of the PyScicat client """
    def __init__(        
        self,
        base_url: str,
        token: str = None,
        timeout_seconds: int = None,
        old_backend: bool = False
    ) -> None:
        if base_url[-1] != "/":
            base_url = base_url + "/"
        self.http_client = AsyncClient()
        self.http_client.base_url = base_url
        self.http_client.timeout = Timeout(timeout_seconds) # httpx has many options 
        if old_backend:
            self.headers = {"Authorization": token}
        else:
            self.headers = {"Authorization": f"Bearer {token}"}


    async def _call_endpoint(
        self,
        method: str,
        endpoint: str,
        data: BaseModel = None,
        operation: str = "",
        allow_404=False,
    ) -> Optional[dict]:

        response = await self.http_client.request(method=method, url=endpoint, data=data)
        result = response.json()
        if not response.is_success:
            err = result.get("error", {})
            if (
                allow_404
                and response == 404
                and re.match(r"Unknown (.+ )?id", err.get("message", ""))
            ):
                # The operation failed but because the object does not exist in SciCat.
                logger.error("Error in operation %s: %s", operation, err)
                return None
            raise ScicatCommError(f"Error in operation {operation}: {err}")
        logger.info(
            "Operation '%s' successful%s",
            operation,
            f"pid={result['pid']}" if "pid" in result else "",
        )
        return result

    async def get_dataset_full_facet(self, query):
        endpoint = f'Datasets/fullfacet?fields={{"mode":{{}},"text":"test"}}&facets=["type","creationTime","creationLocation","ownerGroup","keywords"]'
        return await self._call_endpoint(
            method="get",
            endpoint=endpoint,
            operation="get_dataset_full_facet",
            allow_404=True,
        )

    async def datasets_get_one(self, pid: str) -> Optional[dict]:
        """
        Gets dataset with the pid provided.
        This function has been renamed. Provious name has been maintained for backward compatibility.
        Previous names was get_dataset_by_pid

        Parameters
        ----------
        pid : string
            pid of the dataset requested.
        """
        return await self._call_endpoint(
            method="get",
            endpoint=f"Datasets/{quote_plus(pid)}",
            operation="datasets_get_one",
            allow_404=True,
        )
    
    async def datasets_find(
        self, skip: int = 0, limit: int = 25, query_fields: Optional[dict] = None
    ) -> Optional[dict]:
        """
        Gets datasets using the fullQuery mechanism of SciCat. This is
        appropriate for cases where might want paging and cases where you want to perform
        a text search on the Datasets collection. The full features of fullQuery search
        are beyond this document.

        There is no known mechanism to query for fields that are missing or contain a
        a null value.

        To query based on the full text search, send `{"text": "<text to query"}` as query field

        This function was renamed.
        It is still accessible with the original name for backward compatibility
        The original name was find_datasets_full_query and get_datasets_full_query

        Parameters
        ----------
        skip : int
            number of items to skip

        limit : int
            number of items to return

        query_fields : dict
            dictionary of terms to send to the query (must be json serializable)

        """
        if not query_fields:
            query_fields = {}
        query_fields = json.dumps(query_fields)
        query = f'fields={query_fields}&limits={{"skip":{skip},"limit":{limit},"order":"creationTime:desc"}}'

        return await self._call_endpoint(
            method="get",
            endpoint=f"Datasets/fullquery?{query}",
            operation="datasets_find",
            allow_404=True,
        )
    