from datetime import datetime, timedelta
from typing import Optional

import requests
from airflow.hooks.base_hook import BaseHook
from tenacity import after_log, retry, stop_after_attempt, wait_fixed

from grow_with_the_flow.utils.gcp import query_bigquery
from grow_with_the_flow.utils.pythos import get_sql_filepath


# Inventory Entity modes that require ids
ENTITY_MODE_ID_REQ = {
    "invoice": [
        "update",
        "get",
        "draft",
        "submit",
        "approve",
        "void",
        "sent",
        "delete",
    ],
    "item": ["get", "delete", "update"],
}


ZOHO_API_METHODS = [
    "compositeitems",
    "creditnotes",
    "contacts",
    "inventoryadjustments",
    "itemdetails",
    "invoices",
    "items",
    "purchaseorders",
    "salesorders",
    "pricebooks",
]


class ZohoInventoryHook(BaseHook):
    def __init__(self, conn_id="zoho_inventory_oauth"):
        self.endpoint = "https://inventory.zoho.eu/api/v1/"
        self._token_manager = None
        self.conn_id = conn_id
        self.token = {"token": None, "expires_at": datetime(2019, 1, 1, 0, 0, 0, 0)}

    def get_conn(self):
        return self.get_connection(self.conn_id)

    def get_params(self, org_id):
        return {"organization_id": org_id}

    def get_token(self) -> dict:
        if (datetime.utcnow() + timedelta(seconds=300)) < self.token.get("expires_at"):
            return self.token
        else:
            self.log.info("Acquiring OAuth Token")
            exported_token = query_bigquery(
                get_sql_filepath("export_zoho_oauth_token.sql")
            )
            self.token = eval(exported_token["token"].iloc[0])
            self.token["expires_at"] = datetime.strptime(
                self.token["expires_at"], "%Y-%m-%d %H:%M:%S.%f"
            )
            return self.token

    def get_headers(self) -> dict:
        return {"Authorization": f"Zoho-oauthtoken {self.get_token().get('token')}"}

    def get_url(self, method: str, uuid: Optional[str] = None) -> str:
        if unit is not None:
            if method == "salesorder_lineitems":
                url = f"{self.endpoint}salesorders/{uuid}"
            elif method == "invoice_lineitems":
                url = f"{self.endpoint}invoices/{uuid}"
            elif method == "creditnote_lineitems":
                url = f"{self.endpoint}creditnotes/{uuid}"
            elif method == "contact_lineitems":
                url = f"{self.endpoint}contacts/{uuid}"
            elif method == "contact_persons":
                url = f"{self.endpoint}contacts/{uuid}/contact_persons"
            elif method == "inventory_adjustments_lineitems":
                url = f"{self.endpoint}inventoryadjustments/{uuid}"
            elif method == "bundles":
                url = f"{self.endpoint}compositeitems/{uuid}"
            else:
                raise ValueError("Get Url method is not Defined")
        elif method in ZOHO_API_METHODS:
            url = self.endpoint + method
        else:
            raise ValueError("Not a valid Zoho Inventory Api Method")
        return url

    @retry(
        wait=wait_fixed(300),
        stop=stop_after_attempt(3),
        after=after_log(logger, logging.DEBUG),
    )
    def make_request(
        self,
        org_id: str,
        method: str,
        page: int = 1,
        mode: str = "get",
        unit: Optional[str] = None,
        url: Optional[str] = None,
        params: Optional[dict] = None,
        file: Optional[dict] = None,
    ) -> dict:
        """
        :param unit: either salesorder_id for salesorder Or invoice_id for invoices
        :param url: standard url defined in get_urls or a custom url defined by function
        :param params: standard params or specialized defined by functions below
        """
        # Create a standard url if no url provided
        if url is None:
            url = self.get_url(method=method, unit=unit)
        # Apply Standard Pagination Params if no custom params
        if params is None and unit is None:
            params = {
                "organization_id": org_id,
                "page": page,
                "per_page": 200,
                "sort_column": "last_modified_time",
                "sort_order": "D",
            }
        elif unit is not None and params is None:
            params = self.get_params(org_id)
        self.log.info(f"Request Parameters: {params}")
        # Special case got getting image files
        stream_state = True if mode == "get" and method == "image" else False
        # Make request
        response = requests.request(
            method=mode.upper(),
            url=url,
            params=params,
            headers=self.get_headers(),
            files=file,
            stream=stream_state,
        )

        if method in ["get_pdf", "image"]:
            return response
        else:
            data = response.json()
            # Trigger Tenacity Retry for invalid token
            # code 57 means request was not authorized/denied
            if data.get("code") in [2, 57]:
                response.raise_for_status()
            return data

    def invoice(
        self,
        org_id: str,
        invoice_id: Optional[str] = None,
        updates=None,
        mode: Optional[str] = "get",
    ) -> dict:
        params = self.get_params(org_id)
        if mode in ENTITY_MODE_ID_REQ["invoice"] and not invoice_id:
            raise ValueError(f"invoice id not provided for mode {mode} when required")

        if mode == "update":
            params["oauthscope"] = "ZohoInventory.invoices.UPDATE"
            params["JSONString"] = str(updates)
            url = f"{self.endpoint}invoices/{invoice_id}"
            request_method = "put"
        elif mode == "get":
            params["oauthscope"] = "ZohoInventory.invoices.READ"
            params["accept"] = "pdf"
            url = f"{self.endpoint}invoices/{invoice_id}"
            request_method = "get"
        elif mode == "create":
            params["oauthscope"] = "ZohoInventory.invoices.CREATE"
            params["send"] = False
            params["ignore_auto_number_generation"] = True
            params["JSONString"] = str(updates)
            url = f"{self.endpoint}invoices"
            request_method = "post"
        elif mode == "draft":
            url = f"{self.endpoint}invoices/{invoice_id}/status/draft"
            params["oauthscope"] = "ZohoInventory.invoices.CREATE"
            request_method = "post"
        elif mode == "submit":
            url = f"{self.endpoint}invoices/{invoice_id}/submit"
            request_method = "post"
        elif mode == "approve":
            url = f"{self.endpoint}invoices/{invoice_id}/approve"
            request_method = "post"
        elif mode == "void":
            url = f"{self.endpoint}invoices/{invoice_id}/status/void"
            params["oauthscope"] = "ZohoInventory.invoices.CREATE"
            request_method = "post"
        elif mode == "sent":
            url = f"{self.endpoint}invoices/{invoice_id}/status/sent"
            params["oauthscope"] = "ZohoInventory.invoices.CREATE"
            request_method = "post"
        elif mode == "delete":
            url = f"{self.endpoint}invoices/{invoice_id}"
            params["oauthscope"] = "ZohoInventory.invoices.DELETE"
            request_method = "delete"
        else:
            raise ValueError(f"method {mode} not defined")
        response = self.make_request(
            org_id=org_id, method="invoice", mode=request_method, url=url, params=params
        )
        self.log.info(f"url: {url}")
        self.log.info(f"Invoice Mode: {mode} Response Code: {response.get('message')}")
        return response

    def inv_adjustments(
        self, org_id: str, data: dict, record: Optional[str] = None
    ) -> dict:
        params = self.get_params(org_id)
        params["JSONString"] = str(data)
        if record:
            url = f"{self.endpoint}inventoryadjustments/{record}"
            params["oauthscope"] = "ZohoInventory.inventoryadjustments.UPDATE"
            request_method = "put"
        else:
            url = f"{self.endpoint}inventoryadjustments"
            params["oauthscope"] = "ZohoInventory.inventoryadjustments.CREATE"
            request_method = "post"
        response = self.make_request(
            org_id=org_id,
            method="inv_adjustments",
            mode=request_method,
            url=url,
            params=params,
        )
        self.log.info(f"Response Code: {response.get('message')}")
        return response

    def get_item(self, org_id: str, item: str) -> dict:
        url = f"{self.endpoint}items/{item}"
        response = self.make_request(
            org_id=org_id,
            method="get_item",
            mode="get",
            url=url,
            params=self.get_params(org_id),
        )
        self.log.info(f"url: {url}")
        self.log.info(f"message: {response['message']}")
        return response

    def item(
        self,
        org_id: str,
        mode: str,
        item: Optional[str] = None,
        updates: Optional[dict] = None,
    ) -> dict:
        params = self.get_params(org_id)
        params["JSONString"] = str(updates)

        if mode in ENTITY_MODE_ID_REQ["item"] and not item:
            raise ValueError(f"item id not provided for mode {mode} when required")

        if mode == "create":
            url = f"{self.endpoint}items"
            params["oauthscope"] = "ZohoInventory.items.CREATE"
            request_method = "post"

        elif mode in ENTITY_MODE_ID_REQ["item"]:
            url = f"{self.endpoint}items/{item}"
            if mode == "get" and item:
                params["oauthscope"] = "ZohoInventory.items.READ"
                request_method = "get"
            elif mode == "delete" and item:
                params["oauthscope"] = "ZohoInventory.items.DELETE"
                request_method = "delete"
            elif mode == "update" and item:
                params["oauthscope"] = "ZohoInventory.items.UPDATE"
                request_method = "put"
        else:
            raise ValueError(f"item method {mode} not valid")
        response = self.make_request(
            org_id=org_id, method="item", mode=request_method, url=url, params=params
        )
        self.log.info(f"url: {url}")
        self.log.info(f"Response Code: {response.get('message')}")
        return response

    def post_item_status(
        self, org_id: str, item_id: str, mode: Optional[str] = "active"
    ) -> dict:
        """
        :param mode: either active or inactive
        """
        if mode.lower() == "active":
            url = f"{self.endpoint}items/{item_id}/active"
        elif mode.lower() == "inactive":
            url = f"{self.endpoint}items/{item_id}/inactive"
        else:
            raise ValueError(f"incorrect item status: {mode}")

        params = self.get_params(org_id)
        params["oauthscope"] = "ZohoInventory.items.UPDATE"
        response = self.make_request(
            org_id=org_id,
            method="post_item_status",
            mode="post",
            url=url,
            params=params,
        )
        self.log.info(f"url: {url}")
        self.log.info(f"Response Code: {response.get('message')}")
        return response

    def get_item_details(self, org_id: str, items: str) -> dict:
        params = self.get_params(org_id)
        url = self.get_url("itemdetails")
        params["item_ids"] = items
        response = self.make_request(
            org_id=org_id,
            method="get_item_details",
            mode="get",
            url=url,
            params=params,
        )
        self.log.info(f"url: {url} message: {response.get('message')}")
        return response

    def image(
        self,
        org_id: str,
        item: str,
        mode: Optional[str] = "get",
        file: Optional[str] = None,
    ) -> dict:
        url = f"{self.endpoint}items/{str(item)}/image"
        params = self.get_params(org_id)
        if mode == "get":
            pass
        elif mode == "post":
            try:
                with open(file, "rb") as f:
                    data = f.read()
                file = {"image": (file, data, "image/jpg")}
            except FileNotFoundError:
                raise self.log.error(f"zoho image file {file} not found")
        elif mode == "delete":
            pass
        else:
            raise ValueError("Image mode is not defined")
        response = self.make_request(
            org_id=org_id, method="image", mode=mode, url=url, params=params, file=file
        )
        self.log.info(f"Response URL: {url} Code: {response.status_code}")
        return response

    def get_pdf(self, org_id: str, record_id: str, method=None) -> dict:
        """
        :papoetry ram method: either invoice or creditnote
        """
        if method == "invoice":
            url = f"{self.endpoint}invoices/{record_id}"
        elif method == "creditnote":
            url = f"{self.endpoint}creditnotes/{record_id}"
        else:
            raise ValueError(f"pdf method {method} not valid")

        params = self.get_params(org_id)
        params["oauthscope"] = "ZohoInventory.invoices.READ"
        params["accept"] = "pdf"
        response = self.make_request(
            org_id=org_id, method="get_pdf", mode="get", url=url, params=params
        )
        self.log.info(f"Response URL: {url} Code: {response.status_code}")
        return response

    def purchase_order(self, org_id: str, purchaseorder_id: str) -> dict:
        if purchaseorder_id is not None:
            url = f"{self.endpoint}purchaseorders/{purchaseorder_id}/status/cancelled"
        else:
            raise ValueError(f"invalid purchase order id {purchaseorder_id} provided")

        response = self.make_request(
            org_id=org_id,
            method="purchase_order",
            mode="post",
            url=url,
            params=self.get_params(org_id),
        )
        self.log.info(f"url: {url}")
        self.log.info(f"Response Code: {response.get('message')}")
        return response

    def contacts(
        self,
        org_id: str,
        mode: str = "get",
        contact: Optional[dict] = None,
        contact_id: Optional[str] = None,
    ) -> dict:

        url = None
        params = self.get_params(org_id)
        params["oauthscope"] = "ZohoInventory.contacts.READ"

        if mode == "put":
            url = f"{self.endpoint}contacts/{contact_id}"
            params["oauthscope"] = "ZohoInventory.contacts.UPDATE"
            params["JSONString"] = str(contact)

        elif mode == "post":
            url = f"{self.endpoint}contacts"
            params["oauthscope"] = "ZohoInventory.contacts.CREATE"
            params["JSONString"] = str(contact)

        elif mode == "delete":
            url = f"{self.endpoint}contacts/{contact_id}"
            params["oauthscope"] = "ZohoInventory.contacts.DELETE"

        elif mode == "inactive":
            url = f"{self.endpoint}contacts/{contact_id}/inactive"
            params["oauthscope"] = "ZohoInventory.contacts.CREATE"
            mode = "post"

        response = self.make_request(
            org_id=org_id,
            method="contact_details",
            url=url,
            mode=mode,
            unit=contact_id,
            params=params,
        )

        if contact_id:
            self.log.info(
                f"Response Code for zoho id: {contact_id}: {response.get('message')}"
            )
        else:
            self.log.info(
                f"Response Code for zoho id: {response.get('contact', {}).get('contact_id')}: {response.get('message')}"
            )

        return response

    def contact_persons(
        self,
        org_id: str,
        contact_person: Optional[dict] = None,
        contact_person_id: Optional[str] = None,
        contact_id: Optional[str] = None,
        mode: Optional[str] = "get",
    ) -> dict:
    """
    Method for interacting with contact Persons in Zoho Inventory
    param: contact_id zoho inventory backend_id
    mode: get == Get an existing contact
    mode: get_list == List available contacts. 200 per page
    mode: put == Update an existing contact
    mode: post == Create a new Contact
    mode: set_primary == Create a contact as Primary Contact
    """
        url = None
        params = self.get_params(org_id)
        params["oauthscope"] = "ZohoInventory.contacts.READ"

        if mode == "get":
            params["JSONString"] = str(contact_person)
            url = f"{self.endpoint}contacts/{contact_id}/contactpersons/{contact_person_id}"

        elif mode == "get_list":
            mode = "get"
            params["JSONString"] = str(contact_person)
            url = f"{self.endpoint}contacts/{contact_id}/contactpersons"

        elif mode == "put":
            params["oauthscope"] = "ZohoInventory.contacts.UPDATE"
            params["JSONString"] = str(contact_person)
            url = f"{self.endpoint}contacts/contactpersons/{contact_person_id}"

        elif mode == "post":
            params["oauthscope"] = "ZohoInventory.contacts.CREATE"
            params["JSONString"] = str(contact_person)
            url = f"{self.endpoint}contacts/contactpersons"

        elif mode == "set_primary":
            mode = "post"
            params["oauthscope"] = "ZohoInventory.contacts.CREATE"
            url = f"{self.endpoint}contacts/contactpersons/{contact_person_id}/primary"

        response = self.make_request(
            org_id=org_id,
            method="contactpersons",
            url=url,
            mode=mode,
            params=params,
        )
        if contact_person_id:
            self.log.info(
                f"Response Code for zoho contact person: {contact_person_id}: {response.get('message')}"
            )

        else:
            self.log.info(f"Response Code: {response.get('message')}")

        return response

    def price_list(
        self,
        org_id: str,
        mode: Optional[str] = "get",
    ) -> list:
        url = None
        params = self.get_params(org_id)
        params["oauthscope"] = "ZohoInventory.settings.READ"

        response = self.make_request(
            org_id=org_id,
            method="pricebooks",
            url=url,
            mode=mode,
            params=params,
        )

        self.log.info(f"Response Code: {response.get('message')}")

        return response["pricebooks"]
