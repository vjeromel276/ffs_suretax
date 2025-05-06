# /suretax_middleware.py

import requests
import urllib.parse
import json
from typing import Optional, Dict, Any, Union
import xml.etree.ElementTree as ET


class SureTaxAPI:
    def __init__(self, client_number: str, validation_key: str, environment: str = "CERT"):
        self.client_number = client_number
        self.validation_key = validation_key
        self.environment = environment.upper()
        
        if self.environment == 'PRODUCTION':
            self.base_url = "https://api.taxrating.net"
        else:
            self.base_url = "https://testapi.taxrating.net"

    def _request(self, method: str, endpoint: str, payload: dict, raw=False) -> Union[str, dict]:
        url = f"{self.base_url}/Services/V07/SureTax.asmx/{endpoint}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        encoded_json = urllib.parse.quote(json.dumps(payload), safe='')
        body = f"request={encoded_json}"
        response = requests.post(url, headers=headers, data=body)

        if response.status_code != 200:
            raise Exception(f"HTTP Error: {response.status_code}, {response.text}")

        if raw:
            return response.content  # XML string
        else:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            return json.loads(root.text)

    def calculate_tax(self, data: Dict[str, Any]) -> dict:
        endpoint = "PostRequest"
        default_payload = {
            "ClientNumber": self.client_number,
            "ValidationKey": self.validation_key,
            "BusinessUnit": data.get("BusinessUnit", ""),
            "DataYear": data.get("DataYear"),
            "DataMonth": data.get("DataMonth"),
            "CmplDataYear": data.get("CmplDataYear", data.get("DataYear")),
            "CmplDataMonth": data.get("CmplDataMonth", data.get("DataMonth")),
            "TotalRevenue": data.get("TotalRevenue"),
            "ClientTracking": data.get("ClientTracking", ""),
            "ResponseType": data.get("ResponseType", "2D5"),
            "ResponseGroup": data.get("ResponseGroup", "00"),
            "ReturnFileCode": data.get("ReturnFileCode", "Q"),
            "STAN": data.get("STAN", ""),
            "MasterTransID": data.get("MasterTransID", "0"),
            "ItemList": data.get("ItemList", [])
        }
        return self._request('POST', endpoint, default_payload)

    def cancel_transaction(self, trans_id: int, client_tracking: Optional[str] = None) -> dict:
        endpoint = "CancelPostRequest"
        payload = {
            "ClientNumber": self.client_number,
            "ValidationKey": self.validation_key,
            "TransId": trans_id,
            "ClientTracking": client_tracking if client_tracking else ""
        }
        return self._request('POST', endpoint, payload)

    def finalize_transaction(self, master_trans_id: int, client_tracking: Optional[str] = None) -> dict:
        endpoint = "FinalizePostRequest"
        payload = {
            "ClientNumber": self.client_number,
            "ValidationKey": self.validation_key,
            "MasterTransID": master_trans_id,
            "ClientTracking": client_tracking if client_tracking else ""
        }
        return self._request('POST', endpoint, payload)

    def tax_adjustment(self, data: Dict[str, Any]) -> dict:
        endpoint = "PostTaxAdjustmentRequest"
        default_payload = {
            "ClientNumber": self.client_number,
            "ValidationKey": self.validation_key,
            "BusinessUnit": data.get("BusinessUnit", ""),
            "DataYear": data.get("DataYear"),
            "DataMonth": data.get("DataMonth"),
            "CmplDataYear": data.get("CmplDataYear", data.get("DataYear")),
            "CmplDataMonth": data.get("CmplDataMonth", data.get("DataMonth")),
            "ClientTracking": data.get("ClientTracking", "TaxAdjust"),
            "ResponseType": data.get("ResponseType", "2D5"),
            "ResponseGroup": data.get("ResponseGroup", "00"),
            "STAN": data.get("STAN", ""),
            "MasterTransID": data.get("MasterTransID", "0"),
            "TaxAdjustmentItemList": data.get("TaxAdjustmentItemList", [])
        }
        return self._request('POST', endpoint, default_payload)


# ✅ FIXED: Move submit_tax_request OUT of the class
def submit_tax_request(kind: str, items: list, context: dict, commit=True) -> dict:
    """
    Unified entry point for sending tax requests to SureTax.
    Kind: 'OneTime', 'ServiceCharges', 'SAB', 'Usage'
    Context keys:
        - bill_date (datetime.date)
        - client_number, validation_key
        - client_tracking
        - return_file_code (default: "Q")
        - business_unit (optional override)
        - pg_cursor (for inserts)
        - pg_conn (for commit)
    """
    suretax = SureTaxAPI(
        client_number=context["client_number"],
        validation_key=context["validation_key"],
        environment=context.get("environment", "CERT")
    )

    payload = {
        "BusinessUnit": context.get("business_unit", f"BCR-{kind}"),
        "DataMonth": str(context["bill_date"].month),
        "DataYear": str(context["bill_date"].year),
        "TotalRevenue": sum(float(x.get("Revenue", 0)) for x in items),
        "ReturnFileCode": context.get("return_file_code", "Q"),
        "ClientTracking": context["client_tracking"],
        "IndustryExemption": "",
        "ResponseGroup": "01",
        "ResponseType": "D5",
        "ItemList": items
    }

    response = suretax.calculate_tax(payload)

    if context.get("pg_cursor") and commit:
        from store_suretax_response import store_response  # Lazy import
        store_response(context["pg_cursor"], response, data_month=payload["DataMonth"], data_year=payload["DataYear"])
        context["pg_conn"].commit()

    return response
