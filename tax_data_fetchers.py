from datetime import datetime
from db_utils import get_db_conn

def build_billing_address(zip_code: str) -> dict:
    # Minimal improvement: safe fallback to 'US'
    return {
        "PrimaryAddressLine": "N/A",
        "City": "Cleveland",
        "State": "OH",
        "PostalCode": zip_code or "45414",
        "Country": "US",
        "VerifyAddress": 0
    }

def get_one_time_items(cycle_log_id, rerun=False):
    query = """
    SELECT
        c1.cycle_one_time_charge_id,
        c1.account_id,
        c1.orig_zip AS a_zip,
        c1.dest_zip AS z_zip,
        c1.amt,
        c1.suretax_units,
        c1.suretax_transaction_type_cd AS trans_type_code,
        c1.from_date
    FROM biller.cycle_one_time_charges c1
    WHERE c1.cycle_log_id = %s
      AND (c1.suretax_transaction_id IS NULL OR %s = TRUE)
      AND c1.suretax_transaction_type_cd IS NOT NULL
    """

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(query, (cycle_log_id, rerun))
    rows = cur.fetchall()
    conn.close()

    items = []
    for row in rows:
        if row["amt"] == 0:
            continue

        trans_date = row["from_date"].strftime("%Y-%m-01T00:00:00")
        revenue = float(row["amt"])
        if row["z_zip"]:
            revenue /= 2

        if row["trans_type_code"] in ("000000","999999"):
            print(f"⚠️ WARNING: trans_type_code: {row["trans_type_code"]} for account_id: {row["account_id"]}")
            row["trans_type_code"] = "060101"
        
        zip_code = (row["a_zip"] or row["z_zip"] or "")[:5]
        billing_address = build_billing_address(zip_code)
        use_p2p = row["z_zip"] and row["a_zip"] and row["a_zip"] != row["z_zip"]

        item = {
            "LineNumber": str(row["cycle_one_time_charge_id"]),
            "InvoiceNumber": f"{row['account_id']}-{cycle_log_id}",
            "CustomerNumber": str(row["account_id"]),
            "Revenue": revenue,
            "Units": row["suretax_units"],
            "UnitType": "00",
            "Seconds": "0",
            "TaxSitusRule": "17" if use_p2p else "04",
            "TransTypeCode": row["trans_type_code"],
            "SalesTypeCode": "B",
            "RegulatoryCode": "02",
            "TransDate": trans_date,
            "Zipcode": zip_code,
            "Plus4": "",
            "P2PZipcode": row["z_zip"][:5] if use_p2p else "",
            "P2PPlus4": "",
            "TaxExemptionCodeList": [],
            "TaxIncludedCode": "0",
            "BillingAddress": billing_address
        }
        items.append(item)

    return items

def get_service_items(cycle_log_id, rerun=True):
    query = """
    SELECT
        csc.cycle_service_charge_id,
        csc.account_id,
        csc.amt,
        csc.a_zip,
        csc.z_zip,
        csc.suretax_units,
        csc.suretax_transaction_type_cd AS trans_type_code,
        csc.charge_type_cd,
        csc.from_date
    FROM biller.cycle_service_charges csc
    WHERE csc.cycle_log_id = %s
      AND (csc.suretax_transaction_id IS NULL OR %s = TRUE)
      AND csc.suretax_transaction_type_cd IS NOT NULL
    """

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(query, (cycle_log_id, rerun))
    rows = cur.fetchall()
    conn.close()

    items = []
    for row in rows:
        if row["amt"] == 0:
            continue

        trans_date = row["from_date"].strftime("%Y-%m-01T00:00:00")
        revenue = float(row["amt"])
        if row["z_zip"] and row["trans_type_code"] not in ("070251", "070226", "070249"):
            revenue /= 2

        if row["trans_type_code"] in ("000000","999999"):
            print(f"⚠️ WARNING: trans_type_code: {row["trans_type_code"]} for account_id: {row["account_id"]}")
            row["trans_type_code"] = "060101"
        
        zip_code = (row["a_zip"] or row["z_zip"] or "")[:5]
        billing_address = build_billing_address(zip_code)
        use_p2p = row["z_zip"] and row["a_zip"] and row["a_zip"] != row["z_zip"]

        item = {
            "LineNumber": str(row["cycle_service_charge_id"]),
            "InvoiceNumber": f"{row['account_id']}-{cycle_log_id}-{row['charge_type_cd']}",
            "CustomerNumber": str(row["account_id"]),
            "Revenue": revenue,
            "Units": row["suretax_units"],
            "UnitType": "00",
            "Seconds": "0",
            "TaxSitusRule": "17" if use_p2p else "04",
            "TransTypeCode": row["trans_type_code"],
            "SalesTypeCode": "B",
            "RegulatoryCode": "02",
            "TransDate": trans_date,
            "Zipcode": zip_code,
            "Plus4": "",
            "P2PZipcode": row["z_zip"][:5] if use_p2p else "",
            "P2PPlus4": "",
            "TaxExemptionCodeList": [],
            "TaxIncludedCode": "0",
            "BillingAddress": billing_address
        }
        items.append(item)

    return items

def get_sab_items(cycle_log_id, rerun=False):
    query = """
    SELECT
        csra.cycle_sab_rate_adjustment_id,
        csra.account_id,
        csra.amt,
        csra.a_zip,
        csra.z_zip,
        csra.suretax_units,
        csra.suretax_transaction_type_cd AS trans_type_code,
        csra.from_date
    FROM biller.cycle_sab_rate_adjustments csra
    WHERE csra.cycle_log_id = %s
      AND (csra.suretax_transaction_id IS NULL OR %s = TRUE)
      AND csra.suretax_transaction_type_cd IS NOT NULL
    """

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(query, (cycle_log_id, rerun))
    rows = cur.fetchall()
    conn.close()

    items = []
    for row in rows:
        if row["amt"] == 0:
            continue

        trans_date = row["from_date"].strftime("%Y-%m-01T00:00:00")
        revenue = float(row["amt"])
        
        if row["z_zip"] and row["trans_type_code"] not in ("070251", "070226", "070249"):
            revenue /= 2
            
        if row["trans_type_code"] in ("000000","999999"):
            print(f"⚠️ WARNING: trans_type_code: {row["trans_type_code"]} for account_id: {row["account_id"]}")
            row["trans_type_code"] = "060101"
        
        zip_code = (row["a_zip"] or row["z_zip"] or "")[:5]
        billing_address = build_billing_address(zip_code)
        use_p2p = row["z_zip"] and row["a_zip"] and row["a_zip"] != row["z_zip"]

        item = {
            "LineNumber": str(row["cycle_sab_rate_adjustment_id"]),
            "InvoiceNumber": f"{row['account_id']}-{cycle_log_id}-SAB",
            "CustomerNumber": str(row["account_id"]),
            "Revenue": revenue,
            "Units": row["suretax_units"],
            "UnitType": "00",
            "Seconds": "0",
            "TaxSitusRule": "17" if use_p2p else "04",
            "TransTypeCode": row["trans_type_code"],
            "SalesTypeCode": "B",
            "RegulatoryCode": "02",
            "TransDate": trans_date,
            "Zipcode": zip_code,
            "Plus4": "",
            "P2PZipcode": row["z_zip"][:5] if use_p2p else "",
            "P2PPlus4": "",
            "TaxExemptionCodeList": [],
            "TaxIncludedCode": "0",
            "BillingAddress": billing_address
        }
        items.append(item)

    return items

def get_usage_items(cycle_log_id, rerun=False):
    query = """
    SELECT
        cucd.cycle_usage_charge_detail_id,
        cuc.account_id,
        cucd.amt,
        cuc.from_date
    FROM biller.cycle_usage_charges cuc
    JOIN biller.cycle_usage_charge_details cucd USING (cycle_usage_charge_id)
    WHERE cuc.cycle_log_id = %s
      AND usage_charge_type_cd = 'spla'
      AND (cucd.suretax_transaction_id IS NULL OR %s = TRUE)
    """

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(query, (cycle_log_id, rerun))
    rows = cur.fetchall()
    conn.close()

    items = []
    for row in rows:
        if row["amt"] == 0:
            continue

        trans_date = row["from_date"].strftime("%Y-%m-01T00:00:00")
        revenue = float(row["amt"])
        zip_code = "49546"
        billing_address = build_billing_address(zip_code)

        item = {
            "LineNumber": str(row["cycle_usage_charge_detail_id"]),
            "InvoiceNumber": f"{row['account_id']}-{cycle_log_id}-USG",
            "CustomerNumber": str(row["account_id"]),
            "Revenue": revenue,
            "Units": 1,
            "UnitType": "00",
            "Seconds": "0",
            "TaxSitusRule": "04",
            "TransTypeCode": "210406",
            "SalesTypeCode": "B",
            "RegulatoryCode": "02",
            "TransDate": trans_date,
            "Zipcode": zip_code,
            "Plus4": "",
            "P2PZipcode": "",
            "P2PPlus4": "",
            "TaxExemptionCodeList": [],
            "TaxIncludedCode": "0",
            "BillingAddress": billing_address
        }
        items.append(item)

    return items
