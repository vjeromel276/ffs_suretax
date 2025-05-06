import argparse
import logging
import psycopg2
import json
import xml.etree.ElementTree as ET
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("suretax_prod_processor")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

conn_kwargs = {
    "dbname": "GLC",
    "user": "oss_server",
    "password": "3wU3uB28X?!r2?@ebrUg",
    "host": "pg01.comlink.net",
    "port": 5432,
    "cursor_factory": RealDictCursor,
    "connect_timeout": 10,
}


def parse_response(body):
    if isinstance(body, dict):
        return body
    if isinstance(body, str):
        try:
            root = ET.fromstring(body)
            return json.loads(root.text)
        except Exception as e:
            raise ValueError(f"Failed to parse XML: {e}")
    raise TypeError("response_body must be str or dict")


def inspect_transaction(cur, transaction_id):
    cur.execute(
        """
        SELECT transaction_id, created, response_body
        FROM biller.suretax_post_response
        WHERE transaction_id = %s
    """,
        (transaction_id,),
    )
    row = cur.fetchone()

    if not row:
        logger.warning(f"No record found for transaction_id: {transaction_id}")
        return

    try:
        parsed = parse_response(row["response_body"])
        logger.info(f"📦 Full parsed response for {transaction_id}:")
        print(json.dumps(parsed, indent=2))
    except Exception as e:
        logger.error(f"Failed to parse response_body: {e}")


def delete_existing(cur, tx_id):
    logger.info(f"Purging existing rows for transaction_id {tx_id}...")
    cur.execute(
        "DELETE FROM biller.suretax_tax_calc_log WHERE suretax_tax_log_id IN (SELECT suretax_tax_log_id FROM biller.suretax_tax_log WHERE suretax_item_log_id IN (SELECT suretax_item_log_id FROM biller.suretax_item_log WHERE transaction_id = %s))",
        (tx_id,),
    )
    cur.execute(
        "DELETE FROM biller.suretax_tax_log WHERE suretax_item_log_id IN (SELECT suretax_item_log_id FROM biller.suretax_item_log WHERE transaction_id = %s)",
        (tx_id,),
    )
    cur.execute(
        "DELETE FROM biller.suretax_item_log WHERE transaction_id = %s", (tx_id,)
    )
    cur.execute(
        "DELETE FROM biller.suretax_transaction_log WHERE transaction_id = %s", (tx_id,)
    )


def get_tx_rows_for_cycle(cur, cycle_log_id):
    cur.execute(
        """
        WITH selected_cycle AS (SELECT %s::int AS cycle_log_id),
        combined_results AS (
            SELECT csc.suretax_transaction_id
            FROM biller.cycle_service_charges csc
            JOIN selected_cycle sc ON csc.cycle_log_id = sc.cycle_log_id
            LEFT JOIN biller.suretax_transaction_log stl ON csc.suretax_transaction_id = stl.transaction_id
            WHERE csc.suretax_transaction_id IS NOT NULL AND csc.amt > 0 AND stl.transaction_id IS NULL
            UNION ALL
            SELECT csa.suretax_transaction_id
            FROM biller.cycle_service_adjustments csa
            JOIN selected_cycle sc ON csa.cycle_log_id = sc.cycle_log_id
            LEFT JOIN biller.suretax_transaction_log stl ON csa.suretax_transaction_id = stl.transaction_id
            WHERE csa.suretax_transaction_id IS NOT NULL AND csa.amt > 0 AND stl.transaction_id IS NULL
            UNION ALL
            SELECT cotc.suretax_transaction_id
            FROM biller.cycle_one_time_charges cotc
            JOIN selected_cycle sc ON cotc.cycle_log_id = sc.cycle_log_id
            LEFT JOIN biller.suretax_transaction_log stl ON cotc.suretax_transaction_id = stl.transaction_id
            WHERE cotc.suretax_transaction_id IS NOT NULL AND cotc.amt > 0 AND stl.transaction_id IS NULL
        )
        SELECT r.transaction_id, r.response_body
        FROM biller.suretax_post_response r
        JOIN combined_results cr ON r.transaction_id = cr.suretax_transaction_id
        ORDER BY r.created;
    """,
        (cycle_log_id,),
    )
    return cur.fetchall()


def insert_transaction(cur, tx, doc_id=None):
    cur.execute(
        """
        INSERT INTO biller.suretax_transaction_log (
            transaction_id, business_unit, client_number,
            client_tracking, data_month, data_year,
            response_code, document_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            tx["TransId"],
            tx.get("BusinessUnit", "OSS"),
            tx.get("ClientNumber", "164753897"),
            tx.get("ClientTracking"),
            tx.get("DataMonth", "05"),
            tx.get("DataYear", "2025"),
            tx.get("ResponseCode"),
            doc_id,
        ),
    )


def insert_items_and_taxes(cur, response):
    item_mapping = {}
    tax_mapping = {}

    for item in response.get("ItemList", []):
        cur.execute("SELECT nextval('biller.suretax_item_log_suretax_item_log_id_seq')")
        item_log_id = cur.fetchone()["nextval"]
        item_mapping[item["ItemID"]] = item_log_id

        cur.execute(
            """
            INSERT INTO biller.suretax_item_log (
                suretax_item_log_id, invoice_number, item_id, line_number,
                customer_number, service_description, service_group_description,
                revenue, fee, tax, tax_on_tax, transaction_id,
                transaction_type_cd, units, geocode, city_nm,
                county_nm, state_cd, zip_code, plus4,
                product_group, product_item
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                item_log_id,
                item.get("InvoiceNumber"),
                item.get("ItemID"),
                item.get("LineNumber"),
                item.get("CustomerNumber"),
                item.get("ServiceDescription"),
                item.get("ServiceGroupDescription"),
                item.get("Revenue"),
                item.get("Fee", 0.0),
                item.get("Tax", 0.0),
                item.get("TaxonTax", 0.0),
                response["TransId"],
                item.get("TransTypeCode"),
                item.get("Units"),
                item.get("Geocode"),
                item.get("CityName"),
                item.get("CountyName"),
                item.get("StateCode"),
                item.get("ZipCode"),
                item.get("Plus4"),
                item.get("ProductGroup"),
                item.get("ProductItem"),
            ),
        )

    for tax in response.get("TaxList", []):
        item_log_id = item_mapping.get(tax["ItemID"])
        if not item_log_id:
            continue

        cur.execute("SELECT nextval('biller.suretax_tax_log_suretax_tax_log_id_seq')")
        tax_log_id = cur.fetchone()["nextval"]

        cur.execute(
            """
            INSERT INTO biller.suretax_tax_log (
                suretax_tax_log_id, suretax_item_log_id, tax_id,
                detailed_tax_desc, fee_rate, percent_taxable,
                tax_amt, tax_authority_nm, tax_authority_type,
                tax_cat, tax_rate, tax_type, tax_type_desc,
                tax_on_tax_amt, tier
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s)
        """,
            (
                tax_log_id,
                item_log_id,
                tax.get("TaxID"),
                tax.get("DetailedTaxDesc"),
                tax.get("FeeRate"),
                tax.get("PercentTaxable"),
                tax.get("TaxAmt"),
                tax.get("TaxAuthorityName"),
                tax.get("TaxAuthorityType"),
                tax.get("TaxCat"),
                tax.get("TaxRate"),
                tax.get("TaxType"),
                tax.get("TaxTypeDesc"),
                tax.get("TaxonTaxAmt"),
                tax.get("Tier"),
            ),
        )

    for calc in response.get("TaxCalcLog", []):
        key = (calc.get("ItemID"), calc.get("TaxID"), calc.get("Tier"))
        tax_log_id = tax_mapping.get(key)
        if not tax_log_id:
            continue

        cur.execute(
            """
            INSERT INTO biller.suretax_tax_calc_log (
                suretax_tax_log_id, log_id, max_tax, max_tax_base,
                max_tax_base_non_taxable_revenue, max_tax_non_taxable_amt,
                max_tax_non_taxable_revenue, min_tax_base,
                min_tax_base_non_taxable_revenue, round, tax,
                tax_auth_id, tax_base, tax_cat, tax_rate,
                tax_source, tax_type, tier, unit_base
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s)
        """,
            (
                tax_log_id,
                calc.get("LogID"),
                calc.get("MaxTax"),
                calc.get("MaxTaxBase"),
                calc.get("MaxTaxBaseNonTaxableRevenue"),
                calc.get("MaxTaxNonTaxableAmount"),
                calc.get("MaxTaxNonTaxableRevenue"),
                calc.get("MinTaxBase"),
                calc.get("MinTaxBaseNonTaxableRevenue"),
                calc.get("Round"),
                calc.get("Tax"),
                calc.get("TaxAuthID"),
                calc.get("TaxBase"),
                calc.get("TaxCat"),
                calc.get("TaxRate"),
                "|".join(filter(None, calc.get("TaxSource", "").split())),
                calc.get("TaxType"),
                calc.get("Tier"),
                calc.get("UnitBase"),
            ),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycle_log_id", type=int, required=False)
    parser.add_argument("--reprocess", action="store_true")
    parser.add_argument("--inspect_transaction_id", type=str)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    conn = psycopg2.connect(**conn_kwargs)
    cur = conn.cursor()

    if args.inspect_transaction_id:
        inspect_transaction(cur, args.inspect_transaction_id)
        cur.close()
        conn.close()
        return

    if not args.cycle_log_id:
        logger.error(
            "Missing required --cycle_log_id when not using --inspect_transaction_id"
        )
        return

    rows = get_tx_rows_for_cycle(cur, args.cycle_log_id)
    logger.info(f"Found {len(rows)} SureTax responses to process.")

    for row in rows:
        tx_id = row["transaction_id"]
        try:
            response = parse_response(row["response_body"])
            if args.reprocess:
                delete_existing(cur, tx_id)

            insert_transaction(cur, response)
            insert_items_and_taxes(cur, response)
            conn.commit()
            logger.info(f"✔ Transaction {tx_id} inserted into biller schema.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error processing {tx_id}: {e}")

    cur.close()
    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
