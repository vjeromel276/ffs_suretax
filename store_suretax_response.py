import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List


oss_conn_kwargs = {
    "dbname": "GLC",
    "user": "oss_server",
    "password": "3wU3uB28X?!r2?@ebrUg",
    "host": "pg01.comlink.net",
    "port": "5432",
    "cursor_factory": RealDictCursor,
    "connect_timeout": 10,
}


def get_oss_conn():
    return psycopg2.connect(**oss_conn_kwargs)


def get_oss_cursor():
    conn = get_oss_conn()

    cur = conn.cursor()
    cur.execute("SET search_path TO vlettau, public")
    return conn, cur


def close_oss_conn(conn, cur):
    cur.close()
    conn.commit()
    conn.close()


def get_tax_log_id(cur, item_log_id: int) -> int:
    cur.execute(
        """
        SELECT suretax_tax_log_id
        FROM vlettau.suretax_tax_log_api
        WHERE suretax_item_log_id = %s
    """,
        (item_log_id,),
    )
    result = cur.fetchone()
    return result["surtax_tax_log_id"] if result else None


def insert_transaction_log(cur, response: Dict, data_month=None, data_year=None, document_id=None):
    cur.execute(
        """
        INSERT INTO vlettau.suretax_transaction_log_api (
            transaction_id,
            business_unit,
            client_number,
            client_tracking,
            data_month,
            data_year,
            response_code,
            document_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            response.get("TransId"),
            response.get("BusinessUnit", "OSS"),
            164753897,
            response.get("ClientTracking"),
            response.get("DataMonth") or data_month,
            response.get("DataYear") or data_year,
            response.get("ResponseCode"),
            document_id,
        ),
    )


def insert_item_log(cur, item: Dict, transaction_id: int) -> int:
    cur.execute(
        "SELECT nextval('vlettau.suretax_item_log_api_suretax_item_log_id_seq') AS id"
    )
    item_id = cur.fetchone()["id"]

    cur.execute(
        """
        INSERT INTO vlettau.suretax_item_log_api (
            suretax_item_log_id,
            invoice_number,
            item_id,
            line_number,
            customer_number,
            service_description,
            service_group_description,
            revenue,
            fee,
            tax,
            tax_on_tax,
            transaction_id,
            transaction_type_cd,
            units,
            geocode,
            city_nm,
            county_nm,
            state_cd,
            zip_code,
            plus4,
            product_group,
            product_item
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            item_id,
            item.get("InvoiceNumber"),
            item.get("ItemID"),
            item.get("LineNumber"),
            item.get("CustomerNumber"),
            item.get("ServiceDescription"),
            item.get("ServiceGroupDescription"),
            item.get("Revenue"),
            item.get("Fee"),
            item.get("Tax"),
            item.get("TaxonTax"),
            transaction_id,
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

    return item_id


def insert_tax_log(cur, tax: Dict, item_log_id: int) -> int:
    cur.execute(
        "SELECT nextval('vlettau.suretax_tax_log_api_suretax_tax_log_id_seq') AS id"
    )
    tax_id = cur.fetchone()["id"]

    cur.execute(
        """
        INSERT INTO vlettau.suretax_tax_log_api (
            suretax_tax_log_id,
            suretax_item_log_id,
            tax_id,
            detailed_tax_desc,
            fee_rate,
            percent_taxable,
            tax_amt,
            tax_authority_nm,
            tax_authority_type,
            tax_cat,
            tax_rate,
            tax_type,
            tax_type_desc,
            tax_on_tax_amt,
            tier
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            tax_id,
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

    return tax_id


def insert_tax_calc_log(cur, calc: Dict, tax_log_id: int):
    cur.execute(
        """
        INSERT INTO vlettau.suretax_tax_calc_log_api (
            suretax_tax_log_id,
            log_id,
            max_tax,
            max_tax_base,
            max_tax_base_non_taxable_revenue,
            max_tax_non_taxable_amt,
            max_tax_non_taxable_revenue,
            min_tax_base,
            min_tax_base_non_taxable_revenue,
            round,
            tax,
            tax_auth_id,
            tax_base,
            tax_cat,
            tax_rate,
            tax_source,
            tax_type,
            tier,
            unit_base
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def store_response(cur, response: Dict, data_month=None, data_year=None):
    transaction_id = response.get("TransId")
    print(f"Storing response for transaction ID: {transaction_id}")

    # Inject fallback values if response is missing them
    insert_transaction_log(cur, response, data_month=data_month, data_year=data_year)

    item_id_map = {}
    tax_id_map = {}

    for group in response.get("GroupList", []):
        invoice = group.get("InvoiceNumber", "")
        customer = group.get("CustomerNumber", "")
        line = group.get("LineNumber", "")
        for item in group.get("TaxList", []):
            item_log_id = insert_item_log(
                cur,
                {
                    **item,
                    "InvoiceNumber": invoice,
                    "CustomerNumber": customer,
                    "LineNumber": line,
                },
                transaction_id,
            )
            item_id_map[item.get("ItemID")] = item_log_id

            for tax in item.get("TaxBreakdown", []):
                tax_log_id = insert_tax_log(cur, tax, item_log_id)
                for calc in tax.get("CalcLog", []):
                    insert_tax_calc_log(cur, calc, tax_log_id)

    for item in response.get("ItemMessages", []):
        pass  # optionally log or store elsewhere

    return transaction_id
