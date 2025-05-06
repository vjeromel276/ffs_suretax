import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
import datetime

from tax_data_fetchers import (
    get_one_time_items,
    get_service_items,
    get_sab_items,
    get_usage_items,
)
from suretax_middleware import submit_tax_request
from store_suretax_response import store_response
from db_utils import get_db_conn

# Debugging line to check connection parameters


class BillingCycleRunner:
    def __init__(
        self,
        cycle_cd: str,
        company_cd: str,
        bill_date: Optional[datetime.date] = None,
        dev: bool = False,
        test_billing: bool = False,
        rerun_taxes: bool = False,
        no_taxes: bool = False,
        no_usage: bool = False,
    ):
        """
        Initialize the BillingCycleRunner.

        Args:
            cycle_log_id: ID of the billing cycle to process.
            company_cd: Company code (e.g., 'EVB', 'EVR').
            bill_date: Billing date; defaults to today if not provided.
            dev: Whether to run in dev mode.
            test_billing: Whether this is a test bill run.
            rerun_taxes: If True, resend items to SureTax even if transaction ID is already set.
            no_taxes: If True, skip SureTax rating entirely.
            no_usage: If True, skip usage charges.
        """
        self.company_cd = company_cd
        self.cycle_cd = cycle_cd
        self.cycle_log_id = None
        self.bill_date = bill_date or datetime.date.today()
        self.dev = dev
        self.test_billing = test_billing
        self.rerun_taxes = rerun_taxes
        self.no_taxes = no_taxes
        self.no_usage = no_usage

        self.conn = get_db_conn()
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        self.conn.autocommit = False

    def run(self):
        """
        Main orchestration entry point for the billing cycle.
        """
        print("Starting billing cycle...")
        # Begin the billing cycle
        self.begin_cycle()
        print(f"Cycle Log ID: {self.cycle_log_id}")

        # Build the billing cycle
        # Note: The order of these functions is important for the billing cycle
        # to be processed correctly.
        # Adjustments are processed first, followed by payments and charges.

        # Adjustments
        self.build_account_adjustments()
        self.build_service_adjustments()
        if not self.no_taxes:
            self.tax_service_adjustments()
        # Payments
        self.build_payments()
        # Charges
        self.build_service_charges()
        if not self.no_taxes:
            self.tax_service_charges()
        # SAB Charges
        self.build_sab_charges()
        if not self.no_taxes:
            self.tax_sab_charges()
        # One-time Charges
        self.build_one_time_charges()
        if not self.no_taxes:
            self.tax_one_time_charges()
        # Usage Charges
        if not self.no_usage:
            self.build_usage_charges()
            if not self.no_taxes:
                self.tax_usage_charges()
        # Age Accounts
        # Note: Aging accounts is done twice, once for late and once for on-time.
        self.age_accounts(late=False)
        self.age_accounts(late=True)
        # Apply Finance Charges
        # Note: This function is called after all charges and adjustments have been processed.
        # It applies finance charges to accounts that are overdue.
        self.apply_finance_charges()
        # Mark Cycle Complete
        # Note: This function marks the billing cycle as complete in the database.
        # It should be called last to ensure all operations are completed.
        self.mark_cycle_complete()

    # def begin_cycle(self):
    #     """
    #     Begin the billing cycle by calling the cycle_begin function in the database.
    #     """
    #     self.cur.execute(
    #         '''
    #         SELECT biller.cycle_begin(%s, %s, %s, %s, 0)
    #         ''',
    #         (self.cycle_cd, self.bill_date, self.company_cd, self.test_billing)
    #     )
    #     self.cycle_log_id = self.cur.fetchone()['cycle_begin']

    def begin_cycle(self, account_ids: Optional[list[int]] = None):
        """
        Begin the billing cycle by calling the cycle_begin function in the database.

        Args:
            account_ids: Optional list of account IDs. Required for MANUAL cycles.
        """
        if self.cycle_cd.upper() == "MANUAL":
            if not account_ids or not isinstance(account_ids, list):
                raise ValueError(
                    "Manual cycles require a non-empty list of account IDs."
                )

            print(f"Starting MANUAL cycle for {len(account_ids)} accounts...")
            self.cur.execute(
                """
                SELECT biller.cycle_begin(%s, %s, %s, %s, %s, %s)
                """,
                (
                    self.cycle_cd,
                    self.bill_date,
                    self.company_cd,
                    self.test_billing,
                    account_ids,
                    0,
                ),
            )
        else:
            print("Starting automatic cycle...")
            self.cur.execute(
                """
                SELECT biller.cycle_begin(%s, %s, %s, %s, 0)
                """,
                (self.cycle_cd, self.bill_date, self.company_cd, self.test_billing),
            )

        self.cycle_log_id = self.cur.fetchone()["cycle_begin"]
        print(f"✅ Cycle started. Cycle Log ID: {self.cycle_log_id}")

    def build_account_adjustments(self):
        """
        Executes the database function to calculate account adjustments
        for the given billing cycle (e.g., credits, debits, prorations).
        """
        print("Building account adjustments...")
        self.cur.execute(
            """
            SELECT * FROM biller.cycle_get_account_adjustments(%s)
            """,
            (self.cycle_log_id,),
        )

    def build_service_adjustments(self):
        """
        Executes the database function to calculate service adjustments
        for the given billing cycle (e.g., partial credits, service-specific discounts).
        """
        print("Building service adjustments...")
        self.cur.execute(
            """
            SELECT * FROM biller.cycle_get_service_adjustments(%s)
            """,
            (self.cycle_log_id,),
        )

    def tax_service_adjustments(self):
        """
        Fetches service adjustment items, submits them to SureTax for taxation,
        and stores the SureTax response in the database.
        """
        print("Submitting service adjustments to SureTax...")
        items = get_service_items(self.cycle_log_id, rerun=self.rerun_taxes)

        if not items:
            print("✅ No service adjustments to tax.")
            return

        context = {
            "client_number": "164753897",
            "validation_key": "5c7680a5-f676-470a-a776-e388ed8b494f",
            "environment": "CERT" if self.dev else "PRODUCTION",
            "bill_date": self.bill_date,
            "client_tracking": f"Cycle {self.cycle_log_id} - ServiceAdjustments",
            "return_file_code": "Q" if self.test_billing else "0",
            "pg_cursor": self.cur,
            "pg_conn": self.conn,
        }

        response = submit_tax_request(kind="Service", items=items, context=context)

        print(f"🎯 SureTax Response Code: {response.get('ResponseCode')}")
        print(f"💸 Total Tax: {response.get('TotalTax')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")
        print(f"🔁 Transaction ID: {response.get('MasterTransID')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")
        print(f"💸 Total Tax: {response.get('TotalTax')}")

    def build_payments(self):
        """
        Executes the database function to calculate and apply payments
        for the given billing cycle. These payments reduce balances on invoices.
        """
        print("Building payments...")
        self.cur.execute(
            """
            SELECT * FROM biller.cycle_get_payments(%s)
            """,
            (self.cycle_log_id,),
        )

    def build_service_charges(self):
        """
        Executes the database function to build monthly recurring service charges
        (MRCs) for the given billing cycle, including refunds and non-recurring charges (NRCs).
        """
        print("Building service charges...")
        self.cur.execute(
            """
            SELECT biller.cycle_build_service_charges_v2(%s, TRUE)
            """,
            (self.cycle_log_id,),
        )

    def tax_service_charges(self):
        """
        Fetches monthly service charge items, submits them to SureTax for taxation,
        and stores the SureTax response in the database.
        This is typically the largest volume of taxed items in a billing cycle.
        """
        print("Submitting service charges to SureTax...")
        items = get_service_items(self.cycle_log_id, rerun=self.rerun_taxes)

        if not items:
            print("✅ No service charges to tax.")
            return

        context = {
            "client_number": "164753897",
            "validation_key": "5c7680a5-f676-470a-a776-e388ed8b494f",
            "environment": "CERT" if self.dev else "PRODUCTION",
            "bill_date": self.bill_date,
            "client_tracking": f"Cycle {self.cycle_log_id} - ServiceCharges",
            "return_file_code": "Q" if self.test_billing else "0",
            "pg_cursor": self.cur,
            "pg_conn": self.conn,
        }

        response = submit_tax_request(
            kind="ServiceCharges", items=items, context=context
        )

        print(f"🎯 SureTax Response Code: {response.get('ResponseCode')}")
        print(f"💸 Total Tax: {response.get('TotalTax')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")
        print(f"🔁 Transaction ID: {response.get('MasterTransID')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")

    def build_sab_charges(self):
        """
        Executes the database function to build Special Access Billing (SAB)
        service charges for the given billing cycle.
        SAB charges are handled separately due to different rating and tax rules.
        """
        print("Building SAB service charges...")
        self.cur.execute(
            """
            SELECT biller.cycle_build_sab_service_charges(%s, TRUE)
            """,
            (self.cycle_log_id,),
        )

    def tax_sab_charges(self):
        """
        Fetches SAB charge items, submits them to SureTax for taxation,
        and stores the SureTax response in the database.
        SAB charges typically involve special access circuits with unique rates and usage metrics.
        """
        print("Submitting SAB service charges to SureTax...")
        items = get_sab_items(self.cycle_log_id, rerun=self.rerun_taxes)

        if not items:
            print("✅ No SAB service charges to tax.")
            return

        context = {
            "client_number": "164753897",
            "validation_key": "5c7680a5-f676-470a-a776-e388ed8b494f",
            "environment": "CERT" if self.dev else "PRODUCTION",
            "bill_date": self.bill_date,
            "client_tracking": f"Cycle {self.cycle_log_id} - SABCharges",
            "return_file_code": "Q" if self.test_billing else "0",
            "pg_cursor": self.cur,
            "pg_conn": self.conn,
        }

        response = submit_tax_request(kind="SAB", items=items, context=context)

        print(f"🎯 SureTax Response Code: {response.get('ResponseCode')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")
        print(f"🔁 Transaction ID: {response.get('MasterTransID')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")
        print(f"💸 Total Tax: {response.get('TotalTax')}")

    def build_one_time_charges(self):
        """
        Executes the database function to insert one-time workorder charges
        into the cycle. These are typically installation, setup, or field tech fees.
        """
        print("Building one-time charges...")
        self.cur.execute(
            """
            SELECT biller.cycle_calculate_one_time_charges(%s)
            """,
            (self.cycle_log_id,),
        )

    def tax_one_time_charges(self):
        """
        Fetches one-time charge items (e.g. work orders), submits them to SureTax for taxation,
        and stores the SureTax response in the database.
        """
        print("Submitting one-time charges to SureTax...")
        items = get_one_time_items(self.cycle_log_id, rerun=self.rerun_taxes)

        if not items:
            print("✅ No one-time charges to tax.")
            return

        context = {
            "client_number": "164753897",
            "validation_key": "5c7680a5-f676-470a-a776-e388ed8b494f",
            "environment": "CERT" if self.dev else "PRODUCTION",
            "bill_date": self.bill_date,
            "client_tracking": f"Cycle {self.cycle_log_id} - OneTimeCharges",
            "return_file_code": "Q" if self.test_billing else "0",
            "pg_cursor": self.cur,
            "pg_conn": self.conn,
        }

        response = submit_tax_request(kind="OneTime", items=items, context=context)

        print(f"🎯 SureTax Response Code: {response.get('ResponseCode')}")
        print(f"💸 Total Tax: {response.get('TotalTax')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")

    def build_usage_charges(self):
        """
        Builds SPLA-type usage charges for the billing cycle.
        These are not calculated in a single SQL function — they are pre-populated
        via usage billing scripts prior to taxation.
        """
        print("Building usage charges (if any)... (delegated to external process)")
        # If this were to build from SQL, we would do it here
        # For now, we assume external process pre-populates usage charges
        pass

    def tax_usage_charges(self):
        """
        Fetches SPLA-type usage items, submits them to SureTax for taxation,
        and stores the SureTax response in the database.
        """
        print("Submitting usage charges to SureTax...")
        items = get_usage_items(self.cycle_log_id, rerun=self.rerun_taxes)

        if not items:
            print("✅ No usage charges to tax.")
            return
        context = {
            "client_number": "164753897",
            "validation_key": "5c7680a5-f676-470a-a776-e388ed8b494f",
            "environment": "CERT" if self.dev else "PRODUCTION",
            "bill_date": self.bill_date,
            "client_tracking": f"Cycle {self.cycle_log_id} - UsageCharges",
            "return_file_code": "Q" if self.test_billing else "0",
            "pg_cursor": self.cur,
            "pg_conn": self.conn,
        }
        response = submit_tax_request(kind="Usage", items=items, context=context)

        print(f"🎯 SureTax Response Code: {response.get('ResponseCode')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")
        print(f"🔁 Transaction ID: {response.get('MasterTransID')}")
        print(f"📦 Items Returned: {len(response.get('GroupList', []))}")
        print(f"💸 Total Tax: {response.get('TotalTax')}")

    def mark_cycle_complete(self):
        """
        Marks the billing cycle as complete by updating the cycle log and,
        if `--no_taxes` is used or taxes have been imported, triggers invoice posting.
        """
        print("Marking billing cycle complete...")

        self.cur.execute(
            """
            UPDATE biller.cycle_log
            SET run_cycle_complete = TRUE
            WHERE cycle_log_id = %s
            """,
            (self.cycle_log_id,),
        )

        if self.no_taxes or self.all_tax_data_imported():
            print("Building pre-post register...")
            self.cur.execute(
                """
                SELECT * FROM biller.cycle_build_prepost_register(%s)
                """,
                (self.cycle_log_id,),
            )

            print("Posting invoices...")
            self.cur.execute(
                """
                SELECT * FROM biller.cycle_post_invoices(%s, %s)
                """,
                (self.cycle_log_id, 0),
            )
        else:
            print("🚧 Skipping invoice posting — tax data is not fully imported yet.")

    def age_accounts(self, late: bool):
        """
        Executes the database function to age accounts for the billing cycle.
        If `late` is True, performs late aging for overdue balances.
        """
        mode = "late aging" if late else "normal aging"
        print(f"Aging accounts ({mode})...")

        self.cur.execute(
            """
            SELECT biller.age_accounts(%s, %s, %s, %s)
            """,
            (0, self.bill_date, late, self.company_cd),
        )

    def apply_finance_charges(self):
        """
        Executes the database function to calculate and apply finance charges
        (late payment fees) for the billing cycle. These are based on aged balances
        and account configuration.
        """
        print("Applying finance charges...")
        self.cur.execute(
            """
            SELECT * FROM biller.cycle_calculate_finance_charges(%s)
            """,
            (self.cycle_log_id,),
        )

    def all_tax_data_imported(self) -> bool:
        self.cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM biller.cycle_invoice_status
                WHERE cycle_log_id = %s AND tax_data_imported = TRUE
            )
            """,
            (self.cycle_log_id,),
        )
        return self.cur.fetchone()["exists"]

    def close(self):
        self.cur.close()
        self.conn.commit()
        self.conn.close()
