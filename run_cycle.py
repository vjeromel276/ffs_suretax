# run_cycle.py
import argparse
import datetime
from BillingCycleRunner import BillingCycleRunner

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a SureTax Billing Cycle")
    parser.add_argument("--cycle_cd", required=True, help="Billing cycle code (e.g., MNTHLY, MANUAL)")
    parser.add_argument("--company_cd", required=True, help="Company code (e.g., EVB, EVR)")
    parser.add_argument("--bill_date", required=True, type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date())
    parser.add_argument("--dev", action="store_true")
    parser.add_argument("--test_billing", action="store_true")
    parser.add_argument("--rerun_taxes", action="store_true")
    parser.add_argument("--no_taxes", action="store_true")
    parser.add_argument("--no_usage", action="store_true")
    parser.add_argument("--account_ids", nargs="*", type=int, help="List of account IDs (required for MANUAL cycles)")

    args = parser.parse_args()

    # Validate that manual cycles require account_ids
    if args.cycle_cd.upper() == "MANUAL" and not args.account_ids:
        parser.error("Manual cycles require at least one --account_ids value.")

    runner = BillingCycleRunner(
        cycle_cd=args.cycle_cd,
        company_cd=args.company_cd,
        bill_date=args.bill_date,
        dev=args.dev,
        test_billing=args.test_billing,
        rerun_taxes=args.rerun_taxes,
        no_taxes=args.no_taxes,
        no_usage=args.no_usage,
    )

    print("🧾 Starting billing run:")
    print(f"  ▸ Cycle:         {args.cycle_cd}")
    print(f"  ▸ Company:       {args.company_cd}")
    print(f"  ▸ Bill Date:     {args.bill_date}")
    print(f"  ▸ Dev Mode:      {args.dev}")
    print(f"  ▸ Test Billing:  {args.test_billing}")
    print(f"  ▸ Rerun Taxes:   {args.rerun_taxes}")
    print(f"  ▸ No Taxes:      {args.no_taxes}")
    print(f"  ▸ No Usage:      {args.no_usage}")
    if args.account_ids:
        print(f"  ▸ Account IDs:   {args.account_ids}")

    # Start the cycle
    # runner.begin_cycle(account_ids=args.account_ids)
    # runner.run()
    # runner.close()
