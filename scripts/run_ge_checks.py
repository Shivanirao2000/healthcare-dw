import great_expectations as gx
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

logger.info("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

logger.info("Loading RAW_CLAIMS sample...")
df = pd.read_sql("SELECT * FROM HEALTHCARE_DW.RAW.RAW_CLAIMS LIMIT 100000", conn)
conn.close()
logger.info(f"Loaded {len(df)} rows. Running expectations...")

context = gx.get_context(mode="ephemeral")

data_source = context.data_sources.add_pandas("claims_source")
data_asset = data_source.add_dataframe_asset("raw_claims")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

suite = context.suites.add(gx.ExpectationSuite(name="raw_claims_suite"))

expectations = [
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=1000, max_value=10_000_000),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="rndrng_npi"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="hcpcs_cd"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="place_of_srvc"),
    gx.expectations.ExpectColumnValuesToBeInSet(column="place_of_srvc", value_set=["F", "O"]),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="tot_srvcs"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="avg_mdcr_pymt_amt", min_value=0, max_value=500000),
]

print("\n" + "="*60)
print("GREAT EXPECTATIONS — DATA QUALITY REPORT")
print("="*60)
passed = failed = 0
for exp in expectations:
    result = batch.validate(exp)
    status = "✅ PASS" if result.success else "❌ FAIL"
    print(f"{status} | {exp.__class__.__name__}")
    if result.success: passed += 1
    else: failed += 1

print("="*60)
print(f"TOTAL: {passed} passed, {failed} failed")
print("="*60)
if failed > 0:
    exit(1)
