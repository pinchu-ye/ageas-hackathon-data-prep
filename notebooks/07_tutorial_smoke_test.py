# Databricks notebook source
# MAGIC %md
# MAGIC # Ageas hackathon - tutorial smoke test
# MAGIC
# MAGIC Runs every query the morning tutorial asks attendees to run, against whichever
# MAGIC catalog and schema you point it at, and prints one pass or fail line each.
# MAGIC
# MAGIC Run this after `00_prep_session` has finished. It proves the tutorial works in *this*
# MAGIC workspace rather than in ours, which is not the same thing: metric views need
# MAGIC Databricks Runtime 16.4 or above, and the document parsing needs 17.3 or above and a
# MAGIC pro or serverless warehouse.
# MAGIC
# MAGIC It writes nothing. Every statement is a read.

# COMMAND ----------

# MAGIC %md ## 1. Settings

# COMMAND ----------

dbutils.widgets.text("catalog", "sb_training", "1. Catalog")  # noqa: F821
dbutils.widgets.text("schema", "hackathon", "2. Schema")  # noqa: F821
dbutils.widgets.text("volume", "landing", "3. Volume")  # noqa: F821

CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
VOLUME = dbutils.widgets.get("volume")  # noqa: F821
FQ = f"{CATALOG}.{SCHEMA}"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"testing the tutorial against {FQ}, volume {VOLUME}")

# COMMAND ----------

# MAGIC %md ## 2. The checks
# MAGIC
# MAGIC Each entry is a section of the tutorial and the query it asks for. `CATALOG.SCHEMA`
# MAGIC is substituted the same way an attendee would substitute it.

# COMMAND ----------

results = []


def check(section, label, sql, expect_rows=1, optional=False):
    """Run one tutorial query and record whether an attendee would get an answer.

    `optional` marks a query that needs a capability the sandbox may not have, so a failure
    is a WARN rather than a FAIL. Returning zero rows counts as a failure: a query that
    parses but answers nothing still ruins the lab it belongs to.
    """
    sql = sql.replace("CATALOG.SCHEMA", FQ).replace("VOLUME_ROOT", VOLUME_ROOT)
    try:
        n = spark.sql(sql).count()  # noqa: F821
    except Exception as exc:
        results.append(dict(section=section, check=label,
                            status="WARN" if optional else "FAIL",
                            detail=str(exc).split("\n")[0][:220]))
        return
    if n < expect_rows:
        results.append(dict(section=section, check=label, status="FAIL",
                            detail=f"returned {n} rows, expected at least {expect_rows}"))
    else:
        results.append(dict(section=section, check=label, status="PASS",
                            detail=f"{n} rows"))


# --- section 1, the query everybody runs first
check("1 Before you start", "policy count",
      "SELECT COUNT(*) AS policies FROM CATALOG.SCHEMA.fact_policy")

# --- section 2, the tables the tutorial names
for table in ["dim_customer", "dim_product", "dim_broker", "fact_policy", "fact_claim",
              "fact_quote", "fact_complaint", "policy_schedule_expected"]:
    check("2 The data", f"{table} readable",
          f"SELECT * FROM CATALOG.SCHEMA.{table} LIMIT 5", expect_rows=5)

check("2 The data", "Not Yet Incepted policies exist",
      "SELECT 1 FROM CATALOG.SCHEMA.fact_policy "
      "WHERE policy_status = 'Not Yet Incepted' LIMIT 1")
check("2 The data", "repudiated and withdrawn claims cost zero",
      "SELECT claim_status FROM CATALOG.SCHEMA.fact_claim "
      "WHERE claim_status IN ('Repudiated','Withdrawn') "
      "GROUP BY claim_status HAVING MAX(incurred_amount_gbp) = 0", expect_rows=2)

# --- section 3, Lab 1
check("3 Lab 1", "the book by line of business and heritage", """
SELECT p.line_of_business, p.heritage_book, COUNT(*) AS policies,
       ROUND(SUM(f.gross_written_premium_gbp), 0) AS gwp_gbp,
       ROUND(AVG(f.gross_written_premium_gbp), 2) AS avg_premium_gbp
FROM CATALOG.SCHEMA.fact_policy f
JOIN CATALOG.SCHEMA.dim_product p USING (product_code)
GROUP BY ALL ORDER BY gwp_gbp DESC""", expect_rows=4)

check("3 Lab 1", "claims by incident date", """
SELECT DATE_TRUNC('MONTH', incident_date) AS month, COUNT(*) AS claims
FROM CATALOG.SCHEMA.fact_claim WHERE incident_date >= '2026-01-01'
GROUP BY ALL ORDER BY month""", expect_rows=6)

check("3 Lab 1", "claims by notified date", """
SELECT DATE_TRUNC('MONTH', notified_date) AS month, COUNT(*) AS claims
FROM CATALOG.SCHEMA.fact_claim WHERE notified_date >= '2026-01-01'
GROUP BY ALL ORDER BY month""", expect_rows=6)

check("3 Lab 1", "lineage target mv_portfolio_performance exists",
      "DESCRIBE CATALOG.SCHEMA.mv_portfolio_performance")

# --- section 5, Lab 3, metric views
check("5 Lab 3", "metric view by line of business and channel", """
SELECT `Line of business`, `Distribution channel`,
       MEASURE(`Policy count`) AS policies,
       MEASURE(`Gross written premium`) AS gwp,
       MEASURE(`Loss ratio`) AS loss_ratio
FROM CATALOG.SCHEMA.mv_portfolio_performance
GROUP BY ALL ORDER BY gwp DESC""", expect_rows=4)

check("5 Lab 3", "regroup by broker panel tier", """
SELECT `Broker panel tier`, MEASURE(`Policy count`) AS policies,
       MEASURE(`Loss ratio`) AS loss_ratio,
       MEASURE(`Claim frequency`) AS claims_per_policy
FROM CATALOG.SCHEMA.mv_portfolio_performance
WHERE `Distribution channel` = 'Broker'
GROUP BY ALL ORDER BY loss_ratio DESC""", expect_rows=3)

check("5 Lab 3", "mv_quote_conversion", """
SELECT `Price position band`, MEASURE(`Quote count`) AS quotes,
       MEASURE(`Conversion rate`) AS conversion
FROM CATALOG.SCHEMA.mv_quote_conversion GROUP BY ALL""", expect_rows=3)

check("5 Lab 3", "mv_complaint_experience", """
SELECT `Root cause`, MEASURE(`Complaint count`) AS complaints,
       MEASURE(`Uphold rate`) AS uphold_rate
FROM CATALOG.SCHEMA.mv_complaint_experience GROUP BY ALL""", expect_rows=3)

check("5 Lab 3", "the commission columns the added measure needs",
      "SELECT ROUND(SUM(commission_gbp) / SUM(net_premium_gbp), 4) AS commission_ratio "
      "FROM CATALOG.SCHEMA.fact_policy")

# --- section 6, Lab 4, the dashboard dataset
check("6 Lab 4", "dashboard dataset query", """
SELECT `Policy start month`, `Line of business`, `Distribution channel`,
       MEASURE(`Gross written premium`) AS gwp,
       MEASURE(`Policy count`) AS policies,
       MEASURE(`Loss ratio`) AS loss_ratio
FROM CATALOG.SCHEMA.mv_portfolio_performance
WHERE `Policy start month` >= '2024-01-01'
GROUP BY ALL""", expect_rows=20)

# --- section 7, Lab 5, the Genie Agent
check("7 Lab 5", "the sample query given to the agent", """
SELECT `Line of business`,
       MEASURE(`Gross written premium`) AS gwp,
       MEASURE(`Incurred claims`) AS incurred,
       MEASURE(`Loss ratio`) AS loss_ratio
FROM CATALOG.SCHEMA.mv_portfolio_performance
WHERE `Policy start month` >= '2026-01-01'
GROUP BY ALL ORDER BY loss_ratio DESC""", expect_rows=4)

# Every column the agent instructions name has to exist, or the instruction is a lie.
check("7 Lab 5", "columns named in the agent instructions", """
SELECT c.incurred_amount_gbp, c.claim_status, c.fraud_referral_flag, c.fraud_score,
       c.incident_date, c.notified_date, c.closed_date,
       p.policy_id, p.product_code, p.broker_id, p.net_premium_gbp
FROM CATALOG.SCHEMA.fact_claim c
JOIN CATALOG.SCHEMA.fact_policy p USING (policy_id) LIMIT 5""", expect_rows=5)

for q, sql in [
    ("Q1 claims notified last month",
     "SELECT COUNT(*) FROM CATALOG.SCHEMA.fact_claim "
     "WHERE notified_date >= '2026-10-01' AND notified_date < '2026-11-01'"),
    ("Q3 worst panel tier by loss ratio",
     "SELECT `Broker panel tier`, MEASURE(`Loss ratio`) lr "
     "FROM CATALOG.SCHEMA.mv_portfolio_performance "
     "WHERE `Distribution channel` = 'Broker' GROUP BY ALL ORDER BY lr DESC LIMIT 1"),
    ("Q4 open claims over ten thousand",
     "SELECT COUNT(*) FROM CATALOG.SCHEMA.fact_claim "
     "WHERE claim_status = 'Open' AND incurred_amount_gbp > 10000"),
]:
    check("7 Lab 5", f"test question {q} is answerable", sql)

# --- section 9, the documents
check("9 Documents", "document_index lists the documents",
      "SELECT * FROM CATALOG.SCHEMA.document_index", expect_rows=22)
for table, rows in [("doc_fnol_expected", 8), ("doc_estimate_expected", 3),
                    ("doc_submission_expected", 5), ("doc_statement_expected", 2),
                    ("doc_uw_rule_expected", 28), ("doc_panel_report_expected", 3)]:
    check("9 Documents", f"{table} ground truth", f"SELECT * FROM CATALOG.SCHEMA.{table}",
          expect_rows=rows)

check("9 Documents", "the PDFs are readable from the volume",
      "SELECT path FROM READ_FILES('VOLUME_ROOT/documents/*/*.pdf', "
      "format => 'binaryFile')", expect_rows=22)

# ai_parse_document needs runtime 17.3 or above, so this one is allowed to warn.
check("9 Documents", "ai_parse_document runs (needs runtime 17.3+)",
      "SELECT ai_parse_document(content, map('version', '2.0')) AS doc "
      "FROM READ_FILES('VOLUME_ROOT/documents/fnol/*.pdf', format => 'binaryFile') LIMIT 1",
      optional=True)

check("9 Documents", "call transcripts are present for ai_mask",
      "SELECT call_transcript FROM CATALOG.SCHEMA.fact_complaint "
      "WHERE call_transcript IS NOT NULL LIMIT 5", expect_rows=5)

# COMMAND ----------

# MAGIC %md ## 3. Result

# COMMAND ----------

import pandas as pd

summary = pd.DataFrame(results)
n_fail = int((summary.status == "FAIL").sum())
n_warn = int((summary.status == "WARN").sum())
n_pass = int((summary.status == "PASS").sum())

print(f"{n_pass} passed, {n_warn} warned, {n_fail} failed, out of {len(summary)}\n")
if n_fail:
    print("FAILURES, these break a lab on the day:")
    for r in summary[summary.status == "FAIL"].itertuples():
        print(f"  [{r.section}] {r.check}\n      {r.detail}")
if n_warn:
    print("\nWarnings, expected if the sandbox lacks the capability:")
    for r in summary[summary.status == "WARN"].itertuples():
        print(f"  [{r.section}] {r.check}\n      {r.detail}")

verdict = "TUTORIAL_OK" if n_fail == 0 else "TUTORIAL_BROKEN"
print(f"\nverdict: {verdict}")
display(summary)  # noqa: F821

# COMMAND ----------

# MAGIC %md ## 4. Machine-readable result
# MAGIC
# MAGIC So the run can be checked without reading the output by eye, the same way
# MAGIC `00_prep_session` reports its sign-off.

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({  # noqa: F821
    "verdict": verdict,
    "target": FQ,
    "passed": n_pass,
    "warned": n_warn,
    "failed": n_fail,
    "failures": summary[summary.status == "FAIL"][["section", "check", "detail"]]
                .to_dict("records"),
    "warnings": summary[summary.status == "WARN"][["section", "check", "detail"]]
                .to_dict("records"),
}, indent=1))
