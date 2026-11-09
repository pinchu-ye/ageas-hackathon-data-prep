# Databricks notebook source
# MAGIC %md
# MAGIC # Ageas hackathon - environment prep and sign-off
# MAGIC
# MAGIC **Run this on Monday 9 November, one week before the hackathon.** It is the only
# MAGIC notebook you need to open: it checks the environment, runs the two generator notebooks
# MAGIC for you, verifies what landed, and prints a sign-off table at the end.
# MAGIC
# MAGIC ## Before you start
# MAGIC
# MAGIC Put all three notebooks in the same workspace folder:
# MAGIC
# MAGIC ```
# MAGIC 00_prep_session                 <- you are here
# MAGIC 01_generate_hackathon_data
# MAGIC 02_generate_documents
# MAGIC ```
# MAGIC
# MAGIC Then attach to any cluster or serverless compute you can create Unity Catalog tables
# MAGIC from, set the widgets, and run all. Allow around 20 minutes.
# MAGIC
# MAGIC ## What to do with the result
# MAGIC
# MAGIC Section 5 prints a single table. **Screenshot it and send it back.** If anything shows
# MAGIC as BLOCKER we need to know on the Monday, not on the Friday, because two of the possible
# MAGIC blockers need a platform admin to fix and that takes days rather than minutes.
# MAGIC
# MAGIC Everything generated is synthetic. No real customer, broker, claim or complaint is
# MAGIC involved, and nothing reads from any production system.

# COMMAND ----------

# MAGIC %md ## 1. Settings

# COMMAND ----------

dbutils.widgets.text("catalog", "sb_training", "1. Catalog")
dbutils.widgets.text("schema", "hackathon", "2. Schema")
dbutils.widgets.text("volume", "landing", "3. Volume")
dbutils.widgets.dropdown("scale", "full", ["smoke", "full"], "4. Scale")
dbutils.widgets.dropdown("run_generators", "yes", ["yes", "no"], "5. Run the generators")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
SCALE = dbutils.widgets.get("scale")
RUN_GENERATORS = dbutils.widgets.get("run_generators") == "yes"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
GEN_PARAMS = {"catalog": CATALOG, "schema": SCHEMA, "volume": VOLUME, "scale": SCALE}

# Collected as we go, then printed as the sign-off table.
checks = []


def record(area, check, status, detail="", action=""):
    """status is PASS, WARN or BLOCKER."""
    checks.append(dict(area=area, check=check, status=status, detail=str(detail)[:400],
                       action=action))
    print(f"  [{status:7s}] {area} - {check}: {str(detail)[:150]}")


print(f"target: {CATALOG}.{SCHEMA}, volume {VOLUME}, scale {SCALE}")
print(f"run generators: {RUN_GENERATORS}")

# COMMAND ----------

# MAGIC %md ## 2. Pre-flight
# MAGIC
# MAGIC Nothing here changes any data apart from creating the target schema and volume, which we
# MAGIC need anyway. Every check is wrapped, so a failure is reported rather than stopping the
# MAGIC notebook.

# COMMAND ----------

# --- who and where
try:
    host = spark.conf.get("spark.databricks.workspaceUrl")
except Exception:
    host = "unknown"
try:
    me = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
except Exception as exc:
    me = f"unknown ({exc})"
record("Workspace", "Signed in as", "PASS", f"{me} on {host}")

# --- compute and runtime version.
# Three ways to find out, because none of them works everywhere. Cluster tags exist on
# classic compute only, and current_version() reports a DBR version on a cluster but a DBSQL
# channel version on a warehouse or on serverless.
dbr = None
for key in ("spark.databricks.clusterUsageTags.sparkVersion",
            "spark.databricks.clusterUsageTags.effectiveSparkVersion"):
    try:
        dbr = spark.conf.get(key)
        break
    except Exception:
        continue

dbsql = None
if not dbr:
    try:
        v = spark.sql("SELECT current_version() AS v").collect()[0]["v"]
        dbr = v["dbr_version"]
        dbsql = v["dbsql_version"]
    except Exception:
        pass


def dbr_number(text):
    """Pull the leading major.minor out of a runtime string like 17.3.x-scala2.13."""
    if not text:
        return None
    bits = text.split("-")[0].split(".")
    try:
        return float(f"{int(bits[0])}.{int(bits[1])}")
    except Exception:
        return None


version = dbr_number(dbr)
if version is not None and version >= 17.3:
    record("Compute", "Databricks Runtime", "PASS",
           f"{dbr}. Metric views and ai_parse_document are both supported.")
elif version is not None and version >= 16.4:
    record("Compute", "Databricks Runtime", "WARN",
           f"{dbr}. Metric views work, but ai_parse_document needs 17.3 or above.",
           "Fine for the hackathon. The document extraction becomes a demo only.")
elif version is not None:
    record("Compute", "Databricks Runtime", "BLOCKER",
           f"{dbr}. Metric views need 16.4 or above.",
           "Ask the platform team for a cluster on 17.3 or above.")
elif dbsql:
    # A SQL warehouse or serverless compute. These track the current channel, so the version
    # floors that matter for metric views are met. The metric view probe below is the real
    # proof either way.
    record("Compute", "Compute version", "PASS",
           f"serverless or SQL warehouse on channel {dbsql}. No fixed runtime to check, and "
           f"the metric view probe below confirms what matters.")
else:
    record("Compute", "Compute version", "WARN",
           "could not determine a version from cluster tags or current_version()",
           "Not a problem in itself. The metric view and AI function checks below tell us "
           "what this compute can actually do.")

# --- SQL warehouses. This is the check that decides whether Genie is possible at all.
try:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    rows = []
    for wh in warehouses:
        wtype = getattr(wh.warehouse_type, "value", str(wh.warehouse_type))
        serverless = bool(getattr(wh, "enable_serverless_compute", False))
        kind = "SERVERLESS" if serverless else wtype
        rows.append((wh.name, kind, str(getattr(wh.cluster_size, "value", wh.cluster_size)),
                     str(getattr(wh.state, "value", wh.state))))
    usable = [r for r in rows if r[1] in ("PRO", "SERVERLESS")]
    listing = "; ".join(f"{n} ({k}, {s})" for n, k, s, _ in rows) or "none visible"

    if usable:
        record("Genie", "Pro or serverless SQL warehouse", "PASS",
               f"{len(usable)} usable of {len(rows)}: {listing}")
    elif rows:
        record("Genie", "Pro or serverless SQL warehouse", "BLOCKER",
               f"only classic warehouses are visible: {listing}",
               "Genie Agents require a pro or serverless warehouse. Ask the platform owner "
               "to switch three warehouses to pro. Pro is not serverless: it still runs in "
               "the Ageas cloud account, so the serverless infosec position does not apply.")
    else:
        record("Genie", "Pro or serverless SQL warehouse", "WARN",
               "no warehouses visible to this user",
               "You may simply lack permission to list them. Worth confirming with the "
               "platform owner.")

    if len(usable) < 3 and usable:
        record("Genie", "One warehouse per team", "WARN",
               f"{len(usable)} usable warehouse(s), we would like three",
               "Three warehouses stop twenty-one people queueing behind each other.")
except Exception as exc:
    record("Genie", "Pro or serverless SQL warehouse", "WARN",
           f"could not list warehouses: {exc}",
           "Check under SQL Warehouses in the sidebar and tell us the Type column.")

# COMMAND ----------

# --- Unity Catalog: can we actually write where we intend to?
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    record("Unity Catalog", "Create schema", "PASS", f"{CATALOG}.{SCHEMA} is available")
except Exception as exc:
    record("Unity Catalog", "Create schema", "BLOCKER", exc,
           f"You need CREATE SCHEMA on {CATALOG}, or point the widget at a catalog where "
           f"you do.")

try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    record("Unity Catalog", "Create volume", "PASS", VOLUME_ROOT)
except Exception as exc:
    record("Unity Catalog", "Create volume", "BLOCKER", exc,
           "The documents and PDFs need a volume. Without it, sections 4 and 5 of notebook "
           "01 and all of notebook 02 will fail.")

# --- metric views, which the whole semantic layer session depends on.
# The probe deliberately builds over a real Unity Catalog table rather than an inline query,
# because that is the pattern the hackathon actually uses.
probe_tbl = f"{CATALOG}.{SCHEMA}._prep_probe_table"
probe_mv = f"{CATALOG}.{SCHEMA}._prep_probe_metric_view"
try:
    spark.sql(f"CREATE OR REPLACE TABLE {probe_tbl} AS SELECT 'a' AS grp, 1 AS amount")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {probe_mv}
        WITH METRICS LANGUAGE YAML AS $$
        version: 1.1
        source: {probe_tbl}
        fields:
          - name: Group
            expr: source.grp
        measures:
          - name: Total amount
            expr: SUM(source.amount)
        $$
    """)
    got = spark.sql(f"SELECT MEASURE(`Total amount`) AS m FROM {probe_mv}").collect()[0]["m"]
    if got == 1:
        record("Semantic layer", "Metric views", "PASS",
               "created a metric view over a Unity Catalog table and queried it with MEASURE")
    else:
        record("Semantic layer", "Metric views", "WARN",
               f"metric view returned {got}, expected 1",
               "Send us this result. The mechanism works but the answer is odd.")
except Exception as exc:
    msg = str(exc)
    version_problem = any(t in msg.lower() for t in
                          ("version", "not supported", "unsupported", "unrecognized",
                           "parse", "with metrics"))
    record("Semantic layer", "Metric views",
           "BLOCKER" if version_problem else "WARN", msg,
           "The metric view session is a third of the morning. The usual cause is a runtime "
           "below 16.4. Send us this error either way, because a failure here may also just "
           "mean this probe could not run."
           if version_problem else
           "Could not verify metric views. Send us this error rather than assuming they are "
           "broken.")
finally:
    for obj, kind in ((probe_mv, "VIEW"), (probe_tbl, "TABLE")):
        try:
            spark.sql(f"DROP {kind} IF EXISTS {obj}")
        except Exception:
            pass

# COMMAND ----------

# --- AI functions. Expected to fail on classic compute, which is useful to know now.
try:
    out = spark.sql(
        "SELECT ai_query('databricks-claude-sonnet-4-5', 'Reply with the word ready') AS r"
    ).collect()[0]["r"]
    record("AI functions", "ai_query", "PASS", f"responded: {str(out)[:60]}")
except Exception as exc:
    msg = str(exc)
    record("AI functions", "ai_query", "WARN", msg,
           "Expected if this is a classic warehouse, or if no model endpoint is available. "
           "The document extraction becomes a demonstration from the Databricks side. "
           "Check Serving, then AI Gateway, for the endpoint names you do have.")

try:
    spark.sql("SELECT ai_parse_document(CAST('x' AS BINARY)) AS r").collect()
    record("AI functions", "ai_parse_document", "PASS", "function is available")
except Exception as exc:
    msg = str(exc)
    if "UNRESOLVED_ROUTINE" in msg or "cannot be resolved" in msg:
        record("AI functions", "ai_parse_document", "WARN",
               "function not available on this compute",
               "Needs runtime 17.3 or above. Not required for the hackathon itself.")
    else:
        # Any other error means the function resolved and simply rejected our dummy input.
        record("AI functions", "ai_parse_document", "PASS",
               "function resolves. It rejected the dummy input, which is expected.")

# COMMAND ----------

# MAGIC %md ### Two things this notebook cannot check for you
# MAGIC
# MAGIC Both need a workspace or account admin, and both will stop participants building a
# MAGIC Genie Agent on the day even if everything above passes. Please confirm them with the
# MAGIC platform owner.
# MAGIC
# MAGIC 1. **Partner-powered AI features must be enabled at the account and workspace level.**
# MAGIC    Genie will not work without it. It is a documented requirement, not an optional
# MAGIC    setting.
# MAGIC 2. **Each participant needs the Databricks SQL entitlement**, CAN USE on their team
# MAGIC    warehouse, and SELECT on the hackathon schema. Three groups, one per team, is far
# MAGIC    easier than twenty-one individual grants.

# COMMAND ----------

record("Manual check", "Partner-powered AI features enabled", "WARN",
       "cannot be read from a notebook",
       "Confirm with the platform owner at both account and workspace level. Genie does not "
       "work without it.")
record("Manual check", "Participant entitlements and grants", "WARN",
       "cannot be read from a notebook",
       "Databricks SQL entitlement, CAN USE on the team warehouse, SELECT on the schema, "
       "plus a schema per team they can create objects in.")

# COMMAND ----------

# MAGIC %md ## 3. Generate the data
# MAGIC
# MAGIC Runs the two generator notebooks in order. Notebook 02 reads the tables notebook 01
# MAGIC creates, so the order matters. Set the "Run the generators" widget to `no` if you only
# MAGIC want the checks.

# COMMAND ----------

if RUN_GENERATORS:
    for label, path, timeout in [("tables, metric views and policy schedules",
                                  "./01_generate_hackathon_data", 3600),
                                 ("team documents and ground truth",
                                  "./02_generate_documents", 1800)]:
        print(f"running {path} ({label}) ...")
        try:
            params = dict(GEN_PARAMS)
            if path.endswith("02_generate_documents"):
                params.pop("scale", None)  # notebook 02 has no scale widget
            dbutils.notebook.run(path, timeout, params)
            record("Generation", path.lstrip("./"), "PASS", f"completed: {label}")
        except Exception as exc:
            record("Generation", path.lstrip("./"), "BLOCKER", exc,
                   "Send us this error. Do not carry on to the checks below until it is "
                   "resolved.")
else:
    record("Generation", "Generators", "WARN", "skipped by the widget setting",
           "Set 'Run the generators' to yes when you are ready to build the data.")

# COMMAND ----------

# MAGIC %md ## 4. Check what actually landed
# MAGIC
# MAGIC This is the part that matters. A generator that reports success but leaves a table
# MAGIC empty is worse than one that fails loudly.

# COMMAND ----------

EXPECTED_MIN_ROWS = {
    "dim_customer": 500, "dim_product": 12, "dim_broker": 30,
    "fact_policy": 1000, "fact_claim": 100, "fact_quote": 2000,
    "fact_complaint": 100, "policy_schedule_expected": 5,
    "document_index": 22, "doc_fnol_expected": 8,
    "doc_estimate_expected": 3, "doc_estimate_line_expected": 10,
    "doc_submission_expected": 5, "doc_submission_history_expected": 25,
    "doc_statement_expected": 2, "doc_statement_line_expected": 10,
    "doc_uw_rule_expected": 28, "doc_panel_report_expected": 3,
    "doc_panel_line_expected": 20,
}
METRIC_VIEWS = {
    "mv_portfolio_performance": "Gross written premium",
    "mv_quote_conversion": "Quote count",
    "mv_complaint_experience": "Complaint count",
}

# --- tables and row counts
missing, thin, counts = [], [], {}
for name, floor in EXPECTED_MIN_ROWS.items():
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{name}").count()
        counts[name] = n
        if n < floor:
            thin.append(f"{name}={n} (expected at least {floor})")
    except Exception:
        missing.append(name)

if missing:
    record("Data", "All tables present", "BLOCKER", f"missing: {', '.join(missing)}",
           "Re-run the generator that produces them and send us any error.")
elif thin:
    record("Data", "Row counts", "BLOCKER", "; ".join(thin),
           "A table exists but is short. Send us the generator output.")
else:
    record("Data", "All tables present with sensible row counts", "PASS",
           f"{len(counts)} tables, {sum(counts.values()):,} rows in total")

# --- metric views queryable through MEASURE
mv_bad = []
for mv, measure in METRIC_VIEWS.items():
    try:
        spark.sql(f"SELECT MEASURE(`{measure}`) AS m "
                  f"FROM {CATALOG}.{SCHEMA}.{mv}").collect()
    except Exception as exc:
        mv_bad.append(f"{mv}: {str(exc)[:120]}")
if mv_bad:
    record("Semantic layer", "Metric views queryable", "BLOCKER", "; ".join(mv_bad),
           "The afternoon build depends on these.")
else:
    record("Semantic layer", "Metric views queryable", "PASS",
           f"all {len(METRIC_VIEWS)} answered a MEASURE query")

# COMMAND ----------

# --- column comments, which are what a Genie Agent reads
try:
    gaps = spark.sql(f"""
        SELECT table_name, COUNT(*) AS missing
        FROM {CATALOG}.information_schema.columns
        WHERE table_schema = '{SCHEMA}'
          AND table_name NOT LIKE 'mv_%'
          AND (comment IS NULL OR comment = '')
        GROUP BY table_name
    """).collect()
    if gaps:
        detail = "; ".join(f"{r['table_name']}={r['missing']}" for r in gaps)
        record("Metadata", "Column comments", "WARN", f"columns with no comment: {detail}",
               "Genie reads these. Thin metadata is the main cause of a confidently wrong "
               "answer.")
    else:
        record("Metadata", "Column comments", "PASS",
               "every column on every table carries a comment")
except Exception as exc:
    record("Metadata", "Column comments", "WARN", exc)

# --- files in the volume
# Policy schedules follow the scale widget. The team documents do not: notebook 02 has no
# scale widget and always writes the same 22, because the exercises reference them by name.
SCHEDULES_EXPECTED = 60 if SCALE == "full" else 6
for label, pattern, expected in [
    ("policy schedule PDFs", f"{VOLUME_ROOT}/policy_schedules/*.pdf", SCHEDULES_EXPECTED),
    ("team documents", f"{VOLUME_ROOT}/documents/*/*.pdf", 22),
    ("JSON quote events", f"{VOLUME_ROOT}/raw/*.json", 1),
]:
    try:
        n = spark.read.format("binaryFile").load(pattern).count()
        if n == expected:
            record("Volume", label, "PASS", f"{n} file(s) readable")
        elif n < expected:
            record("Volume", label, "BLOCKER", f"found {n}, expected {expected}",
                   "Check the generator output for the section that writes these.")
        else:
            # More files than this run wrote means an earlier run is still on the volume
            # under different names. The globs in the tutorial would then read two
            # generations of documents against one generation of ground truth.
            record("Volume", label, "BLOCKER",
                   f"found {n}, expected exactly {expected}",
                   "Stale files from an earlier run. Delete the folder and re-run, or "
                   "use a clean schema.")
    except Exception as exc:
        record("Volume", label, "BLOCKER", exc,
               "Files are missing or the volume is not readable.")

# --- referential integrity, cheap and worth doing
try:
    bad = spark.sql(f"""
        SELECT
          (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.fact_claim c
             LEFT JOIN {CATALOG}.{SCHEMA}.fact_policy p USING (policy_id)
            WHERE p.policy_id IS NULL)                                   AS orphan_claims,
          (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.fact_complaint m
             LEFT JOIN {CATALOG}.{SCHEMA}.fact_policy p USING (policy_id)
            WHERE p.policy_id IS NULL)                                   AS orphan_complaints,
          (SELECT COUNT(*) - COUNT(DISTINCT policy_id)
             FROM {CATALOG}.{SCHEMA}.fact_policy)                        AS duplicate_policies
    """).collect()[0]
    total = bad["orphan_claims"] + bad["orphan_complaints"] + bad["duplicate_policies"]
    if total == 0:
        record("Data", "Referential integrity", "PASS",
               "no orphan claims or complaints, no duplicate policy keys")
    else:
        record("Data", "Referential integrity", "BLOCKER", dict(bad.asDict()),
               "Send us the generator output.")
except Exception as exc:
    record("Data", "Referential integrity", "WARN", exc)

# COMMAND ----------

# MAGIC %md ### 4b. Do the numbers look like an insurance book?
# MAGIC
# MAGIC Structural checks prove the data loaded. They do not prove it is believable. Twenty-one
# MAGIC insurance professionals will judge this in the first ten minutes, so the figures are
# MAGIC checked against the ranges a UK personal lines book would actually sit in.
# MAGIC
# MAGIC The last three checks are different. They confirm the **teaching signals the afternoon
# MAGIC briefs promise are actually present**. Each team brief says "there is a real signal in
# MAGIC this data". If an edit to the generator ever flattens one of those, this is where it
# MAGIC gets caught, rather than by a team that cannot find what they were told to look for.

# COMMAND ----------

def scalar(sql):
    """Run a query that returns a single value, as a plain float.

    SQL averages over decimal columns come back as `Decimal`, which neither Arrow nor
    `json.dumps` will accept. Coercing here rather than at each use site is what stops that
    turning into a failure three cells later.
    """
    value = spark.sql(sql).collect()[0][0]
    return None if value is None else float(value)


T = f"{CATALOG}.{SCHEMA}"
plaus_rows = []  # referenced by the exit payload, so it must always exist


def plausible(metric, value, low, high, note=""):
    value = None if value is None else float(value)
    ok = value is not None and low <= value <= high
    plaus_rows.append(dict(metric=metric,
                           value=round(value, 1) if value is not None else None,
                           expected=f"{low} to {high}", status="PASS" if ok else "WARN",
                           note=note))
    if not ok:
        record("Plausibility", metric, "WARN", f"{value} outside {low} to {high}", note)


def signal(name, value, minimum, note):
    value = None if value is None else float(value)
    ok = value is not None and value >= minimum
    plaus_rows.append(dict(metric=name,
                           value=round(value, 1) if value is not None else None,
                           expected=f"at least {minimum}", status="PASS" if ok else "WARN",
                           note=note))
    if not ok:
        record("Teaching signal", name, "WARN",
               f"{value}, expected at least {minimum}", note)


try:
    for lob, low, high in [("Motor", 55, 80), ("Home", 38, 68), ("Pet", 48, 78),
                           ("Travel", 22, 58)]:
        lr = scalar(f"""
            SELECT SUM(c.incurred_amount_gbp) / SUM(p.net_premium_gbp) * 100
            FROM {T}.fact_policy p
            JOIN {T}.dim_product d USING (product_code)
            LEFT JOIN {T}.fact_claim c ON c.policy_id = p.policy_id
            WHERE d.line_of_business = '{lob}'
        """)
        plausible(f"Loss ratio, {lob} (pct)", lr, low, high,
                  "Incurred claims over net premium.")

    for lob, low, high in [("Motor", 450, 850), ("Home", 200, 420), ("Pet", 230, 500),
                           ("Travel", 70, 200)]:
        ap = scalar(f"""
            SELECT AVG(p.gross_written_premium_gbp) FROM {T}.fact_policy p
            JOIN {T}.dim_product d USING (product_code)
            WHERE d.line_of_business = '{lob}'
        """)
        plausible(f"Average premium, {lob} (GBP)", ap, low, high, "Gross of IPT.")

    plausible("Quote conversion (pct)",
              scalar(f"SELECT AVG(CASE WHEN is_converted THEN 100.0 ELSE 0 END) "
                     f"FROM {T}.fact_quote"), 8, 22, "Across all channels.")
    plausible("Fraud referral rate (pct)",
              scalar(f"SELECT AVG(CASE WHEN fraud_referral_flag THEN 100.0 ELSE 0 END) "
                     f"FROM {T}.fact_claim"), 1, 7, "Claims above the referral threshold.")
    plausible("Complaint uphold rate (pct)",
              scalar(f"SELECT AVG(CASE WHEN is_upheld THEN 100.0 ELSE 0 END) "
                     f"FROM {T}.fact_complaint WHERE is_upheld IS NOT NULL"), 38, 68,
              "Of closed complaints.")
    plausible("Complaints breaching 8 weeks (pct)",
              scalar(f"SELECT AVG(CASE WHEN days_to_close > 56 THEN 100.0 ELSE 0 END) "
                     f"FROM {T}.fact_complaint WHERE days_to_close IS NOT NULL"), 2, 14,
              "The regulatory service standard is 56 days.")

    # --- the three signals the afternoon briefs promise
    spread = scalar(f"""
        WITH t AS (
          SELECT b.panel_tier,
                 SUM(c.incurred_amount_gbp) / SUM(p.net_premium_gbp) * 100 AS lr
          FROM {T}.fact_policy p
          JOIN {T}.dim_broker b USING (broker_id)
          LEFT JOIN {T}.fact_claim c ON c.policy_id = p.policy_id
          GROUP BY b.panel_tier
        )
        SELECT MAX(lr) - MIN(lr) FROM t
    """)
    signal("Brokerage signal: loss ratio spread across panel tiers (pct points)",
           spread, 12, "The Brokerage brief promises that premium volume and loss ratio do "
                       "not rank the same way.")

    late = scalar(f"""
        WITH t AS (
          SELECT CASE WHEN days_to_notify >= 15 THEN 'late' ELSE 'prompt' END AS b,
                 AVG(fraud_referral_flag::INT) * 100 AS pct
          FROM {T}.fact_claim WHERE incurred_amount_gbp > 0 GROUP BY 1
        )
        SELECT MAX(pct) - MIN(pct) FROM t
    """)
    signal("Claims signal: fraud referral gap, late versus prompt notification (pct points)",
           late, 8, "The Claims brief points at days_to_notify against cost and fraud_score.")

    vuln = scalar(f"""
        WITH t AS (
          SELECT cu.is_vulnerable_customer AS v, AVG(m.days_to_close) AS d
          FROM {T}.fact_complaint m
          JOIN {T}.fact_policy p USING (policy_id)
          JOIN {T}.dim_customer cu USING (customer_id)
          WHERE m.days_to_close IS NOT NULL
          GROUP BY cu.is_vulnerable_customer
        )
        SELECT MAX(d) - MIN(d) FROM t
    """)
    signal("Governance signal: extra days to close for vulnerable customers", vuln, 8,
           "The Conduct and Governance brief promises a finding in the slow tail.")

    from pyspark.sql.types import (DoubleType, StringType, StructField,  # noqa: E402
                                   StructType)

    n_bad = sum(1 for r in plaus_rows if r["status"] == "WARN")
    if n_bad == 0:
        record("Plausibility", "Business figures and teaching signals", "PASS",
               f"all {len(plaus_rows)} checks within range")

    plaus_schema = StructType([
        StructField("metric", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("expected", StringType(), True),
        StructField("status", StringType(), True),
        StructField("note", StringType(), True),
    ])
    plaus_spark_rows = [
        (r["metric"], None if r["value"] is None else float(r["value"]),
         r["expected"], r["status"], r["note"])
        for r in plaus_rows
    ]
    for r in plaus_rows:
        print(f"  [{r['status']:4s}] {r['metric']}: {r['value']} "
              f"(expected {r['expected']})")
    display(spark.createDataFrame(plaus_spark_rows, schema=plaus_schema))
except Exception as exc:  # noqa: BLE001
    record("Plausibility", "Business figures", "WARN", exc,
           "Could not compute the plausibility checks. The structural checks above still "
           "stand.")

# COMMAND ----------

# MAGIC %md ## 5. Sign-off
# MAGIC
# MAGIC **Screenshot the summary and the table below, and send them back.**
# MAGIC
# MAGIC The verdict is one of three things:
# MAGIC
# MAGIC - `READY` - nothing to do except the two manual checks, which need the platform owner
# MAGIC - `USABLE, WITH CAVEATS` - the day can go ahead, but read the warnings
# MAGIC - `NOT READY` - something needs fixing this week
# MAGIC
# MAGIC Two warnings always appear, because a notebook cannot read the partner-powered AI
# MAGIC setting or anyone's entitlements. Those are listed separately as things to confirm, and
# MAGIC they do not count against the verdict.

# COMMAND ----------

import pandas as pd

summary = pd.DataFrame(checks)
order = {"BLOCKER": 0, "WARN": 1, "PASS": 2}
summary = summary.sort_values(by="status", key=lambda s: s.map(order)).reset_index(drop=True)

n_block = int((summary.status == "BLOCKER").sum())
n_warn = int((summary.status == "WARN").sum())
n_pass = int((summary.status == "PASS").sum())

# Two warnings are always present by design, because a notebook cannot read the
# partner-powered AI setting or anyone's entitlements. A deliberate generator skip is not a
# problem either. Neither should decide the verdict, or it would never say READY.
manual = summary[summary.area == "Manual check"]
skipped = summary[(summary.status == "WARN") & (summary.area == "Generation")]
actionable = summary[(summary.status == "WARN")
                     & (summary.area != "Manual check")
                     & (summary.area != "Generation")]
n_manual, n_actionable = len(manual), len(actionable)

print("=" * 78)
print(f"  Ageas hackathon environment, {CATALOG}.{SCHEMA}")
print(f"  {n_pass} passed, {n_block} blockers, {n_actionable} warnings to look at, "
      f"{n_manual} to confirm with the platform owner")
if n_block:
    print("  NOT READY. See the blockers at the top of the table.")
elif n_actionable:
    print("  USABLE, WITH CAVEATS. Please read the warnings.")
else:
    print("  READY for 16 November, subject to the two manual checks below.")
if len(skipped):
    print("  Note: the generators were skipped, so the data checks reflect a previous run.")
print()
for _, r in actionable.iterrows():
    print(f"  warning: {r['area']} - {r['check']}: {r['detail'][:110]}")
for _, r in manual.iterrows():
    print(f"  confirm: {r['check']}")
print("=" * 78)

display(spark.createDataFrame(summary))

# COMMAND ----------

if counts:
    display(spark.createDataFrame(
        pd.DataFrame(sorted(counts.items()), columns=["table", "rows"])))

# COMMAND ----------

# MAGIC %md ## 6. Machine-readable result
# MAGIC
# MAGIC The same outcome as a compact JSON payload, so the result can be captured without a
# MAGIC screenshot when this notebook is run from a job or a scheduled check.

# COMMAND ----------

import json as _json

verdict = ("NOT_READY" if n_block else
           ("READY_WITH_CAVEATS" if n_actionable else "READY"))
payload = dict(
    verdict=verdict,
    target=f"{CATALOG}.{SCHEMA}",
    scale=SCALE,
    generators_run=RUN_GENERATORS,
    passed=n_pass, blockers=n_block,
    warnings_actionable=n_actionable, warnings_total=n_warn,
    manual_checks=n_manual,
    blocking=[dict(area=c["area"], check=c["check"], detail=c["detail"][:200])
              for c in checks if c["status"] == "BLOCKER"],
    warnings=[dict(area=r["area"], check=r["check"], detail=r["detail"][:200])
              for _, r in actionable.iterrows()],
    row_counts=counts,
    plausibility=[dict(metric=r["metric"], value=r["value"], expected=r["expected"],
                       status=r["status"]) for r in plaus_rows],
)
print(_json.dumps(payload, indent=2)[:4000])
dbutils.notebook.exit(_json.dumps(payload))
