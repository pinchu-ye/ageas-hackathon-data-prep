# Databricks notebook source
# MAGIC %md
# MAGIC # Ageas hackathon - synthetic data generator
# MAGIC
# MAGIC Builds the dataset used on the day: four business domains (policy, claims, pricing,
# MAGIC complaints) as Unity Catalog tables, plus PDF policy schedules, JSON quote events and
# MAGIC CSV extracts in a volume.
# MAGIC
# MAGIC **Everything here is synthetic.** No Ageas or esure data is read, and no real customer,
# MAGIC broker or claim is represented. Brand and heritage names are used only so the shapes feel
# MAGIC familiar.
# MAGIC
# MAGIC ## How to run it
# MAGIC 1. Attach to any cluster or SQL warehouse you can write Unity Catalog tables from.
# MAGIC 2. Set the four widgets at the top (catalog, schema, volume, scale).
# MAGIC 3. Run all. It takes roughly 5 to 10 minutes on a small cluster.
# MAGIC
# MAGIC No `%pip install` is needed, so it works on a locked-down cluster with no PyPI access.
# MAGIC Re-running is safe: every table is overwritten, so the output is identical each time
# MAGIC (the random seed is fixed).

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

try:
    dbutils  # noqa: F821
    IN_DATABRICKS = True
except NameError:
    IN_DATABRICKS = False

if IN_DATABRICKS:
    dbutils.widgets.text("catalog", "sb_training", "1. Catalog")  # noqa: F821
    dbutils.widgets.text("schema", "hackathon", "2. Schema")  # noqa: F821
    dbutils.widgets.text("volume", "landing", "3. Volume")  # noqa: F821
    dbutils.widgets.dropdown("scale", "full", ["smoke", "full"], "4. Scale")  # noqa: F821

    CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
    SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
    VOLUME = dbutils.widgets.get("volume")  # noqa: F821
    SCALE = dbutils.widgets.get("scale")  # noqa: F821
    VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
else:
    import os
    CATALOG = SCHEMA = VOLUME = "local"
    SCALE = os.environ.get("SCALE", "smoke")
    VOLUME_ROOT = os.environ.get("OUT", "/tmp/ageas_hackathon")

SEED = 20261116  # the date of the hackathon, so output is reproducible

SIZES = {
    "smoke": dict(customers=800, policies=1200, quotes=3000, complaints=120,
                  brokers=40, schedules=6),
    "full": dict(customers=40000, policies=55000, quotes=150000, complaints=2500,
                 brokers=120, schedules=60),
}[SCALE]

AS_OF = "2026-11-01"  # all history runs backwards from here

if IN_DATABRICKS:
    # Create these first: section 9 writes PDFs straight into the volume.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")  # noqa: F821

print(f"catalog={CATALOG} schema={SCHEMA} volume={VOLUME} scale={SCALE}")
print(f"volume_root={VOLUME_ROOT}")
print(SIZES)

# COMMAND ----------

# MAGIC %md ## 2. Reference data
# MAGIC
# MAGIC Small hand-written lists that give the generated rows their shape. Edit these if you want
# MAGIC the data to look more like your own book.

# COMMAND ----------

import datetime as dt
import json
import math
import os
import random
import pandas as pd

RNG = random.Random(SEED)
AS_OF_DATE = dt.date.fromisoformat(AS_OF)

FIRST_NAMES = [
    "Amelia", "Oliver", "Isla", "Noah", "Ava", "Leo", "Freya", "Arthur", "Ivy", "Oscar",
    "Grace", "Harry", "Daisy", "Henry", "Willow", "Jack", "Maya", "Charlie", "Elsie", "Thomas",
    "Priya", "Rohan", "Zainab", "Omar", "Chloe", "Lewis", "Aoife", "Callum", "Nia", "Dylan",
    "Beth", "Ryan", "Hannah", "Josh", "Sophie", "Adam", "Megan", "Tom", "Keira", "Ben",
]
LAST_NAMES = [
    "Abbott", "Barlow", "Chadwick", "Dunmore", "Ellery", "Fairweather", "Granger", "Halliwell",
    "Ingleby", "Jarvis", "Kingsley", "Lyndhurst", "Marchetti", "Newbury", "Okonkwo", "Pennington",
    "Quilter", "Rashford", "Sandiford", "Thorncroft", "Underhill", "Vance", "Whitlock", "Yardley",
    "Ashworth", "Brightwell", "Carrick", "Delaney", "Easterby", "Fenwick",
]

# Outward postcode area, the town it covers, and the region it rolls up to.
POSTCODE_AREAS = [
    ("SO", "Southampton", "South East"), ("BH", "Bournemouth", "South West"),
    ("PO", "Portsmouth", "South East"), ("GU", "Guildford", "South East"),
    ("RG", "Reading", "South East"), ("BS", "Bristol", "South West"),
    ("EX", "Exeter", "South West"), ("PL", "Plymouth", "South West"),
    ("B", "Birmingham", "Midlands"), ("CV", "Coventry", "Midlands"),
    ("NG", "Nottingham", "Midlands"), ("LE", "Leicester", "Midlands"),
    ("M", "Manchester", "North West"), ("L", "Liverpool", "North West"),
    ("LS", "Leeds", "Yorkshire"), ("S", "Sheffield", "Yorkshire"),
    ("NE", "Newcastle", "North East"), ("G", "Glasgow", "Scotland"),
    ("EH", "Edinburgh", "Scotland"), ("CF", "Cardiff", "Wales"),
    ("SA", "Swansea", "Wales"), ("BT", "Belfast", "Northern Ireland"),
    ("E", "London East", "London"), ("SW", "London South West", "London"),
    ("N", "London North", "London"), ("CR", "Croydon", "London"),
]

AGE_BANDS = ["18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75+"]
AGE_BAND_WEIGHTS = [6, 17, 20, 19, 16, 13, 9]

# product_code, line_of_business, product_name, brand, heritage, cover_level
PRODUCTS = [
    ("MOT-ESS", "Motor", "Car Insurance Essentials", "Ageas", "Ageas", "Essentials"),
    ("MOT-STD", "Motor", "Car Insurance Standard", "Ageas", "Ageas", "Standard"),
    ("MOT-PRM", "Motor", "Car Insurance Premier", "Ageas", "Ageas", "Premier"),
    ("MOT-ESU", "Motor", "Car Insurance", "esure", "esure", "Standard"),
    ("MOT-SWH", "Motor", "Car Insurance", "Sheilas' Wheels", "esure", "Standard"),
    ("HOM-STD", "Home", "Home Insurance Standard", "Ageas", "Ageas", "Standard"),
    ("HOM-PRM", "Home", "Home Insurance Premier", "Ageas", "Ageas", "Premier"),
    ("HOM-ESU", "Home", "Home Insurance", "esure", "esure", "Standard"),
    ("HOM-RIA", "Home", "Over 50s Home Insurance", "RIAS", "Ageas", "Premier"),
    ("TRV-STD", "Travel", "Travel Insurance Standard", "Ageas", "Ageas", "Standard"),
    ("TRV-ANN", "Travel", "Annual Multi-trip Travel", "Ageas", "Ageas", "Premier"),
    ("PET-STD", "Pet", "Pet Insurance", "Ageas", "Ageas", "Standard"),
]

LOB_PREMIUM = {  # mean, standard deviation of the net annual premium in GBP
    "Motor": (545.0, 185.0),
    "Home": (238.0, 85.0),
    "Travel": (96.0, 38.0),
    "Pet": (312.0, 120.0),
}
LOB_SUM_INSURED = {"Motor": (14000, 6000), "Home": (320000, 110000),
                   "Travel": (10000, 2000), "Pet": (8000, 2500)}
LOB_MIX = {"Motor": 0.52, "Home": 0.31, "Travel": 0.10, "Pet": 0.07}

CHANNELS = ["Broker", "Direct", "PCW", "Affinity"]
CHANNEL_WEIGHTS = [42, 23, 29, 6]

BROKER_PREFIX = ["Northgate", "Marchmont", "Castlereagh", "Hollybank", "Pennine", "Wessex",
                 "Clydeside", "Fenland", "Mercia", "Brightwater", "Kingsmead", "Avonvale",
                 "Thameside", "Tyneford", "Severn", "Lanark", "Harbourview", "Oakmere"]
BROKER_SUFFIX = ["Insurance Brokers", "Risk Partners", "Underwriting Services",
                 "Insurance Services", "Broking Group", "Protect"]
PANEL_TIERS = ["Strategic", "Core", "Tail"]
PANEL_TIER_WEIGHTS = [12, 38, 50]

# Claim type, average cost in GBP, cost spread, and how often it happens relative to the
# other types in the same line of business. Cheap, frequent perils dominate, which is what
# keeps the loss ratio in a believable range.
CLAIM_TYPES = {
    "Motor": [("Windscreen", 420, 160, 32), ("Accidental Damage", 1750, 1300, 31),
              ("Third Party Property", 2400, 1800, 20), ("Theft", 5000, 3200, 8),
              ("Personal Injury", 8800, 7000, 6), ("Fire", 11500, 8500, 3)],
    "Home": [("Accidental Damage", 900, 700, 29), ("Escape of Water", 3400, 2400, 24),
             ("Storm", 1800, 1400, 23), ("Theft", 2300, 1700, 16),
             ("Fire", 10000, 8000, 5), ("Subsidence", 15000, 10000, 3)],
    "Travel": [("Baggage", 380, 260, 34), ("Cancellation", 820, 650, 30),
               ("Medical Expenses", 2100, 2600, 26), ("Curtailment", 1300, 950, 10)],
    "Pet": [("Illness", 620, 520, 58), ("Injury", 520, 420, 34),
            ("Third Party Liability", 1800, 1600, 8)],
}

# Share of policies that produce at least one claim in the year, by line of business.
# Motor and pet claim often for small amounts; home claims rarely but expensively.
CLAIM_FREQUENCY = {"Motor": 0.160, "Home": 0.052, "Travel": 0.040, "Pet": 0.280}

# Broker panel tier multiplies claim frequency. Strategic agencies are selected and their
# business performs better; the tail is written on thinner underwriting. This is what makes
# "the broker who writes the most is not the broker you want more of" true in the data rather
# than just a nice line in a brief.
PANEL_TIER_FREQUENCY_FACTOR = {"Strategic": 0.72, "Core": 1.00, "Tail": 1.28}

# Late notification costs more. Damage deteriorates, evidence degrades, and the file is more
# likely to be disputed. It also correlates with fraud, which is why the two move together.
LATE_NOTIFICATION_COST_PER_DAY = 0.016   # applied to the first 30 days of delay
LATE_NOTIFICATION_FRAUD_PER_DAY = 0.011  # added to the mean fraud score per day of delay

# Claims are routed by line of business, and large losses go to a specialist team. A motor
# windscreen claim sitting with Home Large Loss is the kind of detail an insurance audience
# spots immediately, and it would make any analysis of handler performance meaningless.
HANDLER_ROUTING = {
    "Motor": ("Motor First Notification", "Motor Large Loss"),
    "Home": ("Home Property", "Home Large Loss"),
    "Travel": ("Travel Assistance", "Travel Assistance"),
    "Pet": ("Pet Claims", "Pet Claims"),
}
LARGE_LOSS_THRESHOLD_GBP = 10000.0
HANDLER_TEAMS = ["Motor First Notification", "Motor Large Loss", "Home Property",
                 "Home Large Loss", "Travel Assistance", "Pet Claims", "Counter Fraud"]
CLAIM_STATUSES = ["Settled", "Open", "Repudiated", "Withdrawn"]
CLAIM_STATUS_WEIGHTS = [71, 18, 6, 5]

DECLINE_REASONS = ["Outside underwriting footprint", "Vehicle not acceptable",
                   "Claims history", "Unacceptable occupation", "Property type not covered"]
RATING_MODELS = ["GLM-2024.4", "GLM-2025.2", "GBM-2025.3", "GBM-2026.1"]

COMPLAINT_ROOT_CAUSES = [
    "Claim delay", "Claim decision disputed", "Premium increase at renewal",
    "Service standards", "Mis-sold cover", "Policy wording unclear",
    "Cancellation administration", "Payment or refund error", "Repair quality",
    "Vulnerable customer handling",
]
COMPLAINT_CHANNELS = ["Telephone", "Email", "Letter", "Web form", "Social media", "Broker"]
SENTIMENTS = ["Frustrated", "Angry", "Neutral", "Resigned", "Distressed"]

print(f"{len(PRODUCTS)} products, {len(POSTCODE_AREAS)} postcode areas, "
      f"{len(COMPLAINT_ROOT_CAUSES)} complaint root causes")

# COMMAND ----------

# MAGIC %md ## 3. Helpers

# COMMAND ----------


def pick(seq, weights=None):
    if weights:
        return RNG.choices(seq, weights=weights, k=1)[0]
    return RNG.choice(seq)


def money(mean, sd, floor=25.0):
    """A plausible, positive, 2dp currency amount."""
    return round(max(floor, RNG.gauss(mean, sd)), 2)


def to_date(value):
    """Normalise whatever pandas hands back into a datetime.date."""
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    return pd.Timestamp(value).date()


def date_between(start, end):
    """A random date in [start, end]. Callers must ensure end is not before start:
    a negative span silently collapses to `start`, which is how 388 claims ended up with
    incident dates in the future."""
    span = (to_date(end) - to_date(start)).days
    return to_date(start) + dt.timedelta(days=RNG.randint(0, max(span, 0)))


def ref(prefix, n, width=7):
    return f"{prefix}-{n:0{width}d}"


def lob_for_product(code):
    return next(p[1] for p in PRODUCTS if p[0] == code)


PRODUCTS_BY_LOB = {}
for p in PRODUCTS:
    PRODUCTS_BY_LOB.setdefault(p[1], []).append(p[0])

# COMMAND ----------

# MAGIC %md ## 4. Dimensions

# COMMAND ----------

customers = []
for i in range(1, SIZES["customers"] + 1):
    area, town, region = pick(POSTCODE_AREAS)
    since = date_between(AS_OF_DATE - dt.timedelta(days=365 * 12), AS_OF_DATE)
    customers.append(dict(
        customer_id=ref("CUST", i),
        customer_name=f"{pick(FIRST_NAMES)} {pick(LAST_NAMES)}",
        postcode_area=area,
        postcode_town=town,
        region=region,
        age_band=pick(AGE_BANDS, AGE_BAND_WEIGHTS),
        customer_since_date=since,
        tenure_years=max(0, (AS_OF_DATE - since).days // 365),
        preferred_contact_channel=pick(["Email", "Telephone", "Post", "App"], [52, 24, 9, 15]),
        is_vulnerable_customer=RNG.random() < 0.07,
        marketing_opt_in=RNG.random() < 0.61,
    ))
dim_customer = pd.DataFrame(customers)

dim_product = pd.DataFrame(PRODUCTS, columns=[
    "product_code", "line_of_business", "product_name", "brand", "heritage_book", "cover_level"])

brokers = []
for i in range(1, SIZES["brokers"] + 1):
    area, town, region = pick(POSTCODE_AREAS)
    tier = pick(PANEL_TIERS, PANEL_TIER_WEIGHTS)
    brokers.append(dict(
        broker_id=ref("BRK", i, 4),
        broker_name=f"{pick(BROKER_PREFIX)} {pick(BROKER_SUFFIX)}",
        broker_group=pick(["Independent", "Network - Cobra", "Network - Broker Network",
                           "Consolidator - Ardonagh", "Consolidator - PIB"], [48, 14, 13, 15, 10]),
        panel_tier=tier,
        region=region,
        appointed_date=date_between(AS_OF_DATE - dt.timedelta(days=365 * 15), AS_OF_DATE),
        commission_rate_pct={"Strategic": 22.5, "Core": 18.0, "Tail": 15.0}[tier],
        is_active=RNG.random() < 0.93,
    ))
dim_broker = pd.DataFrame(brokers)
# Broker names repeat by construction; keep one row per broker_id so the key stays unique.
dim_broker["broker_name"] = dim_broker["broker_name"] + " (" + dim_broker["broker_id"] + ")"

print(f"dim_customer={len(dim_customer)}  dim_product={len(dim_product)}  "
      f"dim_broker={len(dim_broker)}")

# COMMAND ----------

# MAGIC %md ## 5. Policies
# MAGIC
# MAGIC Three years of annual policies. Roughly a third are renewals of an earlier policy, which
# MAGIC is what makes retention measurable.

# COMMAND ----------

IPT_RATE = 0.12  # UK standard rate of Insurance Premium Tax

policies = []
policy_ids_by_customer = {}
window_start = AS_OF_DATE - dt.timedelta(days=365 * 3)

for i in range(1, SIZES["policies"] + 1):
    cust = dim_customer.iloc[RNG.randrange(len(dim_customer))]
    lob = pick(list(LOB_MIX), list(LOB_MIX.values()))
    product_code = pick(PRODUCTS_BY_LOB[lob])
    channel = pick(CHANNELS, CHANNEL_WEIGHTS)
    broker_id = pick(dim_broker["broker_id"].tolist()) if channel == "Broker" else None

    start = date_between(window_start, AS_OF_DATE + dt.timedelta(days=60))
    end = start + dt.timedelta(days=364)

    mean, sd = LOB_PREMIUM[lob]
    # Premier cover and London addresses cost more; Essentials costs less.
    cover = next(p[5] for p in PRODUCTS if p[0] == product_code)
    factor = {"Essentials": 0.82, "Standard": 1.0, "Premier": 1.28}[cover]
    if cust["region"] == "London":
        factor *= 1.22
    net = money(mean * factor, sd * factor, floor=45.0)
    ipt = round(net * IPT_RATE, 2)

    prior = policy_ids_by_customer.get(cust["customer_id"])
    is_renewal = prior is not None and RNG.random() < 0.62

    if start > AS_OF_DATE:
        # Written, but not yet on risk. Counts towards written premium, not in-force.
        status = "Not Yet Incepted"
    elif end < AS_OF_DATE:
        status = pick(["Expired", "Lapsed", "Cancelled"], [74, 18, 8])
    else:
        status = pick(["Active", "Cancelled"], [96, 4])

    si_mean, si_sd = LOB_SUM_INSURED[lob]
    policies.append(dict(
        policy_id=ref("POL", i),
        customer_id=cust["customer_id"],
        product_code=product_code,
        broker_id=broker_id,
        distribution_channel=channel,
        policy_start_date=start,
        policy_end_date=end,
        policy_status=status,
        is_renewal=is_renewal,
        previous_policy_id=prior if is_renewal else None,
        net_premium_gbp=net,
        ipt_gbp=ipt,
        gross_written_premium_gbp=round(net + ipt, 2),
        commission_gbp=round(net * 0.18, 2) if broker_id else 0.0,
        sum_insured_gbp=float(max(1000, int(RNG.gauss(si_mean, si_sd)))),
        excess_gbp=float(pick([100, 150, 250, 350, 500, 750], [14, 22, 31, 17, 11, 5])),
        payment_method=pick(["Annual", "Monthly Direct Debit"], [38, 62]),
        auto_renew_flag=RNG.random() < 0.78,
    ))
    policy_ids_by_customer[cust["customer_id"]] = ref("POL", i)

fact_policy = pd.DataFrame(policies)
print(f"fact_policy={len(fact_policy)}  GWP="
      f"{fact_policy['gross_written_premium_gbp'].sum():,.0f} GBP")

# COMMAND ----------

# MAGIC %md ## 6. Claims
# MAGIC
# MAGIC Three dates on purpose: the incident happened, it was notified later, and it closed later
# MAGIC still. This is exactly the shape that confuses an agent with thin metadata, so the column
# MAGIC comments applied in section 11 matter.

# COMMAND ----------

panel_tier_by_broker = dim_broker.set_index("broker_id")["panel_tier"].to_dict()
policy_lookup = fact_policy[["policy_id", "product_code", "policy_start_date",
                             "policy_end_date", "net_premium_gbp",
                             "broker_id"]].to_dict("records")

# Walk every policy and decide whether it claimed, rather than drawing a fixed number of
# claims. Claim volume then falls out of the frequency assumptions above, and the loss ratio
# stays believable without being hand-tuned.
claims = []
claim_seq = 0
for pol in policy_lookup:
    # A policy that has not incepted yet cannot have claimed. Without this guard the
    # incident date falls back to the policy start date, which is in the future.
    if to_date(pol["policy_start_date"]) > AS_OF_DATE:
        continue
    lob = lob_for_product(pol["product_code"])
    tier = panel_tier_by_broker.get(pol["broker_id"])
    frequency = CLAIM_FREQUENCY[lob] * PANEL_TIER_FREQUENCY_FACTOR.get(tier, 1.0)
    draws = RNG.random()
    n_claims = 0
    if draws < frequency:
        # A small share of claimants claim twice in the year.
        n_claims = 2 if RNG.random() < 0.08 else 1
    for _ in range(n_claims):
        claim_seq += 1
        i = claim_seq
        types = CLAIM_TYPES[lob]
        claim_type, mean, sd, _w = RNG.choices(
            types, weights=[t[3] for t in types], k=1)[0]

        incident = date_between(pol["policy_start_date"],
                                min(pol["policy_end_date"], AS_OF_DATE))
        notified = min(
            incident + dt.timedelta(days=RNG.choices(
                [0, 1, 2, 5, 12, 30], weights=[38, 22, 14, 13, 8, 5], k=1)[0]),
            AS_OF_DATE)
        status = pick(CLAIM_STATUSES, CLAIM_STATUS_WEIGHTS)

        lag = (notified - incident).days
        late_factor = 1.0 + min(lag, 30) * LATE_NOTIFICATION_COST_PER_DAY
        incurred = money(mean * late_factor, sd * late_factor, floor=80.0)
        if status == "Settled":
            closed = notified + dt.timedelta(days=int(abs(RNG.gauss(34, 28))) + 2)
        elif status in ("Repudiated", "Withdrawn"):
            closed = notified + dt.timedelta(days=int(abs(RNG.gauss(21, 18))) + 1)
        else:
            closed = None
        # A claim cannot have closed in the future, so anything that would is still open.
        if closed and closed > AS_OF_DATE:
            closed, status = None, "Open"

        if status == "Settled":
            paid, reserve = incurred, 0.0
        elif status in ("Repudiated", "Withdrawn"):
            # Nothing is paid and no reserve is held on a claim we did not meet.
            incurred, paid, reserve = 0.0, 0.0, 0.0
        else:
            paid = round(incurred * RNG.uniform(0.0, 0.55), 2)
            reserve = round(incurred - paid, 2)

        fraud_score = round(min(0.99, abs(RNG.gauss(
            0.12 + LATE_NOTIFICATION_FRAUD_PER_DAY * min(lag, 30), 0.19))), 3)
        # Referred claims move to Counter Fraud; otherwise route by line of business and
        # whether the claim is a large loss.
        standard_team, large_loss_team = HANDLER_ROUTING[lob]
        if fraud_score > 0.55:
            handler_team = "Counter Fraud"
        elif incurred > LARGE_LOSS_THRESHOLD_GBP:
            handler_team = large_loss_team
        else:
            handler_team = standard_team
        claims.append(dict(
            claim_id=ref("CLM", i),
            policy_id=pol["policy_id"],
            product_code=pol["product_code"],
            claim_type=claim_type,
            incident_date=incident,
            notified_date=notified,
            closed_date=closed,
            claim_status=status,
            incurred_amount_gbp=incurred,
            paid_amount_gbp=paid,
            outstanding_reserve_gbp=reserve,
            handler_team=handler_team,
            fraud_score=fraud_score,
            fraud_referral_flag=fraud_score > 0.55,
            days_to_notify=(notified - incident).days,
            days_to_close=(closed - notified).days if closed else None,
        ))

fact_claim = pd.DataFrame(claims)
print(f"fact_claim={len(fact_claim)}  incurred="
      f"{fact_claim['incurred_amount_gbp'].sum():,.0f} GBP  "
      f"open={(fact_claim['claim_status'] == 'Open').sum()}")

# COMMAND ----------

# MAGIC %md ## 7. Quotes
# MAGIC
# MAGIC The pricing table. Conversion falls away as the quoted price moves above the best
# MAGIC competitor price, which gives the pricing team a real elasticity signal to find.

# COMMAND ----------

quotes = []
for i in range(1, SIZES["quotes"] + 1):
    lob = pick(list(LOB_MIX), list(LOB_MIX.values()))
    product_code = pick(PRODUCTS_BY_LOB[lob])
    channel = pick(CHANNELS, CHANNEL_WEIGHTS)
    mean, sd = LOB_PREMIUM[lob]

    rated = money(mean, sd, floor=40.0)
    # What we actually showed the customer, after any discount or loading.
    quoted = round(rated * RNG.uniform(0.92, 1.14), 2)
    competitor_best = round(rated * RNG.uniform(0.74, 1.26), 2)
    ratio = quoted / competitor_best

    # Rank against a notional PCW panel of 20.
    rank = max(1, min(20, int(round(1 + (ratio - 0.8) * 26))))
    declined = RNG.random() < 0.04
    if declined:
        converted, decline_reason = False, pick(DECLINE_REASONS)
    else:
        base = {"PCW": 0.11, "Broker": 0.27, "Direct": 0.21, "Affinity": 0.33}[channel]
        p = base * math.exp(-3.1 * max(0.0, ratio - 0.95))
        converted, decline_reason = RNG.random() < p, None

    quotes.append(dict(
        quote_id=ref("QTE", i, 8),
        quote_date=date_between(AS_OF_DATE - dt.timedelta(days=365 * 2), AS_OF_DATE),
        product_code=product_code,
        distribution_channel=channel,
        customer_age_band=pick(AGE_BANDS, AGE_BAND_WEIGHTS),
        region=pick(POSTCODE_AREAS)[2],
        no_claims_years=int(pick([0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                                 [9, 8, 9, 10, 11, 12, 11, 10, 10, 10])),
        rated_premium_gbp=rated,
        quoted_premium_gbp=quoted,
        competitor_best_premium_gbp=competitor_best,
        price_position_rank=rank,
        risk_score=round(min(0.999, abs(RNG.gauss(0.34, 0.16))), 3),
        rating_model_version=pick(RATING_MODELS),
        is_converted=converted,
        decline_reason=decline_reason,
    ))

fact_quote = pd.DataFrame(quotes)
print(f"fact_quote={len(fact_quote)}  conversion="
      f"{fact_quote['is_converted'].mean():.1%}")

# COMMAND ----------

# MAGIC %md ## 8. Complaints
# MAGIC
# MAGIC Each row carries a short free-text summary and a call transcript, so the governance and
# MAGIC conduct questions can be answered from text rather than codes alone.

# COMMAND ----------

TRANSCRIPT_OPENERS = [
    "Agent: Good morning, you are through to {brand}, my name is {agent}. How can I help?",
    "Agent: Thanks for holding, {brand} claims, {agent} speaking.",
    "Agent: Hello, {brand} customer care, this is {agent}.",
]
TRANSCRIPT_BODY = {
    "Claim delay": [
        "Customer: I notified the claim {days} days ago on policy {policy} and nobody has come back to me.",
        "Agent: I can see the claim was logged and is with the {team} team. I am sorry about the wait.",
        "Customer: I have called three times. Each time I am told someone will ring back and nobody does.",
        "Agent: That is not the standard we should be holding. Let me escalate it and get you a call today.",
    ],
    "Claim decision disputed": [
        "Customer: You have declined my claim and I do not accept the reason.",
        "Agent: The file says the damage was assessed as gradual rather than a single event.",
        "Customer: The report I paid for says the opposite. I want this looked at again.",
        "Agent: You are entitled to challenge it. I will request a technical review and write to you.",
    ],
    "Premium increase at renewal": [
        "Customer: My renewal has gone up by {pct} percent and I have not claimed at all.",
        "Agent: I can see no claims on the policy. The increase is from the rating review and claims inflation.",
        "Customer: That is not my problem. I can get it cheaper elsewhere in two minutes.",
        "Agent: Let me see what I can do on the price and talk you through the cover you would lose.",
    ],
    "Service standards": [
        "Customer: I have been passed between four people this morning.",
        "Agent: I am sorry, I can see the transfers on the record.",
        "Customer: Nobody owns this. I just want one person to deal with it.",
        "Agent: I will own it from here and give you my reference so you come straight back to me.",
    ],
    "Mis-sold cover": [
        "Customer: I was told legal cover was included and now I am told it is not.",
        "Agent: Looking at the schedule, legal expenses was not added at inception.",
        "Customer: Then I was given the wrong information when I bought it.",
        "Agent: I will pull the sales call and have it reviewed. If we got it wrong we will put it right.",
    ],
    "Policy wording unclear": [
        "Customer: The wording on escape of water does not say anything about this situation.",
        "Agent: I can see why that reads as ambiguous. Let me get you the section and a plain explanation.",
        "Customer: I should not need a lawyer to understand what I bought.",
        "Agent: Agreed. I am logging this as feedback on the wording as well as handling your case.",
    ],
    "Cancellation administration": [
        "Customer: I cancelled six weeks ago and you are still taking the direct debit.",
        "Agent: I can see the cancellation was recorded but the collection was not stopped.",
        "Customer: So you have taken money you were not owed.",
        "Agent: We have. I will refund it and add compensation for the error.",
    ],
    "Payment or refund error": [
        "Customer: My refund is {amount} pounds short of what I was quoted.",
        "Agent: I can see two adjustments on the account that were not explained to you.",
        "Customer: I want the difference and I want to know why it happened.",
        "Agent: That is fair. I will correct the balance and send a written breakdown.",
    ],
    "Repair quality": [
        "Customer: The repair has failed again. This is the second time.",
        "Agent: I can see the first repair was signed off in {month}.",
        "Customer: I do not want the same garage touching it again.",
        "Agent: Understood, I will arrange an independent inspection and a different approved repairer.",
    ],
    "Vulnerable customer handling": [
        "Customer: I told you I am registered blind and you keep sending me letters I cannot read.",
        "Agent: I can see the accessibility preference on the record was not applied. I am very sorry.",
        "Customer: It has taken four calls to get anyone to listen.",
        "Agent: I am fixing the preference now and I will call you to confirm rather than write.",
    ],
}
AGENT_NAMES = ["Priya", "Dan", "Shona", "Mo", "Katie", "Raf", "Bev", "Niall"]

vulnerable_by_customer = dim_customer.set_index(
    "customer_id")["is_vulnerable_customer"].to_dict()
_started = fact_policy.loc[
    fact_policy["policy_start_date"].map(to_date) <= AS_OF_DATE,
    ["policy_id", "product_code", "customer_id"]].copy()
_started["is_vulnerable_customer"] = _started["customer_id"].map(vulnerable_by_customer)
complaint_policies = _started.to_dict("records")

# Complaints from customers with a recorded vulnerability take longer to resolve. This is a
# real conduct pattern and the reason Consumer Duty exists, so the governance team should be
# able to find it rather than be told it is there.
VULNERABLE_DELAY_MULTIPLIER = (1.45, 2.10)

complaints = []
for i in range(1, SIZES["complaints"] + 1):
    pol = complaint_policies[RNG.randrange(len(complaint_policies))]
    brand = next(p[3] for p in PRODUCTS if p[0] == pol["product_code"])
    cause = pick(COMPLAINT_ROOT_CAUSES)
    received = date_between(AS_OF_DATE - dt.timedelta(days=365 * 2), AS_OF_DATE)

    # Most complaints close quickly, but a minority drag badly. Without that tail nothing
    # ever breaches the 8 week service standard and the measure is dead on arrival.
    if RNG.random() < 0.07:
        days_open = RNG.randint(58, 150)
    else:
        days_open = int(abs(RNG.gauss(17, 14))) + 1
    if pol.get("is_vulnerable_customer"):
        days_open = int(days_open * RNG.uniform(*VULNERABLE_DELAY_MULTIPLIER))
    closed = received + dt.timedelta(days=days_open)
    if closed > AS_OF_DATE:
        closed, status = None, "Open"
    else:
        status = "Closed"

    upheld = RNG.random() < {"Claim delay": 0.62, "Cancellation administration": 0.71,
                             "Payment or refund error": 0.74,
                             "Vulnerable customer handling": 0.68}.get(cause, 0.41)
    redress = money(180, 220, floor=0.0) if (upheld and status == "Closed") else 0.0
    fos = status == "Closed" and not upheld and RNG.random() < 0.09

    fields = dict(brand=brand, agent=pick(AGENT_NAMES), policy=pol["policy_id"],
                  days=RNG.randint(8, 40), pct=RNG.randint(14, 62),
                  amount=RNG.randint(20, 240), team=pick(HANDLER_TEAMS),
                  month=received.strftime("%B"))
    transcript = "\n".join(
        [pick(TRANSCRIPT_OPENERS).format(**fields)]
        + [line.format(**fields) for line in TRANSCRIPT_BODY[cause]]
    )

    complaints.append(dict(
        complaint_id=ref("CMP", i, 6),
        policy_id=pol["policy_id"],
        product_code=pol["product_code"],
        received_date=received,
        closed_date=closed,
        complaint_status=status,
        complaint_channel=pick(COMPLAINT_CHANNELS),
        root_cause_category=cause,
        is_upheld=upheld if status == "Closed" else None,
        referred_to_fos=fos,
        redress_paid_gbp=redress,
        days_to_close=(closed - received).days if closed else None,
        customer_sentiment=pick(SENTIMENTS),
        complaint_summary=f"{cause} raised on policy {pol['policy_id']} "
                          f"({brand}). Resolution target 8 weeks from receipt.",
        call_transcript=transcript,
    ))

fact_complaint = pd.DataFrame(complaints)
print(f"fact_complaint={len(fact_complaint)}  uphold_rate="
      f"{fact_complaint['is_upheld'].mean():.1%}  fos={fact_complaint['referred_to_fos'].sum()}")

# COMMAND ----------

# MAGIC %md ## 9. Policy schedule PDFs
# MAGIC
# MAGIC A dependency-free PDF writer, so this cell works on a cluster with no PyPI access.
# MAGIC The PDFs are what the document-extraction part of the morning session reads.

# COMMAND ----------

PAGE_W, PAGE_H, MARGIN, LEADING, FONT_SIZE = 595, 842, 56, 15, 10


def _esc(text):
    return str(text).replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _stream_for(lines):
    out = ["BT", f"1 0 0 1 {MARGIN} {PAGE_H - MARGIN} Tm", f"{LEADING} TL"]
    for ln in lines:
        out.append(f"{'/F2' if ln.get('bold') else '/F1'} {ln.get('size', FONT_SIZE)} Tf")
        if ln.get("gap"):
            out.append(f"0 -{ln['gap']} Td")
        if ln.get("indent"):
            out.append(f"{ln['indent']} 0 Td")
        out.append(f"({_esc(ln.get('text', ''))}) Tj")
        if ln.get("indent"):
            out.append(f"-{ln['indent']} 0 Td")
        out.append("T*")
    out.append("ET")
    return "\n".join(out)


def write_pdf(path, lines, title="Document"):
    """Write `lines` (dicts of text/bold/size/indent/gap) to a paginated A4 PDF."""
    usable, pages, cur, used = PAGE_H - 2 * MARGIN, [], [], 0.0
    for ln in lines:
        cost = LEADING + ln.get("gap", 0)
        if used + cost > usable and cur:
            pages.append(cur)
            cur, used, ln, cost = [], 0.0, dict(ln, gap=0), LEADING
        cur.append(ln)
        used += cost
    if cur:
        pages.append(cur)

    n = len(pages)
    objs = {
        1: "<< /Type /Catalog /Pages 2 0 R >>",
        2: f"<< /Type /Pages /Kids [{' '.join(f'{5 + 2 * i} 0 R' for i in range(n))}] "
           f"/Count {n} >>",
        3: "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica "
           "/Encoding /WinAnsiEncoding >>",
        4: "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold "
           "/Encoding /WinAnsiEncoding >>",
    }
    streams = {}
    for i, pg in enumerate(pages):
        pid, cid = 5 + 2 * i, 6 + 2 * i
        objs[pid] = (f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_W} {PAGE_H}] "
                     f"/Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents {cid} 0 R >>")
        objs[cid] = None
        streams[cid] = _stream_for(pg).encode("latin-1", "replace")

    buf, offsets = bytearray(b"%PDF-1.4\n"), {}
    for num in sorted(objs):
        offsets[num] = len(buf)
        buf += f"{num} 0 obj\n".encode()
        if objs[num] is None:
            body = streams[num]
            buf += f"<< /Length {len(body)} >>\nstream\n".encode() + body + b"\nendstream\n"
        else:
            buf += objs[num].encode("latin-1", "replace") + b"\n"
        buf += b"endobj\n"

    xref_at, top = len(buf), max(objs) + 1
    buf += f"xref\n0 {top}\n".encode() + b"0000000000 65535 f \n"
    for num in range(1, top):
        buf += f"{offsets.get(num, 0):010d} 00000 n \n".encode()
    buf += (f"trailer\n<< /Size {top} /Root 1 0 R /Info << /Title ({_esc(title)}) "
            f"/Producer (Ageas hackathon synthetic data generator) >> >>\n"
            f"startxref\n{xref_at}\n%%EOF\n").encode()

    with open(path, "wb") as fh:
        fh.write(bytes(buf))
    return path


SECTION_WORDING = {
    "Motor": [
        "Section 1 - Loss of or damage to your vehicle",
        "We will cover loss of or damage to your vehicle caused by accident, fire, theft or",
        "attempted theft, up to the market value at the time of the loss. The excess shown in",
        "this schedule applies to each and every claim under this section.",
        "Section 2 - Liability to third parties",
        "We will cover your legal liability for death of or injury to any person and for damage",
        "to third party property arising from the use of your vehicle on a public road.",
        "Section 3 - Windscreen damage",
        "Repair or replacement of glass in the windscreen or windows of your vehicle. A",
        "separate windscreen excess applies and no claim under this section affects your",
        "no claims discount.",
        "Section 4 - Personal belongings",
        "Loss of or damage to personal belongings in your vehicle, up to the limit shown.",
    ],
    "Home": [
        "Section 1 - Buildings",
        "We will cover the cost of repairing or rebuilding the buildings of your home following",
        "loss or damage caused by an insured event, including fire, storm, flood, escape of",
        "water, theft and subsidence, up to the sum insured shown in this schedule.",
        "Section 2 - Contents",
        "We will cover loss of or damage to the contents of your home caused by an insured",
        "event. Valuables are covered up to the single item limit shown unless specified.",
        "Section 3 - Escape of water",
        "Damage caused by water escaping from a fixed domestic water system. Trace and",
        "access costs are included up to the limit shown. Gradual seepage is not covered.",
        "Section 4 - Alternative accommodation",
        "Reasonable costs of alternative accommodation if your home cannot be lived in.",
    ],
    "Travel": [
        "Section 1 - Emergency medical expenses",
        "We will cover necessary medical, hospital and repatriation costs incurred abroad",
        "following illness or injury during your trip, up to the limit shown.",
        "Section 2 - Cancellation and curtailment",
        "We will cover irrecoverable deposits and charges if you have to cancel or cut short",
        "your trip for an insured reason, up to the limit shown.",
        "Section 3 - Baggage and personal money",
        "Loss, theft of or damage to baggage and personal money, subject to the single item",
        "limit and the excess shown in this schedule.",
    ],
    "Pet": [
        "Section 1 - Veterinary fees",
        "We will cover the cost of treatment by a qualified veterinary surgeon for illness or",
        "injury, up to the annual limit shown in this schedule.",
        "Section 2 - Third party liability",
        "Your legal liability for injury to a person or damage to property caused by your dog.",
        "Section 3 - Complementary treatment",
        "Physiotherapy, hydrotherapy and acupuncture when referred by your vet.",
    ],
}

EXCLUSIONS = [
    "Wear, tear and gradual deterioration.",
    "Any loss you have not told us about within a reasonable time.",
    "Loss or damage caused deliberately by you or anyone living with you.",
    "Any amount below the excess shown in this schedule.",
    "Mechanical or electrical breakdown unless cover has been added.",
]


def build_schedule(policy_row, customer_row, product_row, broker_name):
    pid = policy_row["policy_id"]
    lob = product_row["line_of_business"]
    lines = [
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL POLICY", "bold": True, "size": 9},
        {"text": f"{product_row['brand'].upper()}", "bold": True, "size": 16, "gap": 10},
        {"text": "Policy Schedule", "size": 12},
        {"text": f"{product_row['product_name']} - {product_row['cover_level']} cover",
         "size": 10, "gap": 2},
        {"text": "Your policy details", "bold": True, "size": 11, "gap": 14},
        {"text": f"Policy number: {pid}"},
        {"text": f"Policyholder: {customer_row['customer_name']}"},
        {"text": f"Address area: {customer_row['postcode_area']}, "
                 f"{customer_row['postcode_town']}"},
        {"text": f"Product code: {product_row['product_code']}"},
        {"text": f"Line of business: {lob}"},
        {"text": f"Period of cover: {policy_row['policy_start_date']} to "
                 f"{policy_row['policy_end_date']}"},
        {"text": f"Sold through: {policy_row['distribution_channel']}"
                 + (f" ({broker_name})" if broker_name else "")},
        {"text": f"Payment method: {policy_row['payment_method']}"},
        {"text": "What you pay", "bold": True, "size": 11, "gap": 14},
        {"text": f"Net premium: GBP {policy_row['net_premium_gbp']:,.2f}"},
        {"text": f"Insurance Premium Tax at 12 percent: GBP {policy_row['ipt_gbp']:,.2f}"},
        {"text": f"Total premium payable: GBP "
                 f"{policy_row['gross_written_premium_gbp']:,.2f}", "bold": True},
        {"text": "Limits and excesses", "bold": True, "size": 11, "gap": 14},
        {"text": f"Sum insured: GBP {policy_row['sum_insured_gbp']:,.0f}"},
        {"text": f"Standard excess: GBP {policy_row['excess_gbp']:,.0f}"},
        {"text": "Single item limit: GBP 1,500 unless separately specified"},
        {"text": "What is covered", "bold": True, "size": 11, "gap": 14},
    ]
    lines += [{"text": t, "bold": t.startswith("Section")} for t in SECTION_WORDING[lob]]
    lines.append({"text": "What is not covered", "bold": True, "size": 11, "gap": 14})
    lines += [{"text": f"- {t}"} for t in EXCLUSIONS]
    lines.append({"text": "Making a claim", "bold": True, "size": 11, "gap": 14})
    lines += [
        {"text": "Call the claims line shown in your policy booklet as soon as you can. Please"},
        {"text": "have your policy number ready. If you are unhappy with how we handle your"},
        {"text": "claim you can complain to us and then refer the matter to the Financial"},
        {"text": "Ombudsman Service within six months of our final response."},
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL POLICY", "bold": True, "size": 9,
         "gap": 14},
    ]
    return lines


def fresh_dir(path):
    """Create `path`, and remove any files a previous run left in it.

    Re-running the generator has to leave the volume holding exactly this run's output.
    Table writes overwrite, but file writes do not: any change that moves a policy or claim
    id also moves the file name, so the previous run's PDFs stay behind. Every `*.pdf` glob
    in the tutorial then reads two generations of documents against one generation of ground
    truth, and the measured extraction accuracy silently becomes meaningless.
    """
    os.makedirs(path, exist_ok=True)
    for name in os.listdir(path):
        victim = os.path.join(path, name)
        if os.path.isfile(victim):
            os.remove(victim)
    return path


schedule_dir = fresh_dir(os.path.join(VOLUME_ROOT, "policy_schedules"))

cust_by_id = dim_customer.set_index("customer_id").to_dict("index")
prod_by_code = dim_product.set_index("product_code").to_dict("index")
broker_by_id = dim_broker.set_index("broker_id")["broker_name"].to_dict()

# Pick schedules from policies that are still in force, so the documents look current.
in_force = fact_policy[fact_policy["policy_status"] == "Active"]
if len(in_force) < SIZES["schedules"]:
    in_force = fact_policy
sample = in_force.sample(n=SIZES["schedules"], random_state=SEED).to_dict("records")

expected_rows = []
for row in sample:
    product = prod_by_code[row["product_code"]]
    product = dict(product, product_code=row["product_code"])
    customer = cust_by_id[row["customer_id"]]
    broker_name = broker_by_id.get(row["broker_id"]) if row["broker_id"] else None
    path = os.path.join(schedule_dir, f"{row['policy_id']}_schedule.pdf")
    write_pdf(path, build_schedule(row, customer, product, broker_name),
              title=f"Policy Schedule {row['policy_id']}")
    expected_rows.append(dict(
        file_name=os.path.basename(path),
        policy_id=row["policy_id"],
        policyholder_name=customer["customer_name"],
        product_code=row["product_code"],
        line_of_business=product["line_of_business"],
        cover_level=product["cover_level"],
        brand=product["brand"],
        cover_start_date=row["policy_start_date"],
        cover_end_date=row["policy_end_date"],
        net_premium_gbp=row["net_premium_gbp"],
        ipt_gbp=row["ipt_gbp"],
        total_premium_gbp=row["gross_written_premium_gbp"],
        sum_insured_gbp=row["sum_insured_gbp"],
        excess_gbp=row["excess_gbp"],
    ))

policy_schedule_expected = pd.DataFrame(expected_rows)
print(f"wrote {len(expected_rows)} PDFs to {schedule_dir}")

# COMMAND ----------

# MAGIC %md ## 10. JSON quote events and CSV extracts
# MAGIC
# MAGIC The JSON is deliberately nested, so reading it is a worked example of `read_files`
# MAGIC and semi-structured handling rather than a flat load.

# COMMAND ----------

raw_dir = fresh_dir(os.path.join(VOLUME_ROOT, "raw"))

json_sample = fact_quote.sample(n=min(len(fact_quote), 20000),
                                random_state=SEED).to_dict("records")
json_path = os.path.join(raw_dir, "quote_events.json")
with open(json_path, "w") as fh:
    for q in json_sample:
        fh.write(json.dumps({
            "event_id": q["quote_id"],
            "event_type": "quote.priced",
            "event_timestamp": f"{q['quote_date']}T"
                               f"{RNG.randint(7, 21):02d}:{RNG.randint(0, 59):02d}:00Z",
            "channel": q["distribution_channel"],
            "product": {"code": q["product_code"],
                        "line_of_business": lob_for_product(q["product_code"])},
            "risk": {"age_band": q["customer_age_band"], "region": q["region"],
                     "no_claims_years": q["no_claims_years"],
                     "risk_score": q["risk_score"]},
            "pricing": {"rated_premium_gbp": q["rated_premium_gbp"],
                        "quoted_premium_gbp": q["quoted_premium_gbp"],
                        "model_version": q["rating_model_version"],
                        "competitor_best_gbp": q["competitor_best_premium_gbp"],
                        "price_position_rank": q["price_position_rank"]},
            "outcome": {"converted": bool(q["is_converted"]),
                        "decline_reason": q["decline_reason"]},
        }) + "\n")
print(f"wrote {len(json_sample)} JSON events to {json_path}")

csv_dir = fresh_dir(os.path.join(VOLUME_ROOT, "csv"))
for name, df in [("dim_broker", dim_broker), ("fact_complaint", fact_complaint),
                 ("policy_schedule_expected", policy_schedule_expected)]:
    df.to_csv(os.path.join(csv_dir, f"{name}.csv"), index=False)
print(f"wrote CSV extracts to {csv_dir}")

# COMMAND ----------

# MAGIC %md ## 11. Write to Unity Catalog, with comments on every column
# MAGIC
# MAGIC The comments are not decoration. A Genie Agent reads them, and the single biggest
# MAGIC cause of a wrong answer is an undocumented column, especially where a table has more
# MAGIC than one date.

# COMMAND ----------

TABLE_COMMENTS = {
    "dim_customer": "Synthetic customer master. One row per customer. No real person is "
                    "represented.",
    "dim_product": "Synthetic product reference. One row per product code. heritage_book "
                   "separates the Ageas and esure books.",
    "dim_broker": "Synthetic intermediary master. One row per broker_id.",
    "fact_policy": "Synthetic written policies, one row per annual policy. Grain: policy_id. "
                   "Use gross_written_premium_gbp for income reporting.",
    "fact_claim": "Synthetic claims, one row per claim. Grain: claim_id. Joins to fact_policy "
                  "on policy_id.",
    "fact_quote": "Synthetic quotes, one row per quote. Grain: quote_id. Not every quote "
                  "becomes a policy; use is_converted for conversion.",
    "fact_complaint": "Synthetic complaints, one row per complaint. Grain: complaint_id. "
                      "Includes free-text summary and call transcript.",
    "policy_schedule_expected": "Ground truth for the policy schedule PDFs in the "
                                "policy_schedules volume folder. Use it to score extraction "
                                "accuracy.",
}

COLUMN_COMMENTS = {
    "dim_customer": {
        "customer_id": "Unique customer reference. Primary key.",
        "customer_name": "Synthetic full name.",
        "postcode_area": "Outward postcode area only, for example SO or BT.",
        "postcode_town": "Town the postcode area covers.",
        "region": "UK region the customer address sits in.",
        "age_band": "Age band at the point the record was created.",
        "customer_since_date": "Date the customer first held any policy with us.",
        "tenure_years": "Whole years between customer_since_date and 2026-11-01.",
        "preferred_contact_channel": "Channel the customer asked to be contacted on.",
        "is_vulnerable_customer": "True where a vulnerability has been recorded. Used for "
                                  "Consumer Duty reporting.",
        "marketing_opt_in": "True where the customer consented to marketing contact.",
    },
    "dim_product": {
        "product_code": "Unique product reference. Primary key.",
        "line_of_business": "Motor, Home, Travel or Pet.",
        "product_name": "Customer-facing product name.",
        "brand": "Trading brand the product is sold under.",
        "heritage_book": "Which book the product came from: Ageas or esure.",
        "cover_level": "Essentials, Standard or Premier.",
    },
    "dim_broker": {
        "broker_id": "Unique broker reference. Primary key.",
        "broker_name": "Synthetic broker trading name, suffixed with the broker_id so it "
                       "is unique.",
        "broker_group": "Network or consolidator the broker belongs to, or Independent.",
        "panel_tier": "Strategic, Core or Tail. Drives commission and service levels.",
        "region": "Region the broker operates in.",
        "appointed_date": "Date the agency was appointed.",
        "commission_rate_pct": "Standard commission percentage for the panel tier.",
        "is_active": "True where the agency is still open for new business.",
    },
    "fact_policy": {
        "policy_id": "Unique policy reference. Primary key.",
        "customer_id": "Joins to dim_customer.",
        "product_code": "Joins to dim_product.",
        "broker_id": "Joins to dim_broker. Null where the policy was not sold by a broker.",
        "distribution_channel": "Broker, Direct, PCW (price comparison website) or Affinity.",
        "policy_start_date": "First day of cover. Use this date for new business reporting.",
        "policy_end_date": "Last day of cover, 364 days after the start date.",
        "policy_status": "Active, Expired, Lapsed, Cancelled, or Not Yet Incepted as at 2026-11-01. Not Yet Incepted means the policy is written but not on risk, so it counts towards written premium but not towards in-force policies.",
        "is_renewal": "True where this policy renewed an earlier policy for the same customer.",
        "previous_policy_id": "The policy this one renewed. Null for new business.",
        "net_premium_gbp": "Premium before Insurance Premium Tax, in GBP.",
        "ipt_gbp": "Insurance Premium Tax charged, at the 12 percent standard rate.",
        "gross_written_premium_gbp": "net_premium_gbp plus ipt_gbp. This is the measure to "
                                     "use for written premium and income.",
        "commission_gbp": "Commission payable to the broker. Zero for non-broker business.",
        "sum_insured_gbp": "Maximum amount payable under the policy.",
        "excess_gbp": "Amount the customer pays on each claim before we pay.",
        "payment_method": "Annual or Monthly Direct Debit.",
        "auto_renew_flag": "True where the policy is set to renew automatically.",
    },
    "fact_claim": {
        "claim_id": "Unique claim reference. Primary key.",
        "policy_id": "Joins to fact_policy.",
        "product_code": "Joins to dim_product. Copied from the policy for convenience.",
        "claim_type": "Peril claimed under, for example Escape of Water or Windscreen.",
        "incident_date": "Date the incident happened. Use for exposure and accident year.",
        "notified_date": "Date the claim was reported to us. Use for claims volumes and "
                         "reporting periods. This is the default date for 'how many claims '"
                         "'this month'.",
        "closed_date": "Date the claim was closed. Null while the claim is open.",
        "claim_status": "Settled, Open, Repudiated or Withdrawn.",
        "incurred_amount_gbp": "Total expected cost: paid plus outstanding reserve. Use for "
                               "loss ratio.",
        "paid_amount_gbp": "Amount paid out so far.",
        "outstanding_reserve_gbp": "Amount still held in reserve on open claims.",
        "handler_team": "Team handling the claim. Routed by line of business, with claims over 10000 GBP going to the large loss team and any claim above the fraud referral threshold going to Counter Fraud.",
        "fraud_score": "Model score between 0 and 1. Higher means more suspicious.",
        "fraud_referral_flag": "True where fraud_score is above the 0.55 referral threshold.",
        "days_to_notify": "notified_date minus incident_date, in days.",
        "days_to_close": "closed_date minus notified_date, in days. Null while open.",
    },
    "fact_quote": {
        "quote_id": "Unique quote reference. Primary key.",
        "quote_date": "Date the quote was produced. The only date on this table.",
        "product_code": "Joins to dim_product.",
        "distribution_channel": "Broker, Direct, PCW or Affinity.",
        "customer_age_band": "Age band of the proposer.",
        "region": "Region of the risk address.",
        "no_claims_years": "Years of no claims discount declared, 0 to 9.",
        "rated_premium_gbp": "Technical price the rating model produced.",
        "quoted_premium_gbp": "Price actually shown to the customer, after discount or "
                              "loading. Use this for price position.",
        "competitor_best_premium_gbp": "Cheapest competitor price seen for the same risk.",
        "price_position_rank": "Our rank against a notional panel of 20. 1 is cheapest.",
        "risk_score": "Model risk score between 0 and 1. Higher means more risk.",
        "rating_model_version": "Version of the rating model that priced the quote.",
        "is_converted": "True where the quote became a policy. Conversion rate is the mean "
                        "of this column.",
        "decline_reason": "Why we declined to quote. Null where we did quote.",
    },
    "fact_complaint": {
        "complaint_id": "Unique complaint reference. Primary key.",
        "policy_id": "Joins to fact_policy.",
        "product_code": "Joins to dim_product.",
        "received_date": "Date the complaint was received. Use for complaint volumes.",
        "closed_date": "Date the final response was issued. Null while open.",
        "complaint_status": "Open or Closed.",
        "complaint_channel": "How the complaint reached us.",
        "root_cause_category": "Root cause recorded by the handler.",
        "is_upheld": "True where the complaint was upheld in the customer's favour. Null "
                     "while the complaint is open.",
        "referred_to_fos": "True where the customer took the case to the Financial "
                           "Ombudsman Service.",
        "redress_paid_gbp": "Compensation paid. Zero where nothing was paid.",
        "days_to_close": "closed_date minus received_date, in days. The 8 week service "
                         "standard is 56 days.",
        "customer_sentiment": "Sentiment recorded by the handler.",
        "complaint_summary": "One line free-text summary.",
        "call_transcript": "Transcript of the complaint call, as free text.",
    },
    "policy_schedule_expected": {
        "file_name": "Name of the PDF in the policy_schedules folder of the volume.",
        "policy_id": "Policy the schedule relates to. Joins to fact_policy.",
        "policyholder_name": "Name printed on the schedule.",
        "product_code": "Product printed on the schedule.",
        "line_of_business": "Line of business printed on the schedule.",
        "cover_level": "Cover level printed on the schedule.",
        "brand": "Brand printed on the schedule.",
        "cover_start_date": "Period of cover start, as printed.",
        "cover_end_date": "Period of cover end, as printed.",
        "net_premium_gbp": "Net premium, as printed.",
        "ipt_gbp": "Insurance Premium Tax, as printed.",
        "total_premium_gbp": "Total premium payable, as printed.",
        "sum_insured_gbp": "Sum insured, as printed.",
        "excess_gbp": "Standard excess, as printed.",
    },
}

FRAMES = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "dim_broker": dim_broker,
    "fact_policy": fact_policy,
    "fact_claim": fact_claim,
    "fact_quote": fact_quote,
    "fact_complaint": fact_complaint,
    "policy_schedule_expected": policy_schedule_expected,
}

# Columns that must not be left to schema inference. is_upheld and closed_date are
# null for open records, and an inferred schema either fails or silently picks the wrong
# type, which then shows up as a wrong answer in Genie rather than as an error here.
BOOLEAN_COLUMNS = {
    "is_vulnerable_customer", "marketing_opt_in", "is_active", "is_renewal",
    "auto_renew_flag", "fraud_referral_flag", "is_converted", "is_upheld",
    "referred_to_fos",
}
INTEGER_COLUMNS = {
    "tenure_years", "no_claims_years", "price_position_rank", "days_to_notify",
    "days_to_close",
}


def spark_schema(pdf):
    """An explicit, fully nullable Spark schema for a generated frame."""
    from pyspark.sql.types import (BooleanType, DateType, DoubleType, LongType,
                                   StringType, StructField, StructType)
    fields = []
    for col in pdf.columns:
        if col.endswith("_date"):
            dtype = DateType()
        elif col in BOOLEAN_COLUMNS:
            dtype = BooleanType()
        elif col in INTEGER_COLUMNS:
            dtype = LongType()
        elif pd.api.types.is_float_dtype(pdf[col]):
            dtype = DoubleType()
        else:
            dtype = StringType()
        fields.append(StructField(col, dtype, True))
    return StructType(fields)


def to_spark_df(pdf):
    """Convert a pandas frame to Spark with explicit types and real nulls."""
    schema = spark_schema(pdf)
    coercers = []
    for field in schema.fields:
        kind = field.dataType.simpleString()
        if kind == "date":
            coercers.append(lambda v: None if pd.isna(v) else
                            (v.date() if isinstance(v, pd.Timestamp) else v))
        elif kind == "boolean":
            coercers.append(lambda v: None if pd.isna(v) else bool(v))
        elif kind == "bigint":
            coercers.append(lambda v: None if pd.isna(v) else int(v))
        elif kind == "double":
            coercers.append(lambda v: None if pd.isna(v) else float(v))
        else:
            coercers.append(lambda v: None if v is None or
                            (isinstance(v, float) and pd.isna(v)) else str(v))
    rows = [tuple(fn(val) for fn, val in zip(coercers, rec))
            for rec in pdf.itertuples(index=False, name=None)]
    return spark.createDataFrame(rows, schema=schema)  # noqa: F821


if IN_DATABRICKS:
    for name, pdf in FRAMES.items():
        fqn = f"{CATALOG}.{SCHEMA}.{name}"
        (to_spark_df(pdf)
            .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn))
        comment = TABLE_COMMENTS[name].replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {fqn} IS '{comment}'")  # noqa: F821
        for col, text in COLUMN_COMMENTS.get(name, {}).items():
            spark.sql(f"ALTER TABLE {fqn} ALTER COLUMN {col} "  # noqa: F821
                      f"COMMENT '{text.replace(chr(39), chr(39) * 2)}'")
        print(f"  wrote {fqn} ({len(pdf):,} rows, "
              f"{len(COLUMN_COMMENTS.get(name, {}))} column comments)")
else:
    out = os.path.join(VOLUME_ROOT, "tables")
    os.makedirs(out, exist_ok=True)
    for name, pdf in FRAMES.items():
        pdf.to_csv(os.path.join(out, f"{name}.csv"), index=False)
        print(f"  wrote {name}.csv ({len(pdf):,} rows)")
    missing = {n: sorted(set(df.columns) - set(COLUMN_COMMENTS.get(n, {})))
               for n, df in FRAMES.items()}
    missing = {n: c for n, c in missing.items() if c}
    print("columns with no comment:", missing if missing else "none")

# COMMAND ----------

# MAGIC %md ## 12. Metric views
# MAGIC
# MAGIC The semantic layer the afternoon builds on. Defining loss ratio and conversion once,
# MAGIC here, is what stops four teams inventing four different versions of the same number.
# MAGIC
# MAGIC Metric views need Databricks Runtime 16.4 or above to create and 17.3 or above for the
# MAGIC full feature set. If this cell fails on version, the rest of the dataset is unaffected.
# MAGIC
# MAGIC The dimension block is keyed `dimensions:` and not `fields:`. `fields:` is the newer
# MAGIC and otherwise preferred spelling, but an older runtime rejects it outright with
# MAGIC `Unrecognized field "fields"` and creates nothing. `dimensions:` is accepted by both,
# MAGIC which is what matters when we do not control the customer's runtime. Verified on
# MAGIC 10 November 2026 after it failed in the Ageas sandbox.

# COMMAND ----------

PORTFOLIO_MV = f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_portfolio_performance
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Written premium, claims cost and loss ratio by product, channel and broker.
source: {CATALOG}.{SCHEMA}.fact_policy
joins:
  - name: product
    source: {CATALOG}.{SCHEMA}.dim_product
    'on': source.product_code = product.product_code
  - name: broker
    source: {CATALOG}.{SCHEMA}.dim_broker
    'on': source.broker_id = broker.broker_id
  - name: customer
    source: {CATALOG}.{SCHEMA}.dim_customer
    'on': source.customer_id = customer.customer_id
  - name: claims
    source: |
      SELECT policy_id,
             COUNT(*) AS claim_count,
             SUM(incurred_amount_gbp) AS incurred_gbp,
             SUM(paid_amount_gbp) AS paid_gbp
      FROM {CATALOG}.{SCHEMA}.fact_claim
      GROUP BY policy_id
    'on': source.policy_id = claims.policy_id
dimensions:
  - name: Policy start month
    expr: DATE_TRUNC('MONTH', source.policy_start_date)
    comment: Month cover started. Use this for new business and written premium periods.
  - name: Line of business
    expr: product.line_of_business
    comment: Motor, Home, Travel or Pet.
  - name: Brand
    expr: product.brand
    comment: Trading brand the policy was sold under.
  - name: Heritage book
    expr: product.heritage_book
    comment: Which book the product came from, Ageas or esure.
  - name: Cover level
    expr: product.cover_level
    comment: Essentials, Standard or Premier.
  - name: Distribution channel
    expr: source.distribution_channel
    comment: Broker, Direct, PCW (price comparison website) or Affinity.
  - name: Broker name
    expr: broker.broker_name
    comment: Intermediary that sold the policy. Null for non-broker business.
  - name: Broker panel tier
    expr: broker.panel_tier
    comment: Strategic, Core or Tail.
  - name: Region
    expr: customer.region
    comment: UK region of the customer address.
  - name: Policy status
    expr: source.policy_status
    comment: Active, Expired, Lapsed or Cancelled as at 2026-11-01.
  - name: Is renewal
    expr: source.is_renewal
    comment: True where the policy renewed an earlier policy for the same customer.
measures:
  - name: Policy count
    expr: COUNT(1)
    comment: Number of policies written.
  - name: Gross written premium
    expr: SUM(source.gross_written_premium_gbp)
    comment: Net premium plus Insurance Premium Tax, in GBP.
  - name: Average premium
    expr: SUM(source.gross_written_premium_gbp) / COUNT(1)
    comment: Gross written premium divided by policy count, in GBP.
  - name: Commission paid
    expr: SUM(source.commission_gbp)
    comment: Commission payable to intermediaries, in GBP.
  - name: Claim count
    expr: SUM(COALESCE(claims.claim_count, 0))
    comment: Claims made against these policies.
  - name: Incurred claims
    expr: SUM(COALESCE(claims.incurred_gbp, 0))
    comment: Paid claims plus outstanding reserve, in GBP.
  - name: Loss ratio
    expr: SUM(COALESCE(claims.incurred_gbp, 0)) / SUM(source.net_premium_gbp)
    comment: Incurred claims divided by net premium. Above 1.0 means the book lost money.
  - name: Claim frequency
    expr: SUM(COALESCE(claims.claim_count, 0)) / COUNT(1)
    comment: Claims per policy.
  - name: Renewal rate
    expr: SUM(CASE WHEN source.is_renewal THEN 1 ELSE 0 END) / COUNT(1)
    comment: Share of policies that were renewals rather than new business.
$$
"""

QUOTE_MV = f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_quote_conversion
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Quote volume, conversion and price position by product, channel and rating model.
source: {CATALOG}.{SCHEMA}.fact_quote
joins:
  - name: product
    source: {CATALOG}.{SCHEMA}.dim_product
    'on': source.product_code = product.product_code
dimensions:
  - name: Quote month
    expr: DATE_TRUNC('MONTH', source.quote_date)
    comment: Month the quote was produced. The only date on this metric view.
  - name: Line of business
    expr: product.line_of_business
    comment: Motor, Home, Travel or Pet.
  - name: Brand
    expr: product.brand
    comment: Trading brand the quote was produced under.
  - name: Distribution channel
    expr: source.distribution_channel
    comment: Broker, Direct, PCW (price comparison website) or Affinity.
  - name: Rating model version
    expr: source.rating_model_version
    comment: Version of the rating model that priced the quote.
  - name: Age band
    expr: source.customer_age_band
    comment: Age band of the proposer.
  - name: Region
    expr: source.region
    comment: UK region of the risk address.
  - name: Price position band
    expr: >
      CASE WHEN source.price_position_rank <= 3 THEN '1 Top 3'
           WHEN source.price_position_rank <= 10 THEN '2 Rank 4 to 10'
           ELSE '3 Outside top 10' END
    comment: Where our price sat against a notional panel of 20.
measures:
  - name: Quote count
    expr: COUNT(1)
    comment: Number of quotes produced.
  - name: Converted count
    expr: SUM(CASE WHEN source.is_converted THEN 1 ELSE 0 END)
    comment: Quotes that became a policy.
  - name: Conversion rate
    expr: SUM(CASE WHEN source.is_converted THEN 1 ELSE 0 END) / COUNT(1)
    comment: Converted quotes as a share of all quotes.
  - name: Average quoted premium
    expr: AVG(source.quoted_premium_gbp)
    comment: Average price actually shown to the customer, in GBP.
  - name: Average price to market ratio
    expr: AVG(source.quoted_premium_gbp / source.competitor_best_premium_gbp)
    comment: Above 1.0 means we were more expensive than the cheapest competitor.
  - name: Average price position rank
    expr: AVG(source.price_position_rank)
    comment: Average rank against a notional panel of 20. 1 is cheapest.
$$
"""

COMPLAINT_MV = f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.mv_complaint_experience
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Complaint volume, uphold rate, redress and time to close by root cause and product.
source: {CATALOG}.{SCHEMA}.fact_complaint
joins:
  - name: product
    source: {CATALOG}.{SCHEMA}.dim_product
    'on': source.product_code = product.product_code
dimensions:
  - name: Received month
    expr: DATE_TRUNC('MONTH', source.received_date)
    comment: Month the complaint was received. Use this for complaint volumes.
  - name: Line of business
    expr: product.line_of_business
    comment: Motor, Home, Travel or Pet.
  - name: Brand
    expr: product.brand
    comment: Trading brand complained about.
  - name: Root cause
    expr: source.root_cause_category
    comment: Root cause recorded by the complaint handler.
  - name: Complaint channel
    expr: source.complaint_channel
    comment: How the complaint reached us.
  - name: Complaint status
    expr: source.complaint_status
    comment: Open or Closed.
measures:
  - name: Complaint count
    expr: COUNT(1)
    comment: Number of complaints received.
  - name: Uphold rate
    expr: SUM(CASE WHEN source.is_upheld THEN 1 ELSE 0 END)
          / SUM(CASE WHEN source.is_upheld IS NOT NULL THEN 1 ELSE 0 END)
    comment: Upheld complaints as a share of closed complaints.
  - name: Redress paid
    expr: SUM(source.redress_paid_gbp)
    comment: Compensation paid to customers, in GBP.
  - name: FOS referrals
    expr: SUM(CASE WHEN source.referred_to_fos THEN 1 ELSE 0 END)
    comment: Complaints the customer escalated to the Financial Ombudsman Service.
  - name: Average days to close
    expr: AVG(source.days_to_close)
    comment: Average days from receipt to final response. Open complaints are excluded.
  - name: Closed outside 8 weeks
    expr: SUM(CASE WHEN source.days_to_close > 56 THEN 1 ELSE 0 END)
    comment: The regulatory service standard is a final response within 8 weeks, or 56 days.
$$
"""

if IN_DATABRICKS:
    for label, ddl in [("mv_portfolio_performance", PORTFOLIO_MV),
                       ("mv_quote_conversion", QUOTE_MV),
                       ("mv_complaint_experience", COMPLAINT_MV)]:
        try:
            spark.sql(ddl)  # noqa: F821
            print(f"  created {CATALOG}.{SCHEMA}.{label}")
        except Exception as exc:  # noqa: BLE001
            print(f"  SKIPPED {label}: {exc}")
else:
    ddl_path = os.path.join(VOLUME_ROOT, "metric_views.sql")
    with open(ddl_path, "w") as fh:
        fh.write(";\n".join([PORTFOLIO_MV, QUOTE_MV, COMPLAINT_MV]))
    print(f"wrote metric view DDL to {ddl_path}")

# COMMAND ----------

# MAGIC %md ## 13. Check it looks right
# MAGIC
# MAGIC Run this before you hand the environment to anyone. If a number here looks wrong, the
# MAGIC data is wrong, and twenty people will find it before you do.

# COMMAND ----------

summary = pd.DataFrame([
    dict(check="policies", value=f"{len(fact_policy):,}"),
    dict(check="gross written premium",
         value=f"GBP {fact_policy['gross_written_premium_gbp'].sum():,.0f}"),
    dict(check="active policies",
         value=f"{(fact_policy['policy_status'] == 'Active').sum():,}"),
    dict(check="claims", value=f"{len(fact_claim):,}"),
    dict(check="incurred claims",
         value=f"GBP {fact_claim['incurred_amount_gbp'].sum():,.0f}"),
    dict(check="loss ratio on net premium",
         value=f"{fact_claim['incurred_amount_gbp'].sum() / fact_policy['net_premium_gbp'].sum():.1%}"),
    dict(check="open claims", value=f"{(fact_claim['claim_status'] == 'Open').sum():,}"),
    dict(check="quotes", value=f"{len(fact_quote):,}"),
    dict(check="quote conversion", value=f"{fact_quote['is_converted'].mean():.1%}"),
    dict(check="complaints", value=f"{len(fact_complaint):,}"),
    dict(check="complaint uphold rate", value=f"{fact_complaint['is_upheld'].mean():.1%}"),
    dict(check="FOS referrals", value=f"{int(fact_complaint['referred_to_fos'].sum()):,}"),
    dict(check="policy schedule PDFs", value=f"{len(policy_schedule_expected):,}"),
    dict(check="orphan claims (should be 0)",
         value=f"{(~fact_claim['policy_id'].isin(fact_policy['policy_id'])).sum()}"),
    dict(check="orphan complaints (should be 0)",
         value=f"{(~fact_complaint['policy_id'].isin(fact_policy['policy_id'])).sum()}"),
    dict(check="duplicate policy ids (should be 0)",
         value=f"{fact_policy['policy_id'].duplicated().sum()}"),
    dict(check="duplicate broker ids (should be 0)",
         value=f"{dim_broker['broker_id'].duplicated().sum()}"),
])

if IN_DATABRICKS:
    display(spark.createDataFrame(summary))  # noqa: F821
else:
    print(summary.to_string(index=False))
