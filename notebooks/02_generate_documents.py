# Databricks notebook source
# MAGIC %md
# MAGIC # Ageas hackathon - document generator
# MAGIC
# MAGIC Run this **after** `01_generate_hackathon_data.py`. It reads the tables that notebook
# MAGIC created, so every document refers to a real policy, claim or broker and anything you
# MAGIC extract can be joined straight back to the structured data.
# MAGIC
# MAGIC ## Why this notebook exists
# MAGIC
# MAGIC Notebook 01 produces clean, machine-generated policy schedules. Those are close to the
# MAGIC easy case for `ai_parse_document`: it will read them almost perfectly, which demonstrates
# MAGIC very little. The documents here are the ones the three teams actually handle, in the
# MAGIC shapes they actually arrive in.
# MAGIC
# MAGIC | Team | Documents | What makes it a real extraction problem |
# MAGIC | --- | --- | --- |
# MAGIC | Claims | FNOL forms, engineer estimates | The cause of loss is in the narrative, not in a field. Two arrive as forwarded email with no form at all. |
# MAGIC | Brokerage | Risk submissions, statements of account | A five year claims experience table inside the document has to become five rows |
# MAGIC | Pricing | Underwriting guide, competitor panel reports | Rating rules written as prose and tables have to become something you can query |
# MAGIC
# MAGIC ## Scoring, not admiring
# MAGIC
# MAGIC Every document type has a matching `_expected` table holding the true values. That is the
# MAGIC difference between a demo and something you would put near a process: you can measure how
# MAGIC often the extraction is right, and watch it get worse on the documents that carry
# MAGIC simulated scan noise.
# MAGIC
# MAGIC Everything is synthetic. No real customer, broker, claim or complaint appears anywhere.

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
    CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
    SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
    VOLUME = dbutils.widgets.get("volume")  # noqa: F821
    VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
else:
    import os
    CATALOG = SCHEMA = VOLUME = "local"
    VOLUME_ROOT = os.environ.get("OUT", "/tmp/ageas_hackathon")

SEED = 20261116

# How many of each document to produce. Small on purpose: the point is to show the approach,
# not to build a corpus.
COUNTS = dict(fnol_motor=5, fnol_home=3, estimates=3, submissions=5, statements=2, pcw=3)

# Share of documents that get simulated scan noise. The expected values stay correct, so the
# scoring query shows the accuracy drop these cause.
# Which documents get scan noise, by position within their type. Fixed rather than random
# so that each team reliably gets at least one noisy document to compare against a clean one.
NOISY_POSITIONS = {"fnol": {1, 5}, "submission": {0, 3}, "estimate": {2}, "panel": {1}}

# Character error rate applied to those documents. At 0.05 a modern model shrugs this off,
# which is itself worth demonstrating. Turn it up to 0.15 or 0.25 and re-run if you want to
# show a team where extraction actually starts to break.
SCAN_NOISE_RATE = 0.05

print(f"writing documents under {VOLUME_ROOT}")

# COMMAND ----------

# MAGIC %md ## 2. Read the structured data
# MAGIC
# MAGIC Locally this reads the CSV files notebook 01 wrote. On Databricks it reads the tables.

# COMMAND ----------

import datetime as dt
import os
import random
import pandas as pd

RNG = random.Random(SEED)


def load(name):
    if IN_DATABRICKS:
        return spark.table(f"{CATALOG}.{SCHEMA}.{name}").toPandas()  # noqa: F821
    return pd.read_csv(os.path.join(VOLUME_ROOT, "tables", f"{name}.csv"))


dim_customer = load("dim_customer")
dim_product = load("dim_product")
dim_broker = load("dim_broker")
fact_policy = load("fact_policy")
fact_claim = load("fact_claim")
fact_quote = load("fact_quote")

prod_by_code = dim_product.set_index("product_code").to_dict("index")
cust_by_id = dim_customer.set_index("customer_id").to_dict("index")
pol_by_id = fact_policy.set_index("policy_id").to_dict("index")

print(f"loaded {len(fact_policy):,} policies, {len(fact_claim):,} claims, "
      f"{len(dim_broker):,} brokers")

# COMMAND ----------

# MAGIC %md ## 3. PDF writer
# MAGIC
# MAGIC Same dependency-free writer as notebook 01, extended to place several cells on one line
# MAGIC so that forms and tables can be laid out. No `%pip install` is needed.

# COMMAND ----------

PAGE_W, PAGE_H, MARGIN, LEADING, FONT_SIZE = 595, 842, 52, 14, 9.5


def _esc(text):
    return str(text).replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _stream_for(lines):
    out = ["BT", f"1 0 0 1 {MARGIN} {PAGE_H - MARGIN} Tm", f"{LEADING} TL"]
    for ln in lines:
        if ln.get("gap"):
            out.append(f"0 -{ln['gap']} Td")
        cells = ln.get("cells")
        if cells:
            # Several pieces of text on one baseline, each at its own x offset.
            for indent, text, bold in cells:
                out.append(f"{'/F2' if bold else '/F1'} {ln.get('size', FONT_SIZE)} Tf")
                if indent:
                    out.append(f"{indent} 0 Td")
                out.append(f"({_esc(text)}) Tj")
                if indent:
                    out.append(f"-{indent} 0 Td")
        else:
            out.append(f"{'/F2' if ln.get('bold') else '/F1'} "
                       f"{ln.get('size', FONT_SIZE)} Tf")
            if ln.get("indent"):
                out.append(f"{ln['indent']} 0 Td")
            out.append(f"({_esc(ln.get('text', ''))}) Tj")
            if ln.get("indent"):
                out.append(f"-{ln['indent']} 0 Td")
        out.append("T*")
    out.append("ET")
    return "\n".join(out)


def write_pdf(path, lines, title="Document"):
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
            f"/Producer (Ageas hackathon synthetic document generator) >> >>\n"
            f"startxref\n{xref_at}\n%%EOF\n").encode()

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(bytes(buf))
    return path


# ---------------------------------------------------------------- scan noise
OCR_SWAPS = [("0", "O"), ("O", "0"), ("1", "l"), ("l", "1"), ("5", "S"), ("S", "5"),
             ("8", "B"), ("B", "8"), ("rn", "m"), ("cl", "d"), ("I", "1")]


def scanify(text, rate=SCAN_NOISE_RATE):
    """Simulate the character errors a scan and OCR pass introduces.

    The expected values are left untouched, so accuracy on these documents can be compared
    against the clean ones.
    """
    out = list(str(text))
    i = 0
    while i < len(out):
        if RNG.random() < rate:
            for a, b in OCR_SWAPS:
                if "".join(out[i:i + len(a)]) == a:
                    out[i:i + len(a)] = list(b)
                    i += len(b)
                    break
            else:
                if RNG.random() < 0.3:
                    out.insert(i, " ")
                    i += 1
        i += 1
    return "".join(out)


def maybe_scanify(lines, noisy):
    if not noisy:
        return lines
    out = []
    for ln in lines:
        ln = dict(ln)
        if ln.get("cells"):
            ln["cells"] = [(x, scanify(t), b) for x, t, b in ln["cells"]]
        elif ln.get("text"):
            ln["text"] = scanify(ln["text"])
        out.append(ln)
    return out


def kv(label, value, label_w=135):
    """A labelled form field."""
    return {"cells": [(0, label, True), (label_w, str(value), False)]}


def row(values, xs, bold=False, size=8.5):
    return {"cells": [(x, str(v), bold) for x, v in zip(xs, values)], "size": size}


print("PDF writer ready")

# COMMAND ----------

# MAGIC %md ## 4. Claims - FNOL forms
# MAGIC
# MAGIC First Notification of Loss. Three things make these a real extraction problem rather
# MAGIC than a form-filling exercise:
# MAGIC
# MAGIC 1. **The cause of loss is only in the narrative.** There is no cause field. Working out
# MAGIC    that "the kitchen ceiling came down after the bathroom pipe gave way upstairs" is an
# MAGIC    escape of water is a classification job, not an extraction job.
# MAGIC 2. **Two of them arrive as a forwarded email chain** with no form structure at all,
# MAGIC    because in real life that is how a good proportion of them turn up.
# MAGIC 3. **Dates appear in several formats** across documents, and some fields are simply
# MAGIC    blank, as they are on a real notification taken over the phone.

# COMMAND ----------

# Narrative, the cause the handler should land on, whether a third party is involved, and
# whether an injury is mentioned. The cause never appears as a word in the narrative.
MOTOR_NARRATIVES = [
    ("I was queuing at the lights on the A33 when the van behind failed to stop and went "
     "into the back of me. His bumper and my tailgate are both damaged. He admitted he was "
     "looking at his phone. We exchanged details at the roadside.",
     "Third Party Property", True, False),
    ("Came out of the supermarket and found the driver door panel scraped along its whole "
     "length and the mirror hanging off. Nobody had left a note and there was no other "
     "vehicle there by then.",
     "Accidental Damage", False, False),
    ("Parked it on the drive on Friday night and on Saturday morning it had gone. The "
     "steering lock was still on the ground. Reported it to the police the same morning and "
     "they gave me a crime number.",
     "Theft", False, False),
    ("A stone came up off a lorry on the motorway and has put a crack right across the "
     "driver side of the glass. It has spread since and I cannot see properly through it.",
     "Windscreen", False, False),
    ("I pulled out of the junction and did not see the motorcyclist. He came off and was "
     "taken to hospital with a broken wrist. The ambulance and police both attended. I am "
     "very shaken about it.",
     "Personal Injury", True, True),
    ("There was smoke from under the bonnet on the dual carriageway and by the time I had "
     "pulled onto the hard shoulder there were flames. The fire service put it out. The "
     "front of the car is completely gone.",
     "Fire", False, False),
]

HOME_NARRATIVES = [
    ("We came back from a week away to find the kitchen ceiling on the floor and water "
     "still running. A joint on the pipe to the bathroom basin upstairs had given way. The "
     "plumber has capped it off but the units and the flooring are ruined.",
     "Escape of Water", False, False),
    ("During the high winds on Saturday night a large section of tiles came off the roof at "
     "the back and the fence went over onto the neighbour's shed. There is water coming "
     "into the back bedroom now.",
     "Storm", True, False),
    ("Someone has forced the patio door at the rear while we were out. They have taken the "
     "television, my wife's jewellery and a laptop. The police have been and taken "
     "fingerprints.",
     "Theft", False, False),
    ("My son knocked a full tin of paint off the stepladder and it has gone all over the "
     "hall carpet and up the wall. The carpet was fitted in the spring.",
     "Accidental Damage", False, False),
    ("I have noticed the crack above the lounge window has got much wider over the summer "
     "and the door now sticks. There is a large willow in next door's garden. A surveyor "
     "has looked at it and mentioned the clay soil.",
     "Subsidence", False, False),
]

CAR_MAKES = [("Ford", "Focus"), ("Vauxhall", "Corsa"), ("Volkswagen", "Golf"),
             ("Nissan", "Qashqai"), ("Kia", "Sportage"), ("BMW", "1 Series"),
             ("Toyota", "Yaris"), ("Audi", "A3")]
POLICE_FORCES = ["Hampshire", "Dorset", "Avon and Somerset", "Thames Valley",
                 "West Midlands", "Greater Manchester", "Police Scotland"]
HANDLERS = ["Priya Natarajan", "Dan Whitfield", "Shona Baird", "Mo Rahimi",
            "Katie Lomax", "Bev Oyelaran"]

DATE_FORMATS = ["%d/%m/%Y", "%d %B %Y", "%Y-%m-%d", "%d-%b-%y"]


def fmt_date(d, style):
    return d.strftime(DATE_FORMATS[style % len(DATE_FORMATS)])


def plate():
    letters = "ABCDEFGHJKLMNOPRSTUVWXY"
    return (f"{RNG.choice(letters)}{RNG.choice(letters)}{RNG.randint(10, 74):02d} "
            f"{RNG.choice(letters)}{RNG.choice(letters)}{RNG.choice(letters)}")


def fnol_form_lines(d):
    """The structured form layout."""
    lines = [
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL CLAIM", "bold": True, "size": 8},
        {"text": f"{d['brand'].upper()}", "bold": True, "size": 15, "gap": 8},
        {"text": "FIRST NOTIFICATION OF LOSS", "bold": True, "size": 11},
        {"text": f"{d['line_of_business']} claim - telephone notification",
         "size": 9, "gap": 2},
        {"text": "Notification", "bold": True, "size": 10, "gap": 12},
        kv("FNOL reference", d["fnol_reference"]),
        kv("Claim number", d["claim_id"]),
        kv("Policy number", d["policy_id"]),
        kv("Date notified", d["notified_date_text"]),
        kv("Taken by", d["taken_by"]),
        {"text": "Policyholder", "bold": True, "size": 10, "gap": 12},
        kv("Name", d["policyholder_name"]),
        kv("Contact number", d["contact_number"]),
        kv("Address area", d["postcode_area"]),
        {"text": "Incident", "bold": True, "size": 10, "gap": 12},
        kv("Date of incident", d["incident_date_text"]),
        kv("Time of incident", d["incident_time"]),
        kv("Location", d["incident_location"]),
    ]
    if d["line_of_business"] == "Motor":
        lines += [
            {"text": "Vehicle", "bold": True, "size": 10, "gap": 12},
            kv("Registration", d["vehicle_registration"]),
            kv("Make and model", f"{d['vehicle_make']} {d['vehicle_model']}"),
            kv("Driveable", "Yes" if d["vehicle_driveable"] else "No"),
        ]
    lines += [
        {"text": "Description of incident, in the policyholder's own words",
         "bold": True, "size": 10, "gap": 12},
    ]
    lines += [{"text": seg} for seg in wrap(d["narrative"], 96)]
    lines += [
        {"text": "Other parties", "bold": True, "size": 10, "gap": 12},
        kv("Third party involved", "Yes" if d["third_party_involved"] else "No"),
        kv("Third party details",
           d["third_party_details"] if d["third_party_details"] else "Not supplied"),
        kv("Injuries reported", "Yes" if d["injury_reported"] else "No"),
        kv("Police reference",
           d["police_reference"] if d["police_reference"] else "Not applicable"),
        {"text": "Handler assessment", "bold": True, "size": 10, "gap": 12},
        kv("Estimated reserve", f"GBP {d['estimated_reserve_gbp']:,.2f}"),
        kv("Handling team", d["handler_team"]),
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL CLAIM", "bold": True, "size": 8,
         "gap": 14},
    ]
    return lines


def fnol_email_lines(d):
    """The same notification as a forwarded email chain. No form structure at all."""
    lines = [
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL CLAIM", "bold": True, "size": 8,
         "gap": 4},
        kv("From", f"{d['policyholder_name'].lower().replace(' ', '.')}@examplemail.co.uk",
           label_w=58),
        kv("Sent", f"{d['notified_date_text']} {d['email_sent_time']}", label_w=58),
        kv("To", f"claims@{d['brand'].lower().replace(chr(39), '').replace(' ', '')}.co.uk",
           label_w=58),
        kv("Subject", f"FW: Claim on policy {d['policy_id']} - please call me",
           label_w=58),
        {"text": "", "gap": 6},
        {"text": "Hello,"},
        {"text": ""},
    ]
    lines += [{"text": seg} for seg in wrap(d["narrative"], 96)]
    lines += [{"text": ""}]
    extras = [f"It happened on {d['incident_date_text']} at around {d['incident_time']}, "
              f"at {d['incident_location']}."]
    if d["line_of_business"] == "Motor":
        extras.append(f"The car is the {d['vehicle_make']} {d['vehicle_model']}, "
                      f"registration {d['vehicle_registration']}.")
    if d["police_reference"]:
        extras.append(f"The police reference is {d['police_reference']}.")
    if d["third_party_details"]:
        extras.append(f"The other party was {d['third_party_details']}.")
    extras.append("I rang on Tuesday and was told to put it in writing. Please call me on "
                  f"{d['contact_number']}.")
    for seg in extras:
        lines += [{"text": s} for s in wrap(seg, 96)]
    lines += [
        {"text": ""},
        {"text": "Regards,"},
        {"text": d["policyholder_name"]},
        {"text": "", "gap": 8},
        {"text": "------ Forwarded message ------", "size": 8.5},
        {"text": f"> Thank you for calling {d['brand']}. Your reference is "
                 f"{d['fnol_reference']}.", "size": 8.5},
        {"text": f"> A handler from {d['handler_team']} will be in touch within two working "
                 f"days.", "size": 8.5},
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL CLAIM", "bold": True, "size": 8,
         "gap": 12},
    ]
    return lines


def wrap(text, width):
    """Wrap text to a character width, because the PDF writer does not reflow."""
    words, out, cur = str(text).split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > width:
            out.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        out.append(cur)
    return out or [""]


fnol_rows, document_index = [], []
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


fnol_dir = fresh_dir(os.path.join(VOLUME_ROOT, "documents", "fnol"))

motor_claims = fact_claim[fact_claim["product_code"].str.startswith("MOT")]
home_claims = fact_claim[fact_claim["product_code"].str.startswith("HOM")]

plan = ([("Motor", MOTOR_NARRATIVES)] * COUNTS["fnol_motor"]
        + [("Home", HOME_NARRATIVES)] * COUNTS["fnol_home"])

for idx, (lob, narratives) in enumerate(plan):
    pool = motor_claims if lob == "Motor" else home_claims
    claim = pool.iloc[RNG.randrange(len(pool))]
    policy = pol_by_id[claim["policy_id"]]
    product = prod_by_code[claim["product_code"]]
    customer = cust_by_id[policy["customer_id"]]

    narrative, cause, third_party, injury = narratives[idx % len(narratives)]
    incident = pd.to_datetime(claim["incident_date"]).date()
    notified = pd.to_datetime(claim["notified_date"]).date()
    style = idx  # rotate the date format between documents

    layout = "email" if idx in (2, 6) else "form"
    noisy = idx in NOISY_POSITIONS["fnol"]

    d = dict(
        fnol_reference=f"FNOL-{RNG.randint(100000, 999999)}",
        claim_id=claim["claim_id"],
        policy_id=claim["policy_id"],
        brand=product["brand"],
        line_of_business=product["line_of_business"],
        policyholder_name=customer["customer_name"],
        postcode_area=f"{customer['postcode_area']}, {customer['postcode_town']}",
        contact_number=f"07{RNG.randint(100, 999)} {RNG.randint(100000, 999999)}",
        notified_date_text=fmt_date(notified, style),
        incident_date_text=fmt_date(incident, style),
        incident_time=f"{RNG.randint(6, 22):02d}:{RNG.choice(['00', '15', '30', '45'])}",
        email_sent_time=f"{RNG.randint(8, 19):02d}:{RNG.randint(10, 59):02d}",
        incident_location=RNG.choice([
            "A33 Winchester Road, northbound", "Supermarket car park, Eastleigh",
            "Home address, on the driveway", "M27 junction 5 slip road",
            "Residential street outside the property", "Town centre multi-storey, level 3",
        ]) if lob == "Motor" else "The insured property",
        taken_by=RNG.choice(HANDLERS),
        narrative=narrative,
        third_party_involved=third_party,
        injury_reported=injury,
        handler_team=claim["handler_team"],
        estimated_reserve_gbp=float(claim["incurred_amount_gbp"]) or 750.0,
        cause_of_loss=cause,
    )
    if lob == "Motor":
        make, model = RNG.choice(CAR_MAKES)
        d.update(vehicle_registration=plate(), vehicle_make=make, vehicle_model=model,
                 vehicle_driveable=cause not in ("Fire", "Theft"))
    else:
        d.update(vehicle_registration=None, vehicle_make=None, vehicle_model=None,
                 vehicle_driveable=None)
    d["police_reference"] = (f"{RNG.choice(POLICE_FORCES)} {RNG.randint(10, 99)}"
                             f"/{RNG.randint(10000, 99999)}"
                             if cause in ("Theft", "Personal Injury") else None)
    d["third_party_details"] = (
        f"{RNG.choice(['Mr', 'Mrs', 'Ms'])} {RNG.choice(['T Hollis', 'A Beckworth', 'J Nkemelu', 'R Oldfield'])}, "
        f"registration {plate()}" if third_party and lob == "Motor"
        else ("Neighbouring property owner" if third_party else None))

    builder = fnol_email_lines if layout == "email" else fnol_form_lines
    fname = f"{d['fnol_reference']}_{d['claim_id']}.pdf"
    path = os.path.join(fnol_dir, fname)
    write_pdf(path, maybe_scanify(builder(d), noisy), title=f"FNOL {d['fnol_reference']}")

    fnol_rows.append(dict(
        file_name=fname, fnol_reference=d["fnol_reference"], claim_id=d["claim_id"],
        policy_id=d["policy_id"], line_of_business=lob,
        policyholder_name=d["policyholder_name"],
        incident_date=incident, notified_date=notified,
        incident_time=d["incident_time"], incident_location=d["incident_location"],
        vehicle_registration=d["vehicle_registration"],
        vehicle_make=d["vehicle_make"], vehicle_model=d["vehicle_model"],
        cause_of_loss=cause, third_party_involved=third_party,
        injury_reported=injury, police_reference=d["police_reference"],
        estimated_reserve_gbp=d["estimated_reserve_gbp"],
        handler_team=d["handler_team"], document_layout=layout, has_scan_noise=noisy,
    ))
    document_index.append(dict(
        file_name=fname, folder="documents/fnol", document_type="FNOL",
        team="Claims", document_layout=layout, has_scan_noise=noisy,
        expected_table="doc_fnol_expected"))

doc_fnol_expected = pd.DataFrame(fnol_rows)
print(f"wrote {len(doc_fnol_expected)} FNOL documents "
      f"({(doc_fnol_expected.document_layout == 'email').sum()} as email, "
      f"{doc_fnol_expected.has_scan_noise.sum()} with scan noise)")

# COMMAND ----------

# MAGIC %md ## 5. Claims - repairer estimates
# MAGIC
# MAGIC A table of parts and labour with a total at the bottom. Two extraction jobs in one: pull
# MAGIC out the line items as rows, and check whether the arithmetic on the document is right.
# MAGIC One of these deliberately does not add up, which is exactly the thing you would want a
# MAGIC machine to catch before the invoice is paid.

# COMMAND ----------

PARTS = {
    "Motor": [("Front bumper assembly", 285.00), ("Headlamp unit, offside", 196.50),
              ("Wing panel, nearside front", 164.00), ("Tailgate skin", 412.75),
              ("Door mirror, driver side", 88.40), ("Windscreen, heated", 320.00),
              ("Radiator", 174.20), ("Bonnet", 298.00), ("Alloy wheel", 142.60)],
    "Home": [("Plasterboard and skim, per m2", 34.50), ("Kitchen base unit", 118.00),
             ("Laminate flooring, per m2", 26.40), ("Roof tiles, per 100", 96.00),
             ("Felt and battens", 143.25), ("Internal door", 72.00),
             ("Carpet, per m2", 22.75), ("Decoration, per room", 265.00)],
}
LABOUR_RATES = {"Motor": 62.50, "Home": 48.00}
GARAGES = ["Westgate Accident Repair Centre", "Marchwood Motor Bodies",
           "Pennine Vehicle Repairs", "Hollybank Building Services",
           "Severnside Restoration"]

estimate_rows, estimate_line_rows = [], []
est_dir = fresh_dir(os.path.join(VOLUME_ROOT, "documents", "estimates"))

for i in range(COUNTS["estimates"]):
    src = doc_fnol_expected.iloc[i]
    lob = src["line_of_business"]
    supplier = RNG.choice(GARAGES)
    est_ref = f"EST-{RNG.randint(10000, 99999)}"
    rate = LABOUR_RATES[lob]

    chosen = RNG.sample(PARTS[lob], k=RNG.randint(3, 5))
    lines_detail, parts_total = [], 0.0
    for desc, unit in chosen:
        qty = RNG.randint(1, 3)
        amount = round(unit * qty, 2)
        parts_total += amount
        lines_detail.append(("Parts", desc, qty, unit, amount))
    labour_hours = round(RNG.uniform(2.5, 18.0), 1)
    labour_amount = round(labour_hours * rate, 2)
    lines_detail.append(("Labour", f"{lob} repair labour", labour_hours, rate,
                         labour_amount))

    net = round(parts_total + labour_amount, 2)
    vat = round(net * 0.20, 2)
    stated_total = round(net + vat, 2)

    # One estimate per run has a deliberate arithmetic error on the stated total.
    arithmetic_error = (i == 1)
    if arithmetic_error:
        stated_total = round(stated_total + RNG.choice([120.0, 85.5, 240.0]), 2)

    xs = [0, 150, 330, 385, 460]
    body = [
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL ESTIMATE", "bold": True, "size": 8},
        {"text": supplier.upper(), "bold": True, "size": 14, "gap": 8},
        {"text": "Estimate for repair", "size": 10, "gap": 2},
        kv("Estimate reference", est_ref),
        kv("Claim number", src["claim_id"]),
        kv("Policy number", src["policy_id"]),
        kv("Date of estimate", fmt_date(
            pd.to_datetime(src["notified_date"]).date() + dt.timedelta(days=RNG.randint(2, 9)),
            i)),
        kv("Insured", src["policyholder_name"]),
    ]
    if lob == "Motor":
        body.append(kv("Vehicle", f"{src['vehicle_make']} {src['vehicle_model']} "
                                  f"({src['vehicle_registration']})"))
    body += [
        {"text": "", "gap": 8},
        row(["Type", "Description", "Qty", "Unit GBP", "Amount GBP"], xs, bold=True),
        {"text": "-" * 108, "size": 8},
    ]
    for kind, desc, qty, unit, amount in lines_detail:
        body.append(row([kind, desc, f"{qty:g}", f"{unit:,.2f}", f"{amount:,.2f}"], xs))
        estimate_line_rows.append(dict(
            file_name=f"{est_ref}.pdf", estimate_reference=est_ref,
            claim_id=src["claim_id"], line_type=kind, description=desc,
            quantity=float(qty), unit_price_gbp=float(unit), amount_gbp=float(amount)))
    body += [
        {"text": "-" * 108, "size": 8},
        row(["", "Net total", "", "", f"{net:,.2f}"], xs, bold=True),
        row(["", "VAT at 20 percent", "", "", f"{vat:,.2f}"], xs),
        row(["", "Total payable", "", "", f"{stated_total:,.2f}"], xs, bold=True),
        {"text": "", "gap": 8},
        {"text": "Authorisation is required before work commences. Courtesy vehicle is "
                 "subject to availability.", "size": 8.5},
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL ESTIMATE", "bold": True, "size": 8,
         "gap": 12},
    ]

    fname = f"{est_ref}.pdf"
    est_noisy = i in NOISY_POSITIONS["estimate"]
    write_pdf(os.path.join(est_dir, fname), maybe_scanify(body, est_noisy),
              title=f"Estimate {est_ref}")
    estimate_rows.append(dict(
        file_name=fname, estimate_reference=est_ref, claim_id=src["claim_id"],
        policy_id=src["policy_id"], supplier_name=supplier, line_count=len(lines_detail),
        parts_total_gbp=round(parts_total, 2), labour_hours=labour_hours,
        labour_rate_gbp=rate, labour_total_gbp=labour_amount,
        net_total_gbp=net, vat_gbp=vat, stated_total_gbp=stated_total,
        correct_total_gbp=round(net + vat, 2), total_is_correct=not arithmetic_error))
    document_index.append(dict(
        file_name=fname, folder="documents/estimates", document_type="Repairer estimate",
        team="Claims", document_layout="table", has_scan_noise=est_noisy,
        expected_table="doc_estimate_expected"))

doc_estimate_expected = pd.DataFrame(estimate_rows)
doc_estimate_line_expected = pd.DataFrame(estimate_line_rows)
print(f"wrote {len(doc_estimate_expected)} estimates, "
      f"{len(doc_estimate_line_expected)} line items, "
      f"{(~doc_estimate_expected.total_is_correct).sum()} with an arithmetic error")

# COMMAND ----------

# MAGIC %md ## 6. Brokerage - risk submissions
# MAGIC
# MAGIC What a broker sends in when they want a quote. The interesting part is the five year
# MAGIC claims experience table in the middle: one document has to become five rows, joined back
# MAGIC to a broker that exists in `dim_broker`. Getting a nested table out of a document is the
# MAGIC thing people usually assume is impossible.

# COMMAND ----------

TRADES = ["Retail - convenience store", "Light engineering", "Road haulage",
          "Office and professional services", "Residential property owner",
          "Care home operator", "Food manufacturing"]
COVERS = ["Commercial Combined", "Fleet", "Property Owners", "Employers Liability",
          "Public and Products Liability", "Business Interruption"]

submission_rows, submission_history_rows = [], []
sub_dir = fresh_dir(os.path.join(VOLUME_ROOT, "documents", "submissions"))
active_brokers = dim_broker[dim_broker["is_active"]]

for i in range(COUNTS["submissions"]):
    broker = active_brokers.iloc[RNG.randrange(len(active_brokers))]
    sub_ref = f"SUB-{RNG.randint(100000, 999999)}"
    insured = (f"{RNG.choice(['Marchfield', 'Bramley', 'Oakhurst', 'Rivermead', 'Tollgate'])} "
               f"{RNG.choice(['Holdings', 'Group', 'Services', 'Trading', 'Estates'])} Limited")
    trade = RNG.choice(TRADES)
    covers = RNG.sample(COVERS, k=RNG.randint(2, 3))
    sum_insured = float(RNG.randrange(500_000, 12_000_000, 50_000))
    target_premium = round(sum_insured * RNG.uniform(0.0022, 0.0095), 2)
    renewal = dt.date(2026, 12, 1) + dt.timedelta(days=RNG.randint(0, 120))
    noisy = i in NOISY_POSITIONS["submission"]

    # Five years of claims experience, which becomes five rows.
    history = []
    for back in range(5, 0, -1):
        year = 2026 - back
        n = RNG.choices([0, 1, 2, 3, 5], weights=[42, 28, 16, 9, 5], k=1)[0]
        incurred = round(sum(abs(RNG.gauss(4200, 3800)) + 150 for _ in range(n)), 2)
        history.append((year, n, incurred))

    xs = [0, 90, 190, 300]
    body = [
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL SUBMISSION", "bold": True,
         "size": 8},
        {"text": broker["broker_name"].upper(), "bold": True, "size": 13, "gap": 8},
        {"text": "Risk submission and market presentation", "size": 10, "gap": 2},
        kv("Submission reference", sub_ref),
        kv("Broker reference", broker["broker_id"]),
        kv("Date of submission", fmt_date(
            renewal - dt.timedelta(days=RNG.randint(30, 75)), i)),
        kv("Renewal date", fmt_date(renewal, i)),
        {"text": "The risk", "bold": True, "size": 10, "gap": 12},
        kv("Proposed insured", insured),
        kv("Trade or occupation", trade),
        kv("Years established", str(RNG.randint(3, 48))),
        kv("Employees", str(RNG.randint(4, 320))),
        kv("Annual turnover", f"GBP {RNG.randrange(800_000, 40_000_000, 50_000):,}"),
        {"text": "Cover required", "bold": True, "size": 10, "gap": 12},
    ]
    body += [{"text": f"- {c}"} for c in covers]
    body += [
        kv("Total sum insured", f"GBP {sum_insured:,.0f}"),
        kv("Target premium", f"GBP {target_premium:,.2f}"),
        kv("Commission required",
           f"{broker['commission_rate_pct']:.1f} percent of premium"),
        {"text": "Claims experience, last five years", "bold": True, "size": 10,
         "gap": 12},
        row(["Year", "Claims", "Incurred GBP", "Notes"], xs, bold=True),
        {"text": "-" * 104, "size": 8},
    ]
    for year, n, incurred in history:
        note = "Nil" if n == 0 else ("Closed" if year <= 2023 else "One open")
        body.append(row([str(year), str(n), f"{incurred:,.2f}", note], xs))
        submission_history_rows.append(dict(
            file_name=f"{sub_ref}.pdf", submission_reference=sub_ref,
            broker_id=broker["broker_id"], experience_year=year,
            claim_count=n, incurred_gbp=incurred))
    body += [
        {"text": "-" * 104, "size": 8},
        row(["Total", str(sum(h[1] for h in history)),
             f"{sum(h[2] for h in history):,.2f}", ""], xs, bold=True),
        {"text": "Broker declaration", "bold": True, "size": 10, "gap": 12},
        {"text": "We confirm the information above has been supplied by the proposer and "
                 "is presented in good faith."},
        {"text": f"Contact: {RNG.choice(HANDLERS)}, {broker['broker_name']}"},
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL SUBMISSION", "bold": True,
         "size": 8, "gap": 12},
    ]

    fname = f"{sub_ref}.pdf"
    write_pdf(os.path.join(sub_dir, fname), maybe_scanify(body, noisy),
              title=f"Submission {sub_ref}")
    submission_rows.append(dict(
        file_name=fname, submission_reference=sub_ref, broker_id=broker["broker_id"],
        broker_name=broker["broker_name"], panel_tier=broker["panel_tier"],
        proposed_insured=insured, trade_or_occupation=trade,
        covers_requested="; ".join(covers), sum_insured_gbp=sum_insured,
        target_premium_gbp=target_premium,
        commission_rate_pct=float(broker["commission_rate_pct"]),
        renewal_date=renewal,
        total_claims_last_5y=sum(h[1] for h in history),
        total_incurred_last_5y_gbp=round(sum(h[2] for h in history), 2),
        has_scan_noise=noisy))
    document_index.append(dict(
        file_name=fname, folder="documents/submissions",
        document_type="Broker risk submission", team="Brokerage",
        document_layout="form and nested table", has_scan_noise=noisy,
        expected_table="doc_submission_expected"))

doc_submission_expected = pd.DataFrame(submission_rows)
doc_submission_history_expected = pd.DataFrame(submission_history_rows)
print(f"wrote {len(doc_submission_expected)} submissions with "
      f"{len(doc_submission_history_expected)} claims experience rows")

# COMMAND ----------

# MAGIC %md ## 7. Brokerage - statements of account
# MAGIC
# MAGIC The monthly commission reconciliation. Every policy line in the statement exists in
# MAGIC `fact_policy`, so once the lines are extracted they can be checked against the ledger.
# MAGIC Two lines per statement are deliberately wrong, which is the whole point: finding the
# MAGIC two lines that disagree is the job, and nobody wants to do it by eye.

# COMMAND ----------

statement_rows, statement_line_rows = [], []
stmt_dir = fresh_dir(os.path.join(VOLUME_ROOT, "documents", "statements"))
broker_policies = fact_policy[fact_policy["broker_id"].notna()]

for i in range(COUNTS["statements"]):
    broker_id = broker_policies["broker_id"].iloc[RNG.randrange(len(broker_policies))]
    broker = dim_broker[dim_broker["broker_id"] == broker_id].iloc[0]
    theirs = broker_policies[broker_policies["broker_id"] == broker_id]
    theirs = theirs.head(12) if len(theirs) >= 12 else theirs
    stmt_ref = f"STMT-2026-{10 + i:02d}-{broker_id}"

    xs = [0, 105, 215, 300, 390]
    body = [
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL STATEMENT", "bold": True,
         "size": 8},
        {"text": "AGEAS INSURANCE LIMITED", "bold": True, "size": 13, "gap": 8},
        {"text": "Broker statement of account", "size": 10, "gap": 2},
        kv("Statement reference", stmt_ref),
        kv("Agency", broker["broker_name"]),
        kv("Period", "01/10/2026 to 31/10/2026"),
        kv("Agreed commission rate", f"{broker['commission_rate_pct']:.1f} percent"),
        {"text": "", "gap": 8},
        row(["Policy", "Premium GBP", "Rate pct", "Commission GBP", "Net due GBP"],
            xs, bold=True),
        {"text": "-" * 106, "size": 8},
    ]

    mismatch_at = RNG.sample(range(len(theirs)), k=min(2, len(theirs)))
    gross_sum = comm_sum = 0.0
    for n, (_, pol) in enumerate(theirs.iterrows()):
        gross = float(pol["gross_written_premium_gbp"])
        true_comm = round(float(pol["net_premium_gbp"])
                          * broker["commission_rate_pct"] / 100.0, 2)
        shown_comm = true_comm
        if n in mismatch_at:
            # The statement disagrees with the ledger. This is what has to be found.
            shown_comm = round(true_comm * RNG.choice([1.15, 0.82, 1.3]), 2)
        net_due = round(gross - shown_comm, 2)
        gross_sum += gross
        comm_sum += shown_comm
        body.append(row([pol["policy_id"], f"{gross:,.2f}",
                         f"{broker['commission_rate_pct']:.1f}",
                         f"{shown_comm:,.2f}", f"{net_due:,.2f}"], xs))
        statement_line_rows.append(dict(
            file_name=f"{stmt_ref}.pdf", statement_reference=stmt_ref,
            broker_id=broker_id, policy_id=pol["policy_id"],
            premium_gbp=gross, commission_shown_gbp=shown_comm,
            commission_expected_gbp=true_comm,
            matches_ledger=bool(abs(shown_comm - true_comm) < 0.01)))

    body += [
        {"text": "-" * 106, "size": 8},
        row(["Totals", f"{gross_sum:,.2f}", "", f"{comm_sum:,.2f}",
             f"{gross_sum - comm_sum:,.2f}"], xs, bold=True),
        {"text": "", "gap": 8},
        {"text": "Payment is due within 30 days of the statement date. Queries should be "
                 "raised within 14 days.", "size": 8.5},
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL STATEMENT", "bold": True,
         "size": 8, "gap": 12},
    ]

    fname = f"{stmt_ref}.pdf"
    write_pdf(os.path.join(stmt_dir, fname), body, title=stmt_ref)
    statement_rows.append(dict(
        file_name=fname, statement_reference=stmt_ref, broker_id=broker_id,
        broker_name=broker["broker_name"], line_count=len(theirs),
        total_premium_gbp=round(gross_sum, 2),
        total_commission_shown_gbp=round(comm_sum, 2),
        lines_disagreeing_with_ledger=len(mismatch_at)))
    document_index.append(dict(
        file_name=fname, folder="documents/statements",
        document_type="Broker statement of account", team="Brokerage",
        document_layout="table", has_scan_noise=False,
        expected_table="doc_statement_line_expected"))

doc_statement_expected = pd.DataFrame(statement_rows)
doc_statement_line_expected = pd.DataFrame(statement_line_rows)
print(f"wrote {len(doc_statement_expected)} statements, "
      f"{len(doc_statement_line_expected)} lines, "
      f"{(~doc_statement_line_expected.matches_ledger).sum()} disagreeing with the ledger")

# COMMAND ----------

# MAGIC %md ## 8. Pricing - the underwriting guide
# MAGIC
# MAGIC The document a pricing team actually wrestles with. Acceptance rules and rating loadings,
# MAGIC written half as prose and half as tables, across several pages.
# MAGIC
# MAGIC The use case is not "read the PDF". It is: turn eighty pages of rules into rows you can
# MAGIC query, then check whether the prices we actually charged in `fact_quote` follow them.
# MAGIC That is a rate-versus-guide compliance check, and it is the kind of thing that is
# MAGIC currently done by somebody reading a document.

# COMMAND ----------

# factor, condition, action, loading percent, referral required
UW_RULES = [
    ("Driver age", "Under 21 years", "Load", 45.0, False),
    ("Driver age", "21 to 24 years", "Load", 22.0, False),
    ("Driver age", "25 to 69 years", "No adjustment", 0.0, False),
    ("Driver age", "70 to 79 years", "Load", 12.0, False),
    ("Driver age", "80 years and over", "Refer", 0.0, True),
    ("No claims discount", "5 years or more", "Discount", -28.0, False),
    ("No claims discount", "3 to 4 years", "Discount", -18.0, False),
    ("No claims discount", "1 to 2 years", "Discount", -8.0, False),
    ("No claims discount", "None", "No adjustment", 0.0, False),
    ("Claims history", "Two or more fault claims in three years", "Refer", 0.0, True),
    ("Claims history", "One fault claim in three years", "Load", 18.0, False),
    ("Vehicle group", "Group 1 to 10", "No adjustment", 0.0, False),
    ("Vehicle group", "Group 11 to 20", "Load", 15.0, False),
    ("Vehicle group", "Group 21 and above", "Refer", 0.0, True),
    ("Vehicle value", "Over 75,000 GBP", "Refer", 0.0, True),
    ("Vehicle modification", "Performance modification declared", "Refer", 0.0, True),
    ("Vehicle modification", "Cosmetic modification only", "Load", 7.5, False),
    ("Occupation", "Professional sportsperson", "Refer", 0.0, True),
    ("Occupation", "Publican or bar work", "Load", 14.0, False),
    ("Licence", "Held under 12 months", "Load", 30.0, False),
    ("Licence", "Non-UK licence held under 2 years", "Refer", 0.0, True),
    ("Convictions", "Any IN or DR conviction", "Refer", 0.0, True),
    ("Convictions", "Single SP code, 3 points", "Load", 6.0, False),
    ("Property construction", "Thatched roof", "Refer", 0.0, True),
    ("Property construction", "Flat roof over 30 percent of area", "Load", 20.0, False),
    ("Property flood risk", "Flood Re eligible", "Refer", 0.0, True),
    ("Property occupancy", "Unoccupied over 30 consecutive days", "Refer", 0.0, True),
    ("Subsidence history", "Any previous subsidence claim", "Refer", 0.0, True),
]

xs = [0, 120, 300, 370, 440]
guide = [
    {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL UNDERWRITING GUIDE", "bold": True,
     "size": 8},
    {"text": "AGEAS UK PERSONAL LINES", "bold": True, "size": 15, "gap": 8},
    {"text": "Underwriting and rating guide", "bold": True, "size": 11},
    {"text": "Version 2026.3, effective 1 October 2026", "size": 9, "gap": 2},
    {"text": "1. Purpose and scope", "bold": True, "size": 11, "gap": 14},
]
guide += [{"text": s} for s in wrap(
    "This guide sets out the acceptance criteria and rating adjustments that apply to new "
    "business and renewal across motor and home personal lines. Where a risk meets a referral "
    "criterion it must not be quoted automatically and must be passed to an underwriter. "
    "Adjustments are applied multiplicatively to the technical premium in the order shown in "
    "section 3.", 100)]
guide += [{"text": "2. Referral principles", "bold": True, "size": 11, "gap": 14}]
guide += [{"text": s} for s in wrap(
    "A referral is not a decline. Any risk referred under this guide may be written at the "
    "discretion of a senior underwriter, with the rationale recorded. Where a risk attracts "
    "more than one referral criterion the file must be reviewed by a referral panel rather "
    "than an individual. Any deviation from the loadings in section 3 of more than ten "
    "percentage points requires written approval from the pricing manager.", 100)]
guide += [
    {"text": "3. Rating adjustments and acceptance criteria", "bold": True, "size": 11,
     "gap": 14},
    row(["Factor", "Condition", "Action", "Adjustment", "Refer"], xs, bold=True),
    {"text": "-" * 110, "size": 8},
]
for factor, condition, action, loading, refer in UW_RULES:
    adj = "Refer" if refer else (f"{loading:+.1f} pct" if loading else "Nil")
    guide.append(row([factor, condition, action, adj, "Yes" if refer else "No"], xs))
guide += [
    {"text": "-" * 110, "size": 8},
    {"text": "4. Maximum aggregate adjustment", "bold": True, "size": 11, "gap": 14},
]
guide += [{"text": s} for s in wrap(
    "The aggregate of all loadings applied to a single risk must not exceed 85 percent of the "
    "technical premium. Where the calculated aggregate exceeds this figure the risk must be "
    "referred regardless of whether any individual criterion requires referral. The maximum "
    "no claims discount that may be applied is 28 percent, and no further discretionary "
    "discount may be applied on top of it without approval.", 100)]
guide += [
    {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL UNDERWRITING GUIDE", "bold": True,
     "size": 8, "gap": 14},
]

uw_dir = fresh_dir(os.path.join(VOLUME_ROOT, "documents", "underwriting"))
uw_name = "UW-GUIDE-personal-lines-2026.3.pdf"
write_pdf(os.path.join(uw_dir, uw_name), guide, title="Underwriting guide 2026.3")

doc_uw_rule_expected = pd.DataFrame([
    dict(file_name=uw_name, guide_version="2026.3", rule_sequence=i + 1,
         factor=f, condition=c, action=a, adjustment_pct=l, referral_required=r)
    for i, (f, c, a, l, r) in enumerate(UW_RULES)])
document_index.append(dict(
    file_name=uw_name, folder="documents/underwriting",
    document_type="Underwriting guide", team="Pricing",
    document_layout="prose and table, multi-page", has_scan_noise=False,
    expected_table="doc_uw_rule_expected"))
print(f"wrote the underwriting guide with {len(doc_uw_rule_expected)} rules, "
      f"{int(doc_uw_rule_expected.referral_required.sum())} of them referrals")

# COMMAND ----------

# MAGIC %md ## 9. Pricing - competitor panel reports
# MAGIC
# MAGIC The panel report a pricing team gets back from a price comparison site: where our price
# MAGIC sat against the named competitors for a given risk. Each report is built from a real row
# MAGIC in `fact_quote`, so once extracted it can be joined back to what we actually quoted.

# COMMAND ----------

COMPETITORS = ["Hastings Direct", "Admiral", "Direct Line", "LV=", "Churchill",
               "Aviva", "Saga", "Privilege", "Elephant", "More Than"]

pcw_rows, pcw_line_rows = [], []
pcw_dir = fresh_dir(os.path.join(VOLUME_ROOT, "documents", "panel_reports"))
pcw_source = fact_quote[fact_quote["distribution_channel"] == "PCW"]

for i in range(COUNTS["pcw"]):
    q = pcw_source.iloc[RNG.randrange(len(pcw_source))]
    product = prod_by_code[q["product_code"]]
    report_ref = f"PANEL-{RNG.randint(100000, 999999)}"
    our_price = float(q["quoted_premium_gbp"])
    best = float(q["competitor_best_premium_gbp"])

    # A panel of competitor prices spread around the best price we were shown.
    names = RNG.sample(COMPETITORS, k=7)
    prices = sorted(round(best * RNG.uniform(1.0, 1.55), 2) for _ in names)
    prices[0] = best
    panel = list(zip(names, prices)) + [("Ageas", our_price)]
    panel.sort(key=lambda r: r[1])
    our_rank = next(n for n, (nm, _) in enumerate(panel, start=1) if nm == "Ageas")

    xs = [0, 60, 250, 350]
    body = [
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL PANEL REPORT", "bold": True,
         "size": 8},
        {"text": "PRICE COMPARISON PANEL REPORT", "bold": True, "size": 13, "gap": 8},
        {"text": "Market position for a single quoted risk", "size": 10, "gap": 2},
        kv("Report reference", report_ref),
        kv("Quote reference", q["quote_id"]),
        kv("Quote date", fmt_date(pd.to_datetime(q["quote_date"]).date(), i)),
        kv("Product", f"{product['product_name']} ({q['product_code']})"),
        kv("Line of business", product["line_of_business"]),
        {"text": "Risk summary", "bold": True, "size": 10, "gap": 12},
        kv("Age band", q["customer_age_band"]),
        kv("Region", q["region"]),
        kv("No claims years", str(int(q["no_claims_years"]))),
        {"text": "Panel results, cheapest first", "bold": True, "size": 10, "gap": 12},
        row(["Rank", "Insurer", "Annual premium GBP", "Difference to ours"],
            xs, bold=True),
        {"text": "-" * 104, "size": 8},
    ]
    for rank, (name, price) in enumerate(panel, start=1):
        diff = price - our_price
        body.append(row([str(rank), name, f"{price:,.2f}",
                         "ours" if name == "Ageas" else f"{diff:+,.2f}"],
                        xs, bold=(name == "Ageas")))
        pcw_line_rows.append(dict(
            file_name=f"{report_ref}.pdf", report_reference=report_ref,
            quote_id=q["quote_id"], panel_rank=rank, insurer_name=name,
            premium_gbp=price, is_ageas=bool(name == "Ageas")))
    body += [
        {"text": "-" * 104, "size": 8},
        {"text": "", "gap": 8},
        kv("Our rank on this risk", f"{our_rank} of {len(panel)}"),
        kv("Cheapest competitor", f"{panel[0][0]} at GBP {panel[0][1]:,.2f}"
           if panel[0][0] != "Ageas" else "We were cheapest"),
        kv("Our price to market ratio", f"{our_price / best:.3f}"),
        {"text": "SYNTHETIC TEST DOCUMENT - NOT A REAL PANEL REPORT", "bold": True,
         "size": 8, "gap": 12},
    ]

    fname = f"{report_ref}.pdf"
    pcw_noisy = i in NOISY_POSITIONS["panel"]
    write_pdf(os.path.join(pcw_dir, fname), maybe_scanify(body, pcw_noisy),
              title=f"Panel report {report_ref}")
    pcw_rows.append(dict(
        file_name=fname, report_reference=report_ref, quote_id=q["quote_id"],
        product_code=q["product_code"],
        line_of_business=product["line_of_business"],
        quote_date=pd.to_datetime(q["quote_date"]).date(),
        our_premium_gbp=our_price, cheapest_panel_premium_gbp=panel[0][1],
        our_panel_rank=our_rank, panel_size=len(panel),
        price_to_market_ratio=round(our_price / best, 4)))
    document_index.append(dict(
        file_name=fname, folder="documents/panel_reports",
        document_type="Competitor panel report", team="Pricing",
        document_layout="table", has_scan_noise=pcw_noisy,
        expected_table="doc_panel_report_expected"))

doc_panel_report_expected = pd.DataFrame(pcw_rows)
doc_panel_line_expected = pd.DataFrame(pcw_line_rows)
print(f"wrote {len(doc_panel_report_expected)} panel reports with "
      f"{len(doc_panel_line_expected)} competitor lines")

# COMMAND ----------

# MAGIC %md ## 10. Write the ground truth tables
# MAGIC
# MAGIC `document_index` is the one to start from. It lists every document, which team it belongs
# MAGIC to, what shape it is in, whether it carries scan noise, and which table holds the
# MAGIC correct answers.

# COMMAND ----------

doc_index = pd.DataFrame(document_index)
doc_index["volume_path"] = (f"{VOLUME_ROOT}/" + doc_index["folder"] + "/"
                            + doc_index["file_name"])

DOC_FRAMES = {
    "document_index": doc_index,
    "doc_fnol_expected": doc_fnol_expected,
    "doc_estimate_expected": doc_estimate_expected,
    "doc_estimate_line_expected": doc_estimate_line_expected,
    "doc_submission_expected": doc_submission_expected,
    "doc_submission_history_expected": doc_submission_history_expected,
    "doc_statement_expected": doc_statement_expected,
    "doc_statement_line_expected": doc_statement_line_expected,
    "doc_uw_rule_expected": doc_uw_rule_expected,
    "doc_panel_report_expected": doc_panel_report_expected,
    "doc_panel_line_expected": doc_panel_line_expected,
}

DOC_TABLE_COMMENTS = {
    "document_index": "Every synthetic document in the volume, with its type, the team it is "
                      "aimed at, its layout, whether it carries simulated scan noise, and "
                      "which table holds the correct answers. Start here.",
    "doc_fnol_expected": "Ground truth for the FNOL documents. cause_of_loss never appears "
                         "as a word in the document and has to be inferred from the "
                         "narrative.",
    "doc_estimate_expected": "Ground truth for repairer estimates, one row per document. "
                             "total_is_correct is false where the document's own arithmetic "
                             "is wrong.",
    "doc_estimate_line_expected": "Ground truth for repairer estimate line items, one row "
                                  "per line.",
    "doc_submission_expected": "Ground truth for broker risk submissions, one row per "
                               "document.",
    "doc_submission_history_expected": "Ground truth for the five year claims experience "
                                       "table inside each submission, one row per year.",
    "doc_statement_expected": "Ground truth for broker statements of account, one row per "
                              "statement.",
    "doc_statement_line_expected": "Ground truth for statement lines. matches_ledger is "
                                   "false where the commission on the document disagrees "
                                   "with the policy record.",
    "doc_uw_rule_expected": "Ground truth for the underwriting guide, one row per rating "
                            "rule.",
    "doc_panel_report_expected": "Ground truth for competitor panel reports, one row per "
                                 "report.",
    "doc_panel_line_expected": "Ground truth for the competitor lines inside each panel "
                               "report.",
}

DOC_COLUMN_COMMENTS = {
    "document_index": {
        "file_name": "Name of the file in the volume.",
        "folder": "Folder inside the volume that holds it.",
        "document_type": "FNOL, Repairer estimate, Broker risk submission, Broker statement "
                         "of account, Underwriting guide or Competitor panel report.",
        "team": "Which hackathon team the document is aimed at.",
        "document_layout": "Shape of the document. A form, a table, prose, or a forwarded "
                           "email with no structure at all.",
        "has_scan_noise": "True where character errors typical of a scan and OCR pass have "
                          "been introduced. The expected values remain correct, so "
                          "extraction accuracy should measurably drop on these.",
        "expected_table": "Table holding the correct values for this document type.",
        "volume_path": "Full path, ready to pass to READ_FILES.",
    },
    "doc_fnol_expected": {
        "file_name": "Name of the PDF in the fnol folder.",
        "fnol_reference": "Notification reference printed on the document.",
        "claim_id": "Joins to fact_claim.",
        "policy_id": "Joins to fact_policy.",
        "line_of_business": "Motor or Home.",
        "policyholder_name": "Name on the notification.",
        "incident_date": "True date of the incident. The document shows it in one of four "
                         "different formats.",
        "notified_date": "True date we were notified.",
        "incident_time": "Time of the incident as stated.",
        "incident_location": "Location as stated.",
        "vehicle_registration": "Registration as stated. Null for home claims.",
        "vehicle_make": "Vehicle make. Null for home claims.",
        "vehicle_model": "Vehicle model. Null for home claims.",
        "cause_of_loss": "The correct peril. This is the hard one: it is never named in the "
                         "document and must be inferred from the narrative.",
        "third_party_involved": "True where another party is involved.",
        "injury_reported": "True where an injury is mentioned anywhere in the document.",
        "police_reference": "Police reference where one was given, otherwise null.",
        "estimated_reserve_gbp": "Reserve stated on the document.",
        "handler_team": "Team the notification was routed to.",
        "document_layout": "form or email.",
        "has_scan_noise": "True where scan noise was applied.",
    },
    "doc_estimate_expected": {
        "file_name": "Name of the PDF.", "estimate_reference": "Reference on the document.",
        "claim_id": "Joins to fact_claim.", "policy_id": "Joins to fact_policy.",
        "supplier_name": "Repairer or contractor.",
        "line_count": "Number of parts and labour lines.",
        "parts_total_gbp": "Sum of the parts lines.",
        "labour_hours": "Labour hours quoted.",
        "labour_rate_gbp": "Hourly labour rate.",
        "labour_total_gbp": "Labour hours multiplied by the rate.",
        "net_total_gbp": "Parts plus labour, before VAT.",
        "vat_gbp": "VAT at 20 percent of the net total.",
        "stated_total_gbp": "The total printed on the document, which may be wrong.",
        "correct_total_gbp": "What the total should be.",
        "total_is_correct": "False where the document's own arithmetic does not add up.",
    },
    "doc_submission_expected": {
        "file_name": "Name of the PDF.",
        "submission_reference": "Reference on the document.",
        "broker_id": "Joins to dim_broker.", "broker_name": "Broker trading name.",
        "panel_tier": "Strategic, Core or Tail, from dim_broker.",
        "proposed_insured": "Name of the proposed insured.",
        "trade_or_occupation": "Trade of the proposed insured.",
        "covers_requested": "Covers requested, semicolon separated.",
        "sum_insured_gbp": "Total sum insured requested.",
        "target_premium_gbp": "Premium the broker is targeting.",
        "commission_rate_pct": "Commission requested, as a percentage.",
        "renewal_date": "Renewal date of the existing cover.",
        "total_claims_last_5y": "Total claim count across the five experience years.",
        "total_incurred_last_5y_gbp": "Total incurred across the five experience years.",
        "has_scan_noise": "True where scan noise was applied.",
    },
    "doc_statement_line_expected": {
        "file_name": "Name of the PDF.",
        "statement_reference": "Reference on the document.",
        "broker_id": "Joins to dim_broker.", "policy_id": "Joins to fact_policy.",
        "premium_gbp": "Gross written premium shown on the statement.",
        "commission_shown_gbp": "Commission printed on the statement.",
        "commission_expected_gbp": "Commission the policy record implies.",
        "matches_ledger": "False where the statement disagrees with the policy record. "
                          "Finding these lines is the exercise.",
    },
    "doc_uw_rule_expected": {
        "file_name": "Name of the PDF.", "guide_version": "Version of the guide.",
        "rule_sequence": "Order the rule appears in the table.",
        "factor": "Rating factor the rule applies to.",
        "condition": "Condition that triggers the rule.",
        "action": "Load, Discount, Refer or No adjustment.",
        "adjustment_pct": "Percentage adjustment to the technical premium. Negative is a "
                          "discount. Zero where the action is a referral.",
        "referral_required": "True where the risk must go to an underwriter.",
    },
    "doc_estimate_line_expected": {
        "file_name": "Name of the PDF the line came from.",
        "estimate_reference": "Reference printed on the estimate.",
        "claim_id": "Joins to fact_claim.",
        "line_type": "Parts or Labour.",
        "description": "Description of the part or the labour item, as printed.",
        "quantity": "Quantity for a parts line, or hours for a labour line.",
        "unit_price_gbp": "Price per unit, or the hourly rate for labour.",
        "amount_gbp": "Quantity multiplied by the unit price.",
    },
    "doc_submission_history_expected": {
        "file_name": "Name of the PDF the row came from.",
        "submission_reference": "Reference printed on the submission.",
        "broker_id": "Joins to dim_broker.",
        "experience_year": "Year the claims experience relates to.",
        "claim_count": "Claims declared for that year.",
        "incurred_gbp": "Incurred cost declared for that year, in GBP.",
    },
    "doc_statement_expected": {
        "file_name": "Name of the PDF.",
        "statement_reference": "Reference printed on the statement.",
        "broker_id": "Joins to dim_broker.",
        "broker_name": "Agency name as printed.",
        "line_count": "Number of policy lines on the statement.",
        "total_premium_gbp": "Total premium across the lines, as printed.",
        "total_commission_shown_gbp": "Total commission across the lines, as printed.",
        "lines_disagreeing_with_ledger": "How many lines show a commission that does not "
                                         "match the policy record. Finding them is the "
                                         "exercise.",
    },
    "doc_panel_line_expected": {
        "file_name": "Name of the PDF the line came from.",
        "report_reference": "Reference printed on the panel report.",
        "quote_id": "Joins to fact_quote.",
        "panel_rank": "Position on the panel, 1 being cheapest.",
        "insurer_name": "Insurer named on that line.",
        "premium_gbp": "Annual premium quoted by that insurer, in GBP.",
        "is_ageas": "True on the line that is our own price.",
    },
    "doc_panel_report_expected": {
        "file_name": "Name of the PDF.", "report_reference": "Reference on the document.",
        "quote_id": "Joins to fact_quote.", "product_code": "Joins to dim_product.",
        "line_of_business": "Motor, Home, Travel or Pet.",
        "quote_date": "Date of the quote.",
        "our_premium_gbp": "The price we showed.",
        "cheapest_panel_premium_gbp": "Cheapest price anywhere on the panel.",
        "our_panel_rank": "Where we ranked, 1 being cheapest.",
        "panel_size": "Number of insurers on the panel.",
        "price_to_market_ratio": "Our price divided by the best competitor price.",
    },
}

BOOLEAN_COLUMNS = {"has_scan_noise", "third_party_involved", "injury_reported",
                   "total_is_correct", "matches_ledger", "referral_required", "is_ageas"}
INTEGER_COLUMNS = {"rule_sequence", "line_count", "experience_year", "claim_count",
                   "total_claims_last_5y", "panel_rank", "our_panel_rank", "panel_size",
                   "lines_disagreeing_with_ledger"}


def spark_schema(pdf):
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
    for name, pdf in DOC_FRAMES.items():
        fqn = f"{CATALOG}.{SCHEMA}.{name}"
        (to_spark_df(pdf)
            .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn))
        comment = DOC_TABLE_COMMENTS[name].replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {fqn} IS '{comment}'")  # noqa: F821
        for col, text in DOC_COLUMN_COMMENTS.get(name, {}).items():
            spark.sql(f"ALTER TABLE {fqn} ALTER COLUMN {col} "  # noqa: F821
                      f"COMMENT '{text.replace(chr(39), chr(39) * 2)}'")
        print(f"  wrote {fqn} ({len(pdf):,} rows)")
else:
    out = os.path.join(VOLUME_ROOT, "tables")
    os.makedirs(out, exist_ok=True)
    for name, pdf in DOC_FRAMES.items():
        pdf.to_csv(os.path.join(out, f"{name}.csv"), index=False)
        print(f"  wrote {name}.csv ({len(pdf):,} rows)")

# COMMAND ----------

# MAGIC %md ## 11. What was produced

# COMMAND ----------

summary = (doc_index.groupby(["team", "document_type"])
           .agg(documents=("file_name", "size"),
                with_scan_noise=("has_scan_noise", "sum"))
           .reset_index())
if IN_DATABRICKS:
    display(to_spark_df(summary))  # noqa: F821
else:
    print(summary.to_string(index=False))
    print(f"\ntotal documents: {len(doc_index)}")
