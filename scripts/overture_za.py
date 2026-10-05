"""Pull South African places from Overture Maps and either count them or load them into Scowt's Supabase.

Overture places data: CDLA-Permissive 2.0 / Apache 2.0 (see https://docs.overturemaps.org/attribution/).
Logs print aggregates only, never individual businesses.

Usage:
  python overture_za.py count
  python overture_za.py load   (needs SCOWT_JOB_TOKEN)
Env: RELEASE, MIN_CONFIDENCE, EXCLUDE_CATEGORIES (comma list), INCLUDE_CHAINS (true/false),
     PROVINCES (comma list, empty = all, loaded in that order), MAX_ROWS (0 = no limit)
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

import duckdb

RELEASE = os.environ.get("RELEASE") or "2026-09-23.1"
SRC = f"s3://overturemaps-us-west-2/release/{RELEASE}/theme=places/type=place/*"
INGEST_URL = "https://feonpwfzmpfoibkfezef.supabase.co/functions/v1/prospect-ingest"
SUMMARY = os.environ.get("GITHUB_STEP_SUMMARY")

PROV = {
    "GP": "Gauteng", "GT": "Gauteng", "GAUTENG": "Gauteng",
    "WC": "Western Cape", "WESTERN CAPE": "Western Cape",
    "EC": "Eastern Cape", "EASTERN CAPE": "Eastern Cape",
    "KZN": "KwaZulu-Natal", "NL": "KwaZulu-Natal", "KN": "KwaZulu-Natal", "KWAZULU-NATAL": "KwaZulu-Natal", "KWAZULU NATAL": "KwaZulu-Natal",
    "FS": "Free State", "FREE STATE": "Free State",
    "NW": "North West", "NORTH WEST": "North West",
    "LP": "Limpopo", "NP": "Limpopo", "LIMPOPO": "Limpopo",
    "MP": "Mpumalanga", "MPUMALANGA": "Mpumalanga",
    "NC": "Northern Cape", "NORTHERN CAPE": "Northern Cape",
}


def province(region):
    if not region:
        return None
    k = str(region).upper().replace("ZA-", "").strip()
    return PROV.get(k, region)


def summary(md):
    print(md)
    if SUMMARY:
        with open(SUMMARY, "a", encoding="utf-8") as f:
            f.write(md + "\n")


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")
    return con


BASE = f"""
SELECT
  id,
  names.primary AS name,
  coalesce(basic_category, taxonomy.primary) AS category,
  taxonomy.primary AS subcategory,
  confidence,
  operating_status,
  websites[1] AS website,
  phones[1] AS phone,
  emails[1] AS email,
  list_filter(socials, s -> s ILIKE '%facebook.com%')[1] AS facebook,
  brand.wikidata IS NOT NULL AS is_chain,
  addresses[1].locality AS town,
  addresses[1].region AS region,
  addresses[1].freeform AS street,
  addresses[1].postcode AS postcode,
  (bbox.xmin + bbox.xmax) / 2 AS lng,
  (bbox.ymin + bbox.ymax) / 2 AS lat,
  list_max(list_transform(sources, x -> x.update_time)) AS last_update,
  array_to_string(list_distinct(list_transform(sources, x -> x.dataset)), ', ') AS datasets
FROM read_parquet('{SRC}', hive_partitioning = 1)
WHERE bbox.xmin > 16.3 AND bbox.xmax < 33.0 AND bbox.ymin > -35.0 AND bbox.ymax < -22.0
  AND addresses[1].country = 'ZA'
  AND names.primary IS NOT NULL
"""


def count():
    con = connect()
    t0 = time.time()
    con.execute(f"CREATE TABLE za AS {BASE}")
    total, open_, closed = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE coalesce(operating_status,'open') <> 'permanently_closed'), "
        "count(*) FILTER (WHERE operating_status = 'permanently_closed') FROM za").fetchone()
    summary(f"## Overture South Africa, release {RELEASE}\n\nPulled in {time.time() - t0:.0f}s. "
            f"**{total:,}** named places with a South African address: {open_:,} not marked closed, {closed:,} marked permanently closed.\n")

    annotate("notice", f"TOTAL {total} named ZA places; {open_} not closed; {closed} permanently closed; pulled in {time.time() - t0:.0f}s")

    def table(title, sql, cols, per_note=70):
        rows = con.execute(sql).fetchall()
        md = f"\n### {title}\n\n| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
        for r in rows:
            md += "| " + " | ".join(f"{v:,}" if isinstance(v, int) else ("" if v is None else str(v)) for v in r) + " |\n"
        summary(md)
        compact = ["|".join("" if v is None else str(v) for v in r) for r in rows]
        for i in range(0, len(compact), per_note):
            annotate("notice", f"{title} [{'/'.join(cols)}] " + " ; ".join(compact[i:i + per_note]))

    con.create_function("prov", province, [str], str, null_handling="special")
    open_filter = "coalesce(operating_status,'open') <> 'permanently_closed'"
    table("By province", f"SELECT prov(region) p, count(*) n, count(phone) with_phone, count(website) with_site, count(facebook) with_facebook FROM za WHERE {open_filter} GROUP BY 1 ORDER BY 2 DESC",
          ["Province", "Places", "With phone", "With website", "With Facebook"])
    table("By confidence", f"SELECT CASE WHEN confidence >= 0.8 THEN '0.8+' WHEN confidence >= 0.6 THEN '0.6-0.8' WHEN confidence >= 0.4 THEN '0.4-0.6' ELSE 'under 0.4' END b, count(*) FROM za WHERE {open_filter} GROUP BY 1 ORDER BY 1 DESC",
          ["Confidence", "Places"])
    table("By last update", f"SELECT substr(CAST(last_update AS VARCHAR),1,4) y, count(*) FROM za WHERE {open_filter} GROUP BY 1 ORDER BY 1 DESC NULLS LAST",
          ["Year of last source update", "Places"])
    table("Chains vs independents", f"SELECT CASE WHEN is_chain THEN 'Chain (brand on Wikidata)' ELSE 'Independent' END, count(*) FROM za WHERE {open_filter} GROUP BY 1",
          ["Type", "Places"])
    table("Top 200 categories", f"SELECT category, count(*) n, count(phone) with_phone FROM za WHERE {open_filter} GROUP BY 1 ORDER BY 2 DESC LIMIT 200",
          ["Category", "Places", "With phone"])


def post(rows, token):
    data = json.dumps({"rows": rows}, default=str).encode()
    for attempt in range(5):
        req = urllib.request.Request(INGEST_URL, data=data, method="POST",
                                     headers={"Content-Type": "application/json", "x-job-token": token})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")[:300]
            if e.code == 507:
                return {"size_cap": True, "detail": body}
            if e.code in (400, 403):
                raise SystemExit(f"ingest refused ({e.code}): {body}")
            time.sleep(2 ** attempt)
        except Exception:
            time.sleep(2 ** attempt)
    raise SystemExit("ingest failed after retries")


def load():
    token = os.environ.get("SCOWT_JOB_TOKEN")
    if not token:
        raise SystemExit("SCOWT_JOB_TOKEN secret is missing")
    min_conf = float(os.environ.get("MIN_CONFIDENCE") or 0.5)
    exclude = [c.strip().lower() for c in (os.environ.get("EXCLUDE_CATEGORIES") or "").split(",") if c.strip()]
    include_chains = (os.environ.get("INCLUDE_CHAINS") or "false").lower() == "true"
    provinces = [p.strip() for p in (os.environ.get("PROVINCES") or "").split(",") if p.strip()]
    max_rows = int(os.environ.get("MAX_ROWS") or 0)

    con = connect()
    con.create_function("prov", province, [str], str, null_handling="special")
    where = [f"coalesce(confidence,0) >= {min_conf}", "coalesce(operating_status,'open') <> 'permanently_closed'"]
    if not include_chains:
        where.append("NOT is_chain")
    if exclude:
        where.append("lower(coalesce(category,'')) NOT IN (" + ",".join("'" + c.replace("'", "''") + "'" for c in exclude) + ")")
    order = "confidence DESC"
    if provinces:
        cases = " ".join(f"WHEN prov(region) = '{p}' THEN {i}" for i, p in enumerate(provinces))
        where.append("prov(region) IN (" + ",".join(f"'{p}'" for p in provinces) + ")")
        order = f"CASE {cases} ELSE 99 END, confidence DESC"
    sql = f"SELECT * FROM ({BASE}) WHERE {' AND '.join(where)} ORDER BY {order}" + (f" LIMIT {max_rows}" if max_rows else "")
    t0 = time.time()
    cur = con.execute(sql)
    sent = 0
    capped = False
    while True:
        batch = cur.fetchmany(1000)
        if not batch:
            break
        cols = [d[0] for d in cur.description]
        rows = []
        for b in batch:
            r = dict(zip(cols, b))
            addr = ", ".join(x for x in [r["street"], r["town"], r["postcode"]] if x) or None
            rows.append({
                "source": "overture", "source_ref": r["id"], "name": str(r["name"])[:200],
                "category": (r["category"] or "").replace("_", " ") or None, "subcategory": r["subcategory"],
                "town": r["town"], "province": province(r["region"]), "address": addr,
                "lat": r["lat"], "lng": r["lng"], "phone": r["phone"], "website": r["website"],
                "email": r["email"], "facebook": r["facebook"], "is_chain": bool(r["is_chain"]),
                "confidence": r["confidence"], "operating_status": r["operating_status"],
                "source_last_edited": r["last_update"],
                "sources": [{"label": "Overture Maps" + (f" ({r['datasets']})" if r["datasets"] else "")}],
            })
        res = post(rows, token)
        if res.get("size_cap"):
            capped = True
            break
        sent += len(rows)
        if sent % 20000 == 0:
            print(f"loaded {sent:,} so far (database {res.get('db_mb')} MB)")
    summary(f"## Load finished\n\nSent **{sent:,}** places in {time.time() - t0:.0f}s "
            f"(min confidence {min_conf}, chains {'included' if include_chains else 'excluded'}, "
            f"{len(exclude)} categories excluded, provinces: {', '.join(provinces) or 'all'})."
            + ("\n\n**Stopped at the database size cap.**" if capped else ""))


def annotate(level, msg):
    """GitHub shows these as run annotations, readable through the API without the log files."""
    clean = str(msg).replace("%", "%25").replace("\r", "").replace("\n", "%0A")[:3000]
    print(f"::{level}::{clean}", flush=True)


if __name__ == "__main__":
    import traceback
    try:
        {"count": count, "load": load}[sys.argv[1] if len(sys.argv) > 1 else "count"]()
    except SystemExit as e:
        annotate("error", f"stopped: {e}")
        raise
    except Exception:
        annotate("error", traceback.format_exc()[-2500:])
        raise
