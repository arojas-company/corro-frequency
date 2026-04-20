"""
FREQUENCY PIPELINE v1 — Equestrian Labs / Corro
================================================
Responde exactamente lo que pidio Ceci en la reunion:

  "Todos mis clientes del 2025, cuantos han comprado UNA vez,
   DOS veces, TRES O MAS veces?"

  "De los que compraron en Q1 2025, cuantos volvieron a comprar
   en los 12 meses siguientes?"

  "El total tiene que cuadrar con 400-600 activos por mes, no 57."
  FIX: el error anterior filtraba solo clientes nuevos. Aqui se toman
  TODOS los que compraron en el periodo (nuevos + recurrentes).

Tabs que escribe:
  freq_distribution  cuantos clientes: 1x / 2x / 3x+ y su revenue
  freq_monthly       clientes activos por mes (debe dar ~400-600)
  freq_segments      Ecom vs Concierge: frecuencia y lealtad
  freq_cohort        retencion: de los de Q1, cuantos volvieron?

Run:
  python frequency_v1.py --year 2025
  python frequency_v1.py --year 2025 --skip-cohort   (rapido, ~5 min)
  python frequency_v1.py --year 2026

Secrets requeridos en GitHub Actions:
  SHOPIFY_TOKEN_CORRO   GOOGLE_CREDENTIALS   SHEET_ID_FREQ
"""

import os
import json
import time
import requests
import gspread
import argparse
import calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
from collections import defaultdict
import pytz

# ── CONFIG ────────────────────────────────────────────────────────
TIMEZONE    = pytz.timezone("America/Bogota")
API_VERSION = "2024-10"
STORE_URL   = os.environ.get("SHOPIFY_STORE", "equestrian-labs.myshopify.com")
TOKEN       = os.environ.get("SHOPIFY_TOKEN_CORRO", "")
SHEET_ID    = os.environ.get("SHEET_ID_FREQ", "")
SCOPES      = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── SHOPIFY REST — con retry automatico en 429 ────────────────────
def shopify_get(endpoint, params=None):
    """
    Paginacion automatica + retry con backoff en rate limit (429).
    Espera 0.3s entre llamadas para mantenerse bajo el limite de Shopify.
    """
    if params is None:
        params = {}
    url = f"https://{STORE_URL}/admin/api/{API_VERSION}/{endpoint}"
    headers = {"X-Shopify-Access-Token": TOKEN}
    results = []
    while url:
        for attempt in range(8):
            try:
                r = requests.get(url, headers=headers, params=params, timeout=60)
            except requests.exceptions.RequestException as e:
                print(f"    network error attempt {attempt+1}: {e}")
                time.sleep(5)
                continue
            if r.status_code == 429:
                wait = min(int(r.headers.get("Retry-After", "") or 2 ** attempt), 60)
                print(f"    rate-limited — waiting {wait}s (attempt {attempt+1})...")
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                print(f"    server error {r.status_code} — retrying in 10s...")
                time.sleep(10)
                continue
            r.raise_for_status()
            break
        else:
            raise RuntimeError(f"Failed after 8 attempts: {endpoint}")

        data = r.json()
        # Find the data key (skip 'errors' key if present)
        data_keys = [k for k in data if k != "errors"]
        if not data_keys:
            break
        key = data_keys[0]
        batch = data.get(key, [])
        if isinstance(batch, list):
            results.extend(batch)
        else:
            results.append(batch)

        # Pagination
        link = r.headers.get("Link", "")
        url = None
        params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")

        time.sleep(0.35)  # stay safely under Shopify's 2 req/s bucket
    return results

# ── FETCH ORDERS ──────────────────────────────────────────────────
def fetch_orders(start, end):
    """
    Jala TODAS las ordenes pagadas del rango de fechas.
    Incluye nuevos Y recurrentes — este era el bug que daba 57 en lugar de 500+.
    """
    print(f"  Fetching orders {start} to {end}...")
    orders = shopify_get("orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{start}T00:00:00-05:00",
        "created_at_max":   f"{end}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,created_at,subtotal_price,source_name,tags,customer",
    })
    print(f"  -> {len(orders)} orders retrieved")
    return orders

def fetch_customer_order_history(customer_ids):
    """
    Para el analisis de cohorte: jala el historial completo de cada cliente
    para saber si volvio a comprar despues de su primer trimestre.

    En lugar de una llamada por cliente (muy lento y rate-limit),
    jalamos TODAS las ordenes desde 2024 y las agrupamos por cliente_id.
    Mucho mas eficiente: ~10-20 llamadas en lugar de 1500+.
    """
    print("  Fetching full order history for cohort analysis...")
    print("  (pulling all orders since 2024-01-01 — much faster than per-customer calls)")

    # Fetch all orders from 2024 onwards in one paginated sweep
    all_orders = shopify_get("orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   "2024-01-01T00:00:00-05:00",
        "limit":            250,
        "fields":           "id,created_at,customer",
    })
    print(f"  -> {len(all_orders)} total orders loaded for history")

    cid_set = set(str(c) for c in customer_ids)
    history = defaultdict(list)
    for o in all_orders:
        c   = o.get("customer") or {}
        cid = str(c.get("id", ""))
        if not cid or cid not in cid_set:
            continue
        d = o.get("created_at", "")[:10]
        if d:
            history[cid].append(d)

    for cid in history:
        history[cid].sort()

    return history

# ── CHANNEL DETECTION ─────────────────────────────────────────────
# Confirmado del tag audit Q4 2025 (326 ordenes Concierge):
# 'concierge, LH' | 'Concierge, DG' | 'Concierge, JS' | 'Concierge, JW'
def detect_channel(order):
    src  = (order.get("source_name") or "").lower().strip()
    tags = (order.get("tags") or "").lower()
    if "concierge" in tags or "concierge" in src:
        return "Concierge"
    if src == "pos" or "wellington" in tags:
        return "Wellington (POS)"
    return "Online (Ecom)"

# ── TAB 1: DISTRIBUCION DE FRECUENCIA ────────────────────────────
def build_distribution(orders, year_label, now_str):
    """
    La tabla principal que pidio Ceci:
    - Cuantos clientes compraron 1 vez, 2 veces, 3 o mas veces
    - Revenue que trajo cada grupo
    - AOV por grupo
    El total de clientes debe cuadrar con lo que se ve en Shopify.
    """
    # Agrupar ordenes por cliente
    cust_orders  = defaultdict(list)   # cid -> [fecha, ...]
    cust_revenue = defaultdict(float)  # cid -> total revenue

    for o in orders:
        c   = o.get("customer") or {}
        cid = str(c.get("id") or f"guest_{o.get('id','')}")
        d   = (o.get("created_at") or "")[:10]
        rev = float(o.get("subtotal_price") or 0)
        if d:
            cust_orders[cid].append(d)
        cust_revenue[cid] += rev

    # Buckets de frecuencia
    BUCKETS = ["1x — bought once", "2x — bought twice", "3x+ — loyal buyers"]
    data = {b: {"customers": 0, "net_sales": 0.0, "orders": 0} for b in BUCKETS}

    for cid, order_list in cust_orders.items():
        n = len(order_list)
        if n == 1:
            bucket = "1x — bought once"
        elif n == 2:
            bucket = "2x — bought twice"
        else:
            bucket = "3x+ — loyal buyers"
        data[bucket]["customers"] += 1
        data[bucket]["net_sales"] += cust_revenue[cid]
        data[bucket]["orders"]    += n

    total_cust = sum(d["customers"] for d in data.values()) or 1
    total_rev  = sum(d["net_sales"]  for d in data.values()) or 1
    total_ord  = sum(d["orders"]     for d in data.values()) or 1
    avg_freq   = round(total_ord / total_cust, 2)

    print(f"\n  ── FREQUENCY DISTRIBUTION ({year_label}) ──────────────────")
    print(f"  Total unique customers : {total_cust}")
    print(f"  Total orders           : {total_ord}")
    print(f"  Avg orders/customer    : {avg_freq}  (Ceci mentioned ~2.8)")
    for b in BUCKETS:
        d = data[b]
        pct_c = round(d["customers"] / total_cust * 100, 1)
        pct_r = round(d["net_sales"]  / total_rev  * 100, 1)
        print(f"    {b}: {d['customers']} customers ({pct_c}%) -> ${d['net_sales']:,.0f} ({pct_r}%)")
    print(f"  ──────────────────────────────────────────────────────────\n")

    rows = []
    for b in BUCKETS:
        d = data[b]
        aov = round(d["net_sales"] / d["orders"], 2) if d["orders"] else 0
        rows.append([
            now_str, year_label, b,
            d["customers"],
            round(d["customers"] / total_cust * 100, 1),
            round(d["net_sales"], 2),
            round(d["net_sales"] / total_rev * 100, 1),
            d["orders"],
            aov,
        ])

    return rows, total_cust, total_ord

# ── TAB 2: CLIENTES ACTIVOS POR MES ──────────────────────────────
def build_monthly(orders, year, now_str):
    """
    Clientes activos por mes — debe cuadrar con ~400-600 que ve Ceci
    en Shopify Analytics. Si da 57 algo esta mal filtrado.
    Aqui NO filtramos solo nuevos — contamos TODOS los que compraron ese mes.
    """
    monthly = {}
    first_seen_in_year = {}  # cid -> primer mes en que aparece en estos datos

    for o in sorted(orders, key=lambda x: x.get("created_at") or ""):
        c   = o.get("customer") or {}
        cid = str(c.get("id") or f"guest_{o.get('id','')}")
        d   = (o.get("created_at") or "")[:10]
        if not d or len(d) < 7:
            continue
        mo  = d[:7]  # "2025-01"
        rev = float(o.get("subtotal_price") or 0)

        if mo not in monthly:
            monthly[mo] = {
                "all_customers":       set(),
                "new_to_year":         set(),
                "returning_in_year":   set(),
                "orders":              0,
                "net_sales":           0.0,
            }

        monthly[mo]["all_customers"].add(cid)
        monthly[mo]["orders"]    += 1
        monthly[mo]["net_sales"] += rev

        if cid not in first_seen_in_year:
            first_seen_in_year[cid] = mo
            monthly[mo]["new_to_year"].add(cid)
        else:
            monthly[mo]["returning_in_year"].add(cid)

    rows = []
    for mo in sorted(monthly.keys()):
        m = monthly[mo]
        total  = len(m["all_customers"])
        new_c  = len(m["new_to_year"])
        ret_c  = len(m["returning_in_year"])
        orders = m["orders"]
        rev    = m["net_sales"]
        rows.append([
            now_str, str(year), mo,
            total, new_c, ret_c,
            orders,
            round(rev, 2),
            round(orders / total, 2) if total else 0,
            round(rev    / total, 2) if total else 0,
        ])
        print(f"  {mo}: {total:>4} active  ({new_c:>4} new + {ret_c:>4} returning) | ${rev:>10,.0f}")

    return rows

# ── TAB 3: FRECUENCIA POR CANAL ───────────────────────────────────
def build_by_channel(orders, year_label, now_str):
    """
    Ecom vs Concierge vs Wellington:
    - Cual canal tiene clientes mas leales (compran mas veces)?
    - Cual trae mas revenue por cliente?
    Ceci quiere saber si Concierge tiene mejor retencion que Ecom.
    """
    # Construir mapa cid -> {channel, orders, revenue}
    cust_by_channel = defaultdict(lambda: defaultdict(lambda: {
        "order_dates": [],
        "revenue": 0.0,
    }))

    for o in orders:
        c   = o.get("customer") or {}
        cid = str(c.get("id") or f"guest_{o.get('id','')}")
        ch  = detect_channel(o)
        d   = (o.get("created_at") or "")[:10]
        rev = float(o.get("subtotal_price") or 0)
        if d:
            cust_by_channel[ch][cid]["order_dates"].append(d)
        cust_by_channel[ch][cid]["revenue"] += rev

    rows = []
    print(f"\n  ── BY CHANNEL ({year_label}) ─────────────────────────────────")

    for ch in sorted(cust_by_channel.keys()):
        cust_map = cust_by_channel[ch]
        freq_buckets = {"1x": 0, "2x": 0, "3x+": 0}
        rev_buckets  = {"1x": 0.0, "2x": 0.0, "3x+": 0.0}
        total_cust   = len(cust_map)
        total_orders = 0
        total_rev    = 0.0

        for cid, info in cust_map.items():
            n = len(info["order_dates"])
            total_orders += n
            total_rev    += info["revenue"]
            k = "1x" if n == 1 else ("2x" if n == 2 else "3x+")
            freq_buckets[k] += 1
            rev_buckets[k]  += info["revenue"]

        avg_freq = round(total_orders / total_cust, 2) if total_cust else 0
        avg_rev  = round(total_rev    / total_cust, 2) if total_cust else 0

        print(f"  {ch}: {total_cust} customers | avg {avg_freq} orders | "
              f"1x:{freq_buckets['1x']}  2x:{freq_buckets['2x']}  3x+:{freq_buckets['3x+']}")

        for freq_label in ["1x", "2x", "3x+"]:
            cnt = freq_buckets[freq_label]
            rows.append([
                now_str, year_label, ch, freq_label,
                cnt,
                round(cnt / total_cust * 100, 1) if total_cust else 0,
                round(rev_buckets[freq_label], 2),
                total_cust, total_orders, avg_freq, avg_rev,
            ])

    print(f"  ─────────────────────────────────────────────────────────\n")
    return rows

# ── TAB 4: COHORTE DE RETENCION ───────────────────────────────────
def build_cohort(orders, year, now_str, history):
    """
    La pregunta clave de Ceci:
    "De los clientes que compraron en Q1 2025, cuantos volvieron
    a comprar en los siguientes 3, 6 y 12 meses?"

    Logica:
    1. Para cada trimestre, identificar los clientes que compraron ESE trimestre
    2. Ver si en su historial total tienen ordenes DESPUES del trimestre
    3. Calcular tasa de retencion a 3m / 6m / 12m
    """
    # Primer orden de cada cliente dentro del ano analizado
    cust_first_in_year = {}
    for o in orders:
        c   = o.get("customer") or {}
        cid = str(c.get("id") or "")
        if not cid or cid.startswith("guest_"):
            continue
        d = (o.get("created_at") or "")[:10]
        if not d:
            continue
        if cid not in cust_first_in_year or d < cust_first_in_year[cid]:
            cust_first_in_year[cid] = d

    # Agrupar por trimestre de primera compra en el ano
    quarters = {f"Q{q}_{year}": [] for q in range(1, 5)}
    for cid, first_d in cust_first_in_year.items():
        try:
            mo = int(first_d[5:7])
        except (ValueError, IndexError):
            continue
        q  = (mo - 1) // 3 + 1
        qk = f"Q{q}_{year}"
        if qk in quarters:
            quarters[qk].append(cid)

    rows = []
    print(f"\n  ── COHORT RETENTION ({year}) ────────────────────────────────")

    for q_num in range(1, 5):
        qk       = f"Q{q_num}_{year}"
        cohort   = quarters[qk]
        q_start  = date(year, (q_num - 1) * 3 + 1, 1)
        q_end_m  = q_num * 3
        q_end    = date(year, q_end_m, calendar.monthrange(year, q_end_m)[1])

        returned_3m  = 0
        returned_6m  = 0
        returned_12m = 0
        total = len(cohort)

        for cid in cohort:
            past_orders = history.get(cid, [])
            # Solo contar ordenes DESPUES del fin del trimestre
            future_dates = [d for d in past_orders if d > str(q_end)]
            if not future_dates:
                continue
            earliest = min(future_dates)
            try:
                ret_date     = date.fromisoformat(earliest)
                days_elapsed = (ret_date - q_end).days
            except ValueError:
                continue

            if days_elapsed <= 90:
                returned_3m  += 1
            if days_elapsed <= 180:
                returned_6m  += 1
            if days_elapsed <= 365:
                returned_12m += 1

        rate_3m  = round(returned_3m  / total * 100, 1) if total else 0
        rate_6m  = round(returned_6m  / total * 100, 1) if total else 0
        rate_12m = round(returned_12m / total * 100, 1) if total else 0

        print(f"  {qk}: {total:>4} customers | "
              f"3m: {returned_3m:>4} ({rate_3m}%)  "
              f"6m: {returned_6m:>4} ({rate_6m}%)  "
              f"12m: {returned_12m:>4} ({rate_12m}%)")

        rows.append([
            now_str, str(year), qk,
            str(q_start), str(q_end),
            total,
            returned_3m,  returned_6m,  returned_12m,
            rate_3m,      rate_6m,      rate_12m,
        ])

    print(f"  ─────────────────────────────────────────────────────────\n")
    return rows

# ── GOOGLE SHEETS ─────────────────────────────────────────────────
def get_gc():
    raw = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS env var is missing")
    creds = Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    return gspread.authorize(creds)

def sheets_call(fn, *args, **kwargs):
    """Retry automatico en 429/500 de Google Sheets API."""
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, "response") else 0
            if status == 429 or status >= 500:
                wait = min(15 * (attempt + 1), 90)
                print(f"    Sheets API {status} — waiting {wait}s (attempt {attempt+1})...")
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            if attempt < 3:
                print(f"    Sheets error, retrying: {e}")
                time.sleep(10)
                continue
            raise
    raise RuntimeError("Sheets call failed after 8 attempts")

def upsert_tab(sh, tab_name, headers, new_rows, key_cols):
    """
    Upsert con retry automatico en 429, batches de 500 filas,
    y pausa entre tabs para no superar 60 writes/min de Sheets API.
    """
    BATCH_SIZE  = 500
    SLEEP_BATCH = 2.0
    SLEEP_TAB   = 5.0

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheets_call(sh.add_worksheet, tab_name,
                         rows=max(3000, len(new_rows) + 100),
                         cols=len(headers) + 2)
        time.sleep(2)

    existing = {}
    try:
        vals = sheets_call(ws.get_all_values)
        if len(vals) >= 2:
            ex_h = vals[0]
            for r in vals[1:]:
                m = {ex_h[i]: (r[i] if i < len(r) else "") for i in range(len(ex_h))}
                k = tuple(str(m.get(c, "")).strip() for c in key_cols)
                if any(k):
                    existing[k] = [m.get(h, "") for h in headers]
    except Exception as e:
        print(f"    Warning reading {tab_name}: {e}")

    for row in new_rows:
        m = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        k = tuple(str(m.get(c, "")).strip() for c in key_cols)
        existing[k] = row

    merged = sorted(existing.values(), key=lambda r: str(r[1]) if len(r) > 1 else "")

    all_data = [headers]
    for r in merged:
        clean = ["" if (v is None or (isinstance(v, float) and v != v)) else v for v in r]
        all_data.append(clean)

    if len(all_data) > ws.row_count or len(headers) > ws.col_count:
        sheets_call(ws.resize, rows=len(all_data) + 50, cols=len(headers) + 2)
        time.sleep(1)

    sheets_call(ws.clear)
    time.sleep(1)

    written = 0
    for i in range(0, len(all_data), BATCH_SIZE):
        batch = all_data[i:i + BATCH_SIZE]
        sheets_call(ws.append_rows, batch, value_input_option="USER_ENTERED",
                    insert_data_option="INSERT_ROWS")
        written += len(batch)
        if i + BATCH_SIZE < len(all_data):
            time.sleep(SLEEP_BATCH)

    time.sleep(SLEEP_TAB)
    print(f"    ok {tab_name}: {len(new_rows)} new rows, {len(merged)} total")

# ── MAIN ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Frequency analysis pipeline")
    parser.add_argument("--year", type=int, default=2025,
                        help="Year to analyze (default: 2025)")
    parser.add_argument("--skip-cohort", action="store_true",
                        help="Skip cohort tab — faster run, no extra API calls")
    args = parser.parse_args()

    year    = args.year
    today   = datetime.now(TIMEZONE).date()
    start   = date(year, 1, 1)
    end     = date(year, 12, 31) if year < today.year else today
    label   = f"full_year_{year}"
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    if not TOKEN:
        raise RuntimeError("SHOPIFY_TOKEN_CORRO env var is missing")
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID_FREQ env var is missing")

    print(f"\n{'='*60}")
    print(f"  FREQUENCY PIPELINE — Corro")
    print(f"  Store : {STORE_URL}")
    print(f"  Year  : {year}  ({start} to {end})")
    print(f"  Sheet : {SHEET_ID}")
    print(f"{'='*60}\n")

    # ── Fetch orders ──────────────────────────────────────────────
    orders = fetch_orders(start, end)
    if not orders:
        print("  No orders found for this period. Check token and dates.")
        return

    # ── Connect to Sheets ─────────────────────────────────────────
    gc = get_gc()
    sh = gc.open_by_key(SHEET_ID)

    # ── TAB 1: Frequency distribution ────────────────────────────
    print("  Building frequency distribution...")
    dist_rows, total_cust, total_ord = build_distribution(orders, label, now_str)
    upsert_tab(sh, "freq_distribution",
        headers=["updated_at","period","frequency_bucket",
                 "customers","pct_customers",
                 "net_sales","pct_revenue",
                 "orders","avg_order_value"],
        new_rows=dist_rows,
        key_cols=["period","frequency_bucket"])

    # ── TAB 2: Monthly active customers ──────────────────────────
    print("  Building monthly active customers...")
    monthly_rows = build_monthly(orders, year, now_str)
    upsert_tab(sh, "freq_monthly",
        headers=["updated_at","year","month",
                 "total_active_customers","new_customers","returning_customers",
                 "total_orders","net_sales",
                 "avg_orders_per_customer","avg_revenue_per_customer"],
        new_rows=monthly_rows,
        key_cols=["year","month"])

    # ── TAB 3: By channel ─────────────────────────────────────────
    print("  Building frequency by channel...")
    ch_rows = build_by_channel(orders, label, now_str)
    upsert_tab(sh, "freq_segments",
        headers=["updated_at","period","channel","frequency_bucket",
                 "customers","pct_of_channel","net_sales",
                 "total_channel_customers","total_channel_orders",
                 "avg_freq","avg_revenue_per_customer"],
        new_rows=ch_rows,
        key_cols=["period","channel","frequency_bucket"])

    # ── TAB 4: Cohort retention ───────────────────────────────────
    if not args.skip_cohort:
        print("  Building cohort retention...")
        # Collect all real customer IDs from the year's orders
        real_cids = list({
            str(o.get("customer", {}).get("id", ""))
            for o in orders
            if o.get("customer", {}).get("id")
        })
        history = fetch_customer_order_history(real_cids)
        cohort_rows = build_cohort(orders, year, now_str, history)
        upsert_tab(sh, "freq_cohort",
            headers=["updated_at","year","cohort","cohort_start","cohort_end",
                     "total_customers",
                     "returned_3m","returned_6m","returned_12m",
                     "retention_rate_3m","retention_rate_6m","retention_rate_12m"],
            new_rows=cohort_rows,
            key_cols=["year","cohort"])
    else:
        print("  Skipping cohort (--skip-cohort flag set)")

    # ── Summary ───────────────────────────────────────────────────
    avg_freq = round(total_ord / total_cust, 2) if total_cust else 0
    print(f"\n{'='*60}")
    print(f"  DONE — Year {year}")
    print(f"  Unique customers : {total_cust}")
    print(f"  Total orders     : {total_ord}")
    print(f"  Avg freq/customer: {avg_freq}  (Ceci mentioned ~2.8)")
    print(f"  Sheet            : {SHEET_ID}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
