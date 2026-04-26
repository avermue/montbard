#!/usr/bin/env python3
"""
collect_data.py
===============
Collecte les données ouvertes pour le dashboard territorial Montbard.

Communes :
  Montbard (21425), Venarey-les-Laumes (21663),
  Semur-en-Auxois (21603), Châtillon-sur-Seine (21154)

Sources (toutes publiques, aucune donnée en dur) :
  - INSEE Melodi ZIP  : populations 1968-2023 (sans auth)
  - DGFiP data.gouv   : finances communes 2000-2016 (streaming CSV 100MB)
  - OFGL Opendatasoft : finances communes 2017-2024 (API JSON, cbudg=1)

Usage :
  python3 collect_data.py           # collecte tout → data.json
  python3 collect_data.py --force   # vide le cache et re-télécharge

Dépendances :
  pip install requests pandas
"""

import argparse
import hashlib
import io
import json
import sys
import time
import zipfile
from pathlib import Path

import requests
import pandas as pd

# ── Configuration ────────────────────────────────────────────────────────────

COMMUNES = {
    "21425": "Montbard",
    "21663": "Venarey-les-Laumes",
    "21603": "Semur-en-Auxois",
    "21154": "Châtillon-sur-Seine",
}

OUTPUT_FILE = Path("data.json")
CACHE_DIR   = Path(".cache")

# ── Helpers ──────────────────────────────────────────────────────────────────

def log(msg, level="INFO"):
    colors = {"INFO": "\033[94m", "OK": "\033[92m", "WARN": "\033[93m", "ERR": "\033[91m"}
    print(f"{colors.get(level,'')  }[{level}]\033[0m {msg}", flush=True)

def abort(msg):
    log(msg, "ERR")
    sys.exit(1)

def cache_path(key: str) -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    return CACHE_DIR / (hashlib.md5(key.encode()).hexdigest() + ".cache")

def fetch_bytes(url: str, force=False) -> bytes:
    cp = cache_path(url)
    if cp.exists() and not force:
        log(f"  cache → {url[:70]}")
        return cp.read_bytes()
    log(f"  GET   → {url[:70]}")
    r = requests.get(url, timeout=120)
    if not r.ok:
        abort(f"HTTP {r.status_code} pour {url}")
    cp.write_bytes(r.content)
    return r.content

def fetch_json_api(url: str, params: dict, force=False) -> dict:
    key = url + json.dumps(params, sort_keys=True)
    cp = cache_path(key)
    if cp.exists() and not force:
        return json.loads(cp.read_text())
    log(f"  GET   → {url[:60]} {params}")
    r = requests.get(url, params=params, timeout=30)
    if not r.ok:
        abort(f"HTTP {r.status_code} pour {url}")
    cp.write_text(r.text)
    return r.json()

def safe_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

# ── 1. Population (INSEE Melodi ZIP, sans auth) ───────────────────────────────

def collect_population(force=False) -> dict:
    log("Population — INSEE Melodi DS_POPULATIONS_HISTORIQUES")
    url = "https://api.insee.fr/melodi/file/DS_POPULATIONS_HISTORIQUES/DS_POPULATIONS_HISTORIQUES_CSV_FR"
    raw = fetch_bytes(url, force)

    z = zipfile.ZipFile(io.BytesIO(raw))
    data_file = next(n for n in z.namelist() if "data.csv" in n and "metadata" not in n)

    found = {c: {} for c in COMMUNES}
    with z.open(data_file) as f:
        next(f)  # header
        for line in f:
            parts = line.decode("utf-8").strip().split(";")
            if len(parts) < 6:
                continue
            geo    = parts[1].strip('"')
            mesure = parts[3].strip('"')
            year   = parts[4].strip('"')
            value  = parts[5].strip('"')
            if geo in found and mesure == "PMUN":
                try:
                    found[geo][int(year)] = int(float(value))
                except ValueError:
                    pass

    results = {}
    for code, nom in COMMUNES.items():
        series = sorted(
            [{"year": y, "value": v} for y, v in found[code].items()],
            key=lambda x: x["year"]
        )
        if not series:
            abort(f"Aucune donnée population pour {nom} ({code})")
        log(f"  {nom:25s}: {len(series)} points ({series[0]['year']}–{series[-1]['year']})", "OK")
        results[code] = {"commune": nom, "series": series}

    return results

# ── 2. Finances DGFiP 2000-2016 (streaming CSV 100MB) ────────────────────────

def collect_finances_dgfip(force=False) -> dict:
    log("Finances DGFiP — comptes communes 2000-2016 (streaming)")
    url = "https://static.data.gouv.fr/resources/comptes-individuels-des-communes/20181019-174552/comptes-communes-2000-2017.csv"

    codes_csv = {f'"{c}"': c for c in COMMUNES}

    COLS = [
        "annee", "depcom", "population",
        "produits_total", "charges_total",
        "cap_autofinancement", "dette_encours_total",
        "invest_empl_equipements", "invest_emplois_total",
        "prod_impots_locaux", "charges_personnel", "dette_annuite",
    ]

    raw_rows = {c: [] for c in COMMUNES}
    header   = None
    idx      = {}
    buffer   = ""
    n        = 0

    # On streame sans cacher (100MB)
    log(f"  streaming {url[:70]}...")
    r = requests.get(url, stream=True, timeout=120)
    if not r.ok:
        abort(f"HTTP {r.status_code} DGFiP")

    for chunk in r.iter_content(chunk_size=65536, decode_unicode=True):
        buffer += chunk
        lines   = buffer.split("\n")
        buffer  = lines[-1]

        for line in lines[:-1]:
            if not line.strip():
                continue
            n += 1
            if n == 1:
                header = line.split(",")
                for col in COLS:
                    idx[col] = header.index(col) if col in header else None
                log(f"  en-tête OK — {len(header)} colonnes", "OK")
                continue

            cols = line.split(",")
            if idx["depcom"] is None or len(cols) <= idx["depcom"]:
                continue
            code_csv = cols[idx["depcom"]]
            if code_csv not in codes_csv:
                continue
            raw_rows[codes_csv[code_csv]].append(cols)

        if all(len(v) >= 17 for v in raw_rows.values()):
            log(f"  4 communes complètes après {n:,} lignes", "OK")
            break

    results = {}
    for code, nom in COMMUNES.items():
        rows = raw_rows[code]
        if not rows:
            abort(f"Aucune donnée DGFiP pour {nom}")

        def g(cols, col):
            i = idx.get(col)
            if i is None or i >= len(cols):
                return None
            v = cols[i].strip().strip('"')
            f = safe_float(v)
            return round(f * 1000) if f is not None else None  # k€ → €

        def gi(cols, col):
            i = idx.get(col)
            if i is None or i >= len(cols):
                return None
            v = cols[i].strip().strip('"')
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None

        series = []
        for cols in sorted(rows, key=lambda c: c[idx["annee"]]):
            year_str = cols[idx["annee"]].strip().strip('"')
            try:
                year = int(year_str)
            except ValueError:
                continue
            if year >= 2017:          # OFGL prend le relais
                continue
            series.append({
                "year":            year,
                "recettes_fonct":  g(cols, "produits_total"),
                "depenses_fonct":  g(cols, "charges_total"),
                "epargne_brute":   g(cols, "cap_autofinancement"),
                "dette":           g(cols, "dette_encours_total"),
                "equipement":      g(cols, "invest_empl_equipements"),
                "frais_personnel": g(cols, "charges_personnel"),
                "impots_locaux":   g(cols, "prod_impots_locaux"),
                "annuite_dette":   g(cols, "dette_annuite"),
                "depenses_invest": g(cols, "invest_emplois_total"),
                "dgf":             None,
                "pop_dgf":         gi(cols, "population"),
            })

        if not series:
            abort(f"Aucune ligne valide DGFiP pour {nom}")

        results[code] = {"commune": nom, "series": series}
        log(f"  {nom:25s}: {len(series)} années ({series[0]['year']}–{series[-1]['year']})", "OK")

    return results

# ── 3. Finances OFGL 2017-2024 (API JSON paginée, cbudg=1) ───────────────────

def collect_finances_ofgl(force=False) -> dict:
    log("Finances OFGL — 2017-2024 (budget principal, pivot agrégats)")

    base_url = "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes/records"

    AGREGATS = {
        "Recettes de fonctionnement":        "recettes_fonct",
        "Dépenses de fonctionnement":        "depenses_fonct",
        "Epargne brute":                     "epargne_brute",
        "Encours de dette":                  "dette",
        "Dépenses d'équipement":             "equipement",
        "Frais de personnel":                "frais_personnel",
        "Impôts locaux":                     "impots_locaux",
        "Dotation globale de fonctionnement":"dgf",
        "Annuité de la dette":               "annuite_dette",
        "Dépenses d'investissement":         "depenses_invest",
    }
    where_agregats = " OR ".join(f'agregat="{a}"' for a in AGREGATS)

    results = {}
    for code, nom in COMMUNES.items():
        all_recs = []
        offset   = 0
        total    = None

        while total is None or len(all_recs) < total:
            params = {
                "where":    f'insee="{code}" AND cbudg=1 AND ({where_agregats})',
                "limit":    100,
                "offset":   offset,
                "select":   "agregat,exer,montant,ptot_n",
                "order_by": "exer,agregat",
            }
            data = fetch_json_api(base_url, params, force)
            total = data.get("total_count", 0)
            all_recs.extend(data.get("results", []))
            offset += 100
            if len(all_recs) >= total:
                break
            time.sleep(0.3)

        if not all_recs:
            abort(f"Aucun enregistrement OFGL pour {nom}")

        pivot = {}
        for rec in all_recs:
            year    = rec.get("exer")
            agregat = rec.get("agregat", "")
            montant = safe_float(rec.get("montant"))
            ptot_n  = safe_float(rec.get("ptot_n"))
            if not year:
                continue
            year = int(year)
            pivot.setdefault(year, {"pop_dgf": None})
            if ptot_n and pivot[year]["pop_dgf"] is None:
                pivot[year]["pop_dgf"] = int(ptot_n)
            if agregat in AGREGATS and montant is not None:
                pivot[year][AGREGATS[agregat]] = round(montant)

        series = []
        for year in sorted(pivot):
            row = pivot[year]
            if row.get("recettes_fonct") is None:
                continue
            series.append({
                "year":            year,
                "recettes_fonct":  row.get("recettes_fonct"),
                "depenses_fonct":  row.get("depenses_fonct"),
                "epargne_brute":   row.get("epargne_brute"),
                "dette":           row.get("dette"),
                "equipement":      row.get("equipement"),
                "frais_personnel": row.get("frais_personnel"),
                "impots_locaux":   row.get("impots_locaux"),
                "dgf":             row.get("dgf"),
                "annuite_dette":   row.get("annuite_dette"),
                "depenses_invest": row.get("depenses_invest"),
                "pop_dgf":         row.get("pop_dgf"),
            })

        if not series:
            abort(f"Aucune série valide OFGL pour {nom}")

        results[code] = {"commune": nom, "series": series}
        log(f"  {nom:25s}: {len(series)} années OFGL ({series[0]['year']}–{series[-1]['year']})", "OK")

    return results

# ── Main ──────────────────────────────────────────────────────────────────────

def main(force=False):
    if force:
        log("Nettoyage du cache…")
        for f in CACHE_DIR.glob("*.cache"):
            f.unlink()

    log("=" * 60)
    log("Observatoire Montbard — Collecte des données")
    log("=" * 60)

    # Population
    population = collect_population(force)

    # Finances : DGFiP 2000-2016 + OFGL 2017-2024
    fin_dgfip = collect_finances_dgfip(force)
    fin_ofgl  = collect_finances_ofgl(force)

    log("Fusion finances DGFiP + OFGL…")
    finances = {}
    for code, nom in COMMUNES.items():
        merged = sorted(
            fin_dgfip[code]["series"] + fin_ofgl[code]["series"],
            key=lambda s: s["year"]
        )
        finances[code] = {"commune": nom, "series": merged}
        log(f"  {nom:25s}: {len(merged)} années ({merged[0]['year']}–{merged[-1]['year']})", "OK")

    output = {
        "meta": {
            "communes":     COMMUNES,
            "collect_date": pd.Timestamp.now().isoformat(),
            "sources": {
                "population": "INSEE Melodi DS_POPULATIONS_HISTORIQUES (ZIP public, PMUN 1968-2023)",
                "finances":   "DGFiP data.gouv.fr 2000-2016 + OFGL 2017-2024 (cbudg=1)",
            },
        },
        "population": population,
        "finances":   finances,
    }

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    log("=" * 60)
    log(f"✓ {OUTPUT_FILE.absolute()}  ({size_kb:.1f} KB)", "OK")
    log("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Vider le cache et re-télécharger")
    args = parser.parse_args()
    main(force=args.force)
