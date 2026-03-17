#!/usr/bin/env python3
"""
nubel_scraper.py - Nubel Tracker Scraper
Modo normal: cron diario (sin argumentos)
Modo backfill: python nubel_scraper.py --backfill [--desde 2026-01-01] [--hasta 2026-03-16]
"""

import os
import re
import time
import logging
import argparse
from datetime import date, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

HEADERS_SUPA = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

ZONAS_ESTACIONES = {
    "metro": [(-34.84, -56.01), (-34.85, -56.21)],
    "norte": [(-30.40, -56.51), (-30.91, -55.55), (-31.38, -57.97)],
    "sur":   [(-34.46, -57.84), (-33.38, -56.52), (-33.25, -58.08)],
    "este":  [(-34.48, -54.33), (-32.37, -54.17), (-34.91, -54.96)],
    "oeste": [(-32.32, -58.08), (-32.69, -57.63)],
}

SUBRAYADO_TAG = "https://www.subrayado.com.uy/nubel-cisneros-a12869"
SUBRAYADO_TAG_PAGE = "https://www.subrayado.com.uy/nubel-cisneros-a12869?page={page}"

MESES = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def supa_upsert(table: str, rows: list) -> int:
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    res = requests.post(url, headers=HEADERS_SUPA, json=rows, timeout=30)
    if not res.ok:
        log.error("Supabase upsert error %s: %s", res.status_code, res.text[:200])
        return 0
    return len(rows)

def supa_log(estado, pron, reales, detalle=""):
    url = f"{SUPABASE_URL}/rest/v1/scraper_log"
    requests.post(url, headers=HEADERS_SUPA, json=[{
        "estado": estado, "pronosticos_insertados": pron,
        "reales_insertados": reales, "detalle": detalle[:1000],
    }], timeout=15)

def supa_get_existing_dates(table, date_col) -> set:
    headers = {k: v for k, v in HEADERS_SUPA.items() if k != "Prefer"}
    res = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?select={date_col}", headers=headers, timeout=30)
    if not res.ok:
        return set()
    return {r[date_col] for r in res.json() if r.get(date_col)}


def fetch_article_urls_from_page(page=1) -> list:
    try:
        url = SUBRAYADO_TAG if page == 1 else SUBRAYADO_TAG_PAGE.format(page=page)
        res = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0 NubelTracker/1.0"})
        if res.status_code == 404:
            return []
        soup = BeautifulSoup(res.text, "html.parser")
        articles = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            if re.search(r"-n\d{5,}", href):
                keywords = ["temperatura","máxima","mínima","calor","frío","pronóstico",
                            "tiempo","domingo","lunes","martes","miércoles","jueves","viernes","sábado"]
                if any(k in text for k in keywords) or any(k in href.lower() for k in ["temperatura","pronostico"]):
                    full = href if href.startswith("http") else f"https://www.subrayado.com.uy{href}"
                    if full not in [x["url"] for x in articles]:
                        articles.append({"url": full, "title": a.get_text(strip=True)})
        return articles
    except Exception as e:
        log.error("Error fetching page %d: %s", page, e)
        return []

def fetch_all_article_urls_backfill() -> list:
    all_articles = []
    page = 1
    max_pages = 50
    while page <= max_pages:
        log.info("  Scrapeando pagina %d...", page)
        articles = fetch_article_urls_from_page(page)
        if not articles:
            break
        new_found = sum(1 for a in articles if a["url"] not in [x["url"] for x in all_articles])
        all_articles.extend([a for a in articles if a["url"] not in [x["url"] for x in all_articles]])
        log.info("  -> %d articulos nuevos (total: %d)", new_found, len(all_articles))
        if new_found == 0:
            break
        page += 1
        time.sleep(1.5)
    return all_articles

def parse_temperatures_from_article(url: str) -> Optional[dict]:
    try:
        res = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0 NubelTracker/1.0"})
        if not res.ok:
            return None
        soup = BeautifulSoup(res.text, "html.parser")
        text = soup.get_text()
        date_match = re.search(r"(\d{1,2}) de (\w+) de (\d{4})", text)
        if not date_match:
            return None
        d, m_str, y = date_match.groups()
        mes_num = MESES.get(m_str.lower())
        if not mes_num:
            return None
        fecha_pub = date(int(y), mes_num, int(d))
        ZONA_MAX = [
            ("norte", r"(?:norte|NORTE)[^\d]*?(?:max|MAX|maxima|máxima)[^\d]*?(\d+)[°º]"),
            ("sur",   r"(?:sur|SUR)[^\d]*?(?:max|MAX|maxima|máxima)[^\d]*?(\d+)[°º]"),
            ("este",  r"(?:este|ESTE)[^\d]*?(?:max|MAX|maxima|máxima)[^\d]*?(\d+)[°º]"),
            ("oeste", r"(?:oeste|OESTE)[^\d]*?(?:max|MAX|maxima|máxima)[^\d]*?(\d+)[°º]"),
            ("metro", r"(?:metropolitana|METROPOLITANA|metro|METRO)[^\d]*?(?:max|MAX|maxima|máxima)[^\d]*?(\d+)[°º]"),
        ]
        ZONA_MIN = [
            ("norte", r"(?:norte|NORTE)[^\d]*?(?:min|MIN|minima|mínima)[^\d]*?(\d+)[°º]"),
            ("sur",   r"(?:sur|SUR)[^\d]*?(?:min|MIN|minima|mínima)[^\d]*?(\d+)[°º]"),
            ("este",  r"(?:este|ESTE)[^\d]*?(?:min|MIN|minima|mínima)[^\d]*?(\d+)[°º]"),
            ("oeste", r"(?:oeste|OESTE)[^\d]*?(?:min|MIN|minima|mínima)[^\d]*?(\d+)[°º]"),
            ("metro", r"(?:metropolitana|METROPOLITANA|metro|METRO)[^\d]*?(?:min|MIN|minima|mínima)[^\d]*?(\d+)[°º]"),
        ]
        zona_data = {}
        for zona, pat in ZONA_MAX:
            m = re.findall(pat, text, re.IGNORECASE)
            if m:
                zona_data.setdefault(zona, {})["max"] = int(m[0])
        for zona, pat in ZONA_MIN:
            m = re.findall(pat, text, re.IGNORECASE)
            if m:
                zona_data.setdefault(zona, {})["min"] = int(m[0])
        if not zona_data or len(zona_data) < 3:
            return None
        mentioned_days = re.findall(
            r"(?:domingo|lunes|martes|miercoles|miércoles|jueves|viernes|sabado|sábado)\s+(\d+)",
            text, re.IGNORECASE
        )
        target_days = []
        for day_str in mentioned_days[:3]:
            day_num = int(day_str)
            try:
                target = date(fecha_pub.year, fecha_pub.month, day_num)
                if target < fecha_pub - timedelta(days=1):
                    next_month = fecha_pub.month + 1 if fecha_pub.month < 12 else 1
                    next_year = fecha_pub.year if fecha_pub.month < 12 else fecha_pub.year + 1
                    target = date(next_year, next_month, day_num)
                target_days.append(target)
            except ValueError:
                pass
        if not target_days:
            target_days = [fecha_pub]
        fecha_dia = target_days[0]
        entries = []
        for zona, vals in zona_data.items():
            if "max" in vals:
                entries.append({
                    "fecha_dia":        fecha_dia.isoformat(),
                    "fecha_pronostico": fecha_pub.isoformat(),
                    "zona":             zona,
                    "max_nubel":        vals.get("max"),
                    "min_nubel":        vals.get("min"),
                    "url_fuente":       url,
                })
        return {"fecha_pub": fecha_pub, "entries": entries} if entries else None
    except Exception as e:
        log.error("Error parsing %s: %s", url, e)
        return None


def fetch_real_zona(zona, fecha) -> Optional[dict]:
    estaciones = ZONAS_ESTACIONES.get(zona, [])
    maxs, mins = [], []
    for lat, lon in estaciones:
        fecha_str = fecha.isoformat()
        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={fecha_str}&end_date={fecha_str}"
            f"&daily=temperature_2m_max,temperature_2m_min"
            f"&timezone=America%2FMontevideo"
        )
        try:
            r = requests.get(url, timeout=20)
            if r.ok:
                d = r.json()
                mx = d["daily"]["temperature_2m_max"][0]
                mn = d["daily"]["temperature_2m_min"][0]
                if mx is not None: maxs.append(mx)
                if mn is not None: mins.append(mn)
        except Exception as e:
            log.warning("Open-Meteo error %s %s: %s", zona, fecha_str, e)
        time.sleep(0.3)
    if not maxs:
        return None
    return {
        "fecha":    fecha.isoformat(),
        "zona":     zona,
        "max_real": round(sum(maxs)/len(maxs), 1),
        "min_real": round(sum(mins)/len(mins), 1) if mins else None,
        "fuente":   "Open-Meteo/INUMET",
    }

def fetch_reales_rango(desde, hasta, existentes) -> int:
    total = 0
    today = date.today()
    current = desde
    while current <= hasta:
        fecha_str = current.isoformat()
        if fecha_str in existentes:
            current += timedelta(days=1)
            continue
        if (today - current).days < 2:
            current += timedelta(days=1)
            continue
        log.info("  Fetching datos reales para %s...", fecha_str)
        reales = [fetch_real_zona(z, current) for z in ZONAS_ESTACIONES]
        reales = [r for r in reales if r]
        if reales:
            n = supa_upsert("datos_reales", reales)
            total += n
            log.info("  -> %d zonas guardadas", n)
        current += timedelta(days=1)
        time.sleep(1)
    return total


def run_backfill(desde, hasta):
    log.info("=== MODO BACKFILL: %s -> %s ===", desde, hasta)
    pronosticos_total = 0
    fechas_pron_existentes = supa_get_existing_dates("nubel_pronosticos", "fecha_dia")
    fechas_reales_existentes = supa_get_existing_dates("datos_reales", "fecha")
    log.info("Pronosticos existentes: %d | Reales existentes: %d",
             len(fechas_pron_existentes), len(fechas_reales_existentes))
    all_article_urls = fetch_all_article_urls_backfill()
    all_pronosticos = []
    for art in all_article_urls:
        log.info("Parseando: %s", art["url"][:80])
        result = parse_temperatures_from_article(art["url"])
        if result and result["entries"]:
            fecha_dia = result["entries"][0]["fecha_dia"]
            if fecha_dia in fechas_pron_existentes:
                log.info("  -> %s ya existe, saltando.", fecha_dia)
            else:
                log.info("  -> %d entradas para %s", len(result["entries"]), fecha_dia)
                all_pronosticos.extend(result["entries"])
        else:
            log.info("  -> Sin datos de temperatura por zona")
        time.sleep(1.5)
    if all_pronosticos:
        pronosticos_total = supa_upsert("nubel_pronosticos", all_pronosticos)
        log.info("Pronosticos guardados: %d filas", pronosticos_total)
    reales_total = fetch_reales_rango(desde, hasta, fechas_reales_existentes)
    log.info("Datos reales guardados: %d filas", reales_total)
    detalle = f"Backfill {desde}->{hasta}: {len(all_article_urls)} articulos procesados"
    supa_log("backfill_ok", pronosticos_total, reales_total, detalle)
    log.info("=== Backfill completo. Pronosticos: %d | Reales: %d ===", pronosticos_total, reales_total)


def run():
    today = date.today()
    log.info("=== Nubel Tracker Scraper - %s ===", today.isoformat())
    pronosticos_total = 0
    reales_total = 0
    article_urls = fetch_article_urls_from_page(1)[:5]
    log.info("Encontrados %d articulos", len(article_urls))
    all_pronosticos = []
    for art in article_urls:
        result = parse_temperatures_from_article(art["url"])
        if result and result["entries"]:
            all_pronosticos.extend(result["entries"])
        time.sleep(1)
    if all_pronosticos:
        pronosticos_total = supa_upsert("nubel_pronosticos", all_pronosticos)
    for target_date in [today - timedelta(days=2), today - timedelta(days=1)]:
        reales = [fetch_real_zona(z, target_date) for z in ZONAS_ESTACIONES]
        reales = [r for r in reales if r]
        if reales:
            reales_total += supa_upsert("datos_reales", reales)
    supa_log("ok", pronosticos_total, reales_total, f"OK - {len(article_urls)} articulos procesados")
    log.info("=== Fin. Pronosticos: %d | Reales: %d ===", pronosticos_total, reales_total)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--desde", type=str, default="2026-01-01")
    parser.add_argument("--hasta", type=str, default=None)
    args = parser.parse_args()
    if args.backfill:
        desde = date.fromisoformat(args.desde)
        hasta = date.fromisoformat(args.hasta) if args.hasta else date.today() - timedelta(days=1)
        run_backfill(desde, hasta)
    else:
        run()