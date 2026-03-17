#!/usr/bin/env python3
import os, re, time, logging
from datetime import date, timedelta
import requests
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
H = {"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Content-Type":"application/json","Prefer":"resolution=merge-duplicates"}
ESTACIONES = {
    "metro":[(-34.84,-56.01),(-34.85,-56.21)],
    "norte":[(-30.40,-56.51),(-30.91,-55.55),(-31.38,-57.97)],
    "sur":  [(-34.46,-57.84),(-33.38,-56.52),(-33.25,-58.08)],
    "este": [(-34.48,-54.33),(-32.37,-54.17),(-34.91,-54.96)],
    "oeste":[(-32.32,-58.08),(-32.69,-57.63)],
}
MESES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,"julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}
logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def upsert(table, rows):
    if not rows: return 0
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}",headers=H,json=rows,timeout=30)
    if not r.ok: log.error("Supabase error %s
cat > scraper/nubel_scraper.py << 'ENDOFFILE'
#!/usr/bin/env python3
import os, re, time, logging
from datetime import date, timedelta
import requests
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
H = {"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Content-Type":"application/json","Prefer":"resolution=merge-duplicates"}
ESTACIONES = {
    "metro":[(-34.84,-56.01),(-34.85,-56.21)],
    "norte":[(-30.40,-56.51),(-30.91,-55.55),(-31.38,-57.97)],
    "sur":  [(-34.46,-57.84),(-33.38,-56.52),(-33.25,-58.08)],
    "este": [(-34.48,-54.33),(-32.37,-54.17),(-34.91,-54.96)],
    "oeste":[(-32.32,-58.08),(-32.69,-57.63)],
}
MESES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,"julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}
logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

def upsert(table, rows):
    if not rows: return 0
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}",headers=H,json=rows,timeout=30)
    if not r.ok: log.error("Supabase error %s: %s",r.status_code,r.text[:200])
    return len(rows) if r.ok else 0

def log_run(estado,pron,reales,detalle=""):
    requests.post(f"{SUPABASE_URL}/rest/v1/scraper_log",headers=H,json=[{"estado":estado,"pronosticos_insertados":pron,"reales_insertados":reales,"detalle":detalle[:500]}],timeout=15)

def get_article_urls():
    try:
        r = requests.get("https://www.subrayado.com.uy/nubel-cisneros-a12869",timeout=15,headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text,"html.parser")
        urls = []
        for a in soup.select("a[href*='-n']"):
            href = a.get("href","")
            if not href: continue
            full = href if href.startswith("http") else f"https://www.subrayado.com.uy{href}"
            if full not in urls: urls.append(full)
            if len(urls)>=10: break
        return urls[:5]
    except Exception as e:
        log.error("Error index: %s",e); return []

def parse_article(url):
    try:
        r = requests.get(url,timeout=15,headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text,"html.parser")
        text = soup.get_text()
        m = re.search(r"(\d{1,2}) de (\w+) de (\d{4})",text)
        if not m: return []
        d,mes,y = m.groups()
        mn = MESES.get(mes.lower())
        if not mn: return []
        fpub = date(int(y),mn,int(d))
        zona_map = {"norte":"norte","sur":"sur","este":"este","oeste":"oeste","metropolitana":"metro"}
        entries = []
        bloques = re.split(r"Temperaturas para el \w+ (\d+)[:\.]",text)
        for i in range(1,len(bloques),2):
            try:
                dia_num = int(bloques[i])
                bloque = bloques[i+1][:600]
                try:
                    fdia = date(fpub.year,fpub.month,dia_num)
                    if fdia < fpub:
                        nm = fpub.month+1 if fpub.month<12 else 1
                        ny = fpub.year if fpub.month<12 else fpub.year+1
                        fdia = date(ny,nm,dia_num)
                except: continue
                for linea in bloque.split("\n"):
                    l = linea.strip().lower()
                    for zk,zv in zona_map.items():
                        if zk in l:
                            nums = re.findall(r"(\d+)[°º]",l)
                            if len(nums)>=2:
                                entries.append({"fecha_dia":fdia.isoformat(),"fecha_pronostico":fpub.isoformat(),"zona":zv,"max_nubel":int(nums[0]),"min_nubel":int(nums[1]),"url_fuente":url})
            except: continue
        if not entries:
            for zk,zv in zona_map.items():
                pat = rf"{zk}[^\d]{{0,80}}(?:m[aá]x|max)[^\d]{{0,20}}(\d+)[°º][^\d]{{0,80}}(?:m[ií]n|min)[^\d]{{0,20}}(\d+)[°º]"
                ms = re.findall(pat,text,re.IGNORECASE)
                if ms:
                    entries.append({"fecha_dia":fpub.isoformat(),"fecha_pronostico":fpub.isoformat(),"zona":zv,"max_nubel":int(ms[0][0]),"min_nubel":int(ms[0][1]),"url_fuente":url})
        return entries
    except Exception as e:
        log.error("Error parse %s: %s",url,e); return []

def fetch_real(zona,fecha):
    maxs,mins = [],[]
    for lat,lon in ESTACIONES[zona]:
        try:
            url = (f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}"
                   f"&start_date={fecha}&end_date={fecha}&daily=temperature_2m_max,temperature_2m_min&timezone=America%2FMontevideo")
            r = requests.get(url,timeout=20)
            if r.ok:
                d = r.json()["daily"]
                if d["temperature_2m_max"][0]: maxs.append(d["temperature_2m_max"][0])
                if d["temperature_2m_min"][0]: mins.append(d["temperature_2m_min"][0])
        except: pass
        time.sleep(0.3)
    if not maxs: return None
    return {"fecha":fecha,"zona":zona,"max_real":round(sum(maxs)/len(maxs),1),"min_real":round(sum(mins)/len(mins),1) if mins else None,"fuente":"Open-Meteo/INUMET"}

def main():
    today = date.today()
    log.info("=== Scraper %s ===",today)
    pron_total = reales_total = 0
    urls = get_article_urls()
    log.info("%d articulos encontrados",len(urls))
    all_pron = []
    for url in urls:
        entries = parse_article(url)
        log.info("  %s -> %d entradas",url[-50:],len(entries))
        all_pron.extend(entries)
        time.sleep(1)
    if all_pron:
        pron_total = upsert("nubel_pronosticos",all_pron)
        log.info("Pronosticos guardados: %d",pron_total)
    for delta in [2,1]:
        target = (today-timedelta(days=delta)).isoformat()
        log.info("Fetching reales %s...",target)
        rows = [x for z in ESTACIONES if (x:=fetch_real(z,target))]
        if rows:
            n = upsert("datos_reales",rows)
            reales_total += n
            log.info("  %d filas",n)
    log_run("ok",pron_total,reales_total,f"{len(urls)} articulos procesados")
    log.info("=== Fin. Pron: %d | Reales: %d ===",pron_total,reales_total)

if __name__=="__main__":
    main()
