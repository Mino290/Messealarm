import os
import re
import time
import requests
import smtplib
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ====== Konfiguration ======
SUPABASE_URL = "https://bunrrjnxcdsstpkvldhj.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJ1bnJyam54Y2Rzc3Rwa3ZsZGhqIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDA1Nzc1NiwiZXhwIjoyMDY5NjMzNzU2fQ.Y-lCtfIIEal5MVRtCe5ZWke_RqR3X40M2vMQVvTP3Dc"

# Hier SMTP-Daten direkt eintragen
SMTP_USER = "messen.infos@gmail.com"
SMTP_PASS = "gzao nytx exxb hczk"  # App-spezifisches Passwort

AUMA_API_URL = "https://www.auma.de/api/TradeFairData/getWebOverviewTradeFairData"
DETAIL_URL_FMT = "https://www.auma.de/messen-finden/details/?tfd={url_param}"

# Filter: Deutschland, alle Monate, mehrere Jahre
FILTERS = {
    "intFilterYearFrom": 2025,
    "intFilterYearTo": 2032,
    "intFilterMonthFrom": 1,
    "intFilterMonthTo": 12,
    "strLanguage": "de",
    "intSort": 1,
    "strSearchString": "Deutschland"
}

# ====== Hilfsfunktionen ======
def tz_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_datum(date_string):
    if not date_string:
        return None, None
    date_string = date_string.replace("–", "-").replace("bis", "-")
    match = re.findall(r"\d{1,2}\.\d{1,2}\.\d{4}", date_string)
    if len(match) == 2:
        start = datetime.strptime(match[0], "%d.%m.%Y").date()
        end = datetime.strptime(match[1], "%d.%m.%Y").date()
        return start.isoformat(), end.isoformat()
    if len(match) == 1:
        start = datetime.strptime(match[0], "%d.%m.%Y").date()
        return start.isoformat(), start.isoformat()
    return None, None

def normalize_city(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip().lower()

# ====== AUMA Scraping ======
def fetch_auma_messen_de() -> List[Dict[str, Any]]:
    headers = {"Accept": "application/json"}
    seite = 1
    result: List[Dict[str, Any]] = []
    seen_ids = set()

    while True:
        params = {**FILTERS, "intSeitenZahl": seite}
        resp = requests.get(AUMA_API_URL, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        daten = resp.json()

        if not daten:
            break

        for messe in daten:
            mid = messe.get("strMesseTerminKey")
            if not mid or mid in seen_ids:
                continue
            seen_ids.add(mid)

            titel = messe.get("strTitel")
            stadt = messe.get("strStadt")
            land = messe.get("strLand")
            termin = messe.get("strTermin")
            url_param = messe.get("strUrlParameter")
            kategorie = messe.get("strKategorie")

            start_datum, end_datum = parse_datum(termin or "")
            url_full = (DETAIL_URL_FMT.format(url_param=url_param) if url_param else None)

            result.append({
                "id": mid,
                "titel": titel,
                "stadt": stadt,
                "land": land,
                "start_datum": start_datum,
                "end_datum": end_datum,
                "url_param": url_param,
                "url": url_full,
                "kategorie": kategorie,
                "erstellt_am": tz_now_iso()
            })

        seite += 1
        time.sleep(0.2)

    return result

# ====== Supabase Funktionen ======
def supabase_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def fetch_db_messen() -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/Messen?select=*"
    r = requests.get(url, headers=supabase_headers(), timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_abonnenten() -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/Abonnenten?select=*"
    r = requests.get(url, headers=supabase_headers(), timeout=60)
    r.raise_for_status()
    return r.json()

# ====== Vergleichslogik ======
def index_by_id(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {row["id"]: row for row in rows if "id" in row}

def dates_equal(a: Optional[str], b: Optional[str]) -> bool:
    return (a or "") == (b or "")

def diff_messen(api_rows: List[Dict[str, Any]], db_index: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []

    for cur in api_rows:
        mid = cur["id"]
        prev = db_index.get(mid)

        if prev is None:
            changes.append({
                "type": "new",
                "messe": cur,
                "before": None,
                "changed_fields": {"start_datum": cur["start_datum"], "end_datum": cur["end_datum"]}
            })
            continue

        prev_start = prev.get("start_datum")
        prev_end = prev.get("end_datum")
        cur_start = cur.get("start_datum")
        cur_end = cur.get("end_datum")

        date_added = False
        changed_fields = {}

        if (not prev_start and cur_start):
            date_added = True
            changed_fields["start_datum"] = cur_start
        if (not prev_end and cur_end):
            date_added = True
            changed_fields["end_datum"] = cur_end

        if date_added:
            changes.append({
                "type": "date_added",
                "messe": cur,
                "before": prev,
                "changed_fields": changed_fields
            })
            continue

        date_changed = False
        if (cur_start is not None and not dates_equal(prev_start, cur_start)):
            date_changed = True
            changed_fields["start_datum"] = cur_start
        if (cur_end is not None and not dates_equal(prev_end, cur_end)):
            date_changed = True
            changed_fields["end_datum"] = cur_end

        if date_changed:
            changes.append({
                "type": "date_changed",
                "messe": cur,
                "before": prev,
                "changed_fields": changed_fields
            })

    return changes

# ====== DB Updates ======
def upsert_new_messen(new_items: List[Dict[str, Any]]) -> None:
    if not new_items:
        return
    url = f"{SUPABASE_URL}/rest/v1/Messen"
    r = requests.post(
        url,
        headers={**supabase_headers(), "Prefer": "resolution=merge-duplicates"},
        json=new_items,
        timeout=120
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Upsert new failed: {r.status_code} {r.text}")

def patch_messe_dates(messe_id: str, changed_fields: Dict[str, Any]) -> None:
    if not changed_fields:
        return
    url = f"{SUPABASE_URL}/rest/v1/Messen?id=eq.{messe_id}"
    r = requests.patch(url, headers=supabase_headers(), json=changed_fields, timeout=60)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"Patch failed for {messe_id}: {r.status_code} {r.text}")

def apply_changes_to_db(changes: List[Dict[str, Any]]) -> None:
    new_items = [c["messe"] for c in changes if c["type"] == "new"]
    updates = [c for c in changes if c["type"] in ("date_added", "date_changed")]

    BATCH = 500
    for i in range(0, len(new_items), BATCH):
        upsert_new_messen(new_items[i:i+BATCH])
        time.sleep(0.2)

    for upd in updates:
        mid = upd["messe"]["id"]
        payload = {
            "titel": upd["messe"].get("titel"),
            "stadt": upd["messe"].get("stadt"),
            "land": upd["messe"].get("land"),
            "url_param": upd["messe"].get("url_param"),
            "url": upd["messe"].get("url"),
            "kategorie": upd["messe"].get("kategorie"),
            **upd["changed_fields"]
        }
        patch_messe_dates(mid, payload)
        time.sleep(0.1)

# ====== Benachrichtigungslogik ======
def parse_abonnent_staedt(eintrag: Dict[str, Any]) -> List[str]:
    if "staedte" in eintrag:
        v = eintrag["staedte"]
    elif "staedte_csv" in eintrag:
        v = eintrag["staedte_csv"]
    elif "stadt" in eintrag:
        v = eintrag["stadt"]
    else:
        v = None

    if v is None:
        return []
    if isinstance(v, list):
        return [normalize_city(x) for x in v if isinstance(x, str)]
    if isinstance(v, str):
        parts = [p.strip() for p in v.replace(";", ",").split(",") if p.strip()]
        return [normalize_city(p) for p in parts]
    return []

def build_notifications(changes: List[Dict[str, Any]], abonnenten: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    notifications = []
    city_map: Dict[str, List[Dict[str, Any]]] = {}
    
    for c in changes:
        city = normalize_city(c["messe"].get("stadt"))
        if city:
            city_map.setdefault(city, []).append(c)

    for ab in abonnenten:
        email = ab.get("email")
        name = ab.get("name")
        if not email or not name:
            continue
            
        cities = parse_abonnent_staedt(ab)
        if not cities:
            continue

        abo_changes = []
        for city in cities:
            abo_changes.extend(city_map.get(city, []))

        seen = set()
        unique_changes = []
        for item in abo_changes:
            mid = item["messe"]["id"]
            if mid not in seen:
                seen.add(mid)
                unique_changes.append(item)

        if unique_changes:
            notifications.append({
                "abonnent": {
                    "email": email,
                    "name": name,
                    "cities": cities
                },
                "changes": unique_changes
            })

    return notifications

# ====== E-Mail Templates ======
EMAIL_TEMPLATE = """
<html>
<body>
<p>Hallo {name},</p>

<p>Es gibt Neuigkeiten für Ihre abonnierten Städte {cities}:</p>

{events}

<p>Viele Grüße<br>
Mino</p>
</body>
</html>
"""

EVENT_TEMPLATE = """
<div style="margin-bottom: 20px; border-left: 3px solid #4CAF50; padding-left: 10px;">
    <h3 style="margin-top: 0; margin-bottom: 5px; color: #2c3e50;">{titel}</h3>
    <p style="margin: 5px 0; color: #7f8c8d;">
        <span style="color: #3498db;">{change_type}</span> | 
        <b>Termin:</b> {start_datum} - {end_datum}<br>
        <b>Ort:</b> {stadt}, {land}<br>
        <a href="{url}" target="_blank" style="color: #2980b9; text-decoration: none;">› Mehr Infos zu dieser Messe</a>
    </p>
</div>
"""

# ====== E-Mail Versand ======
def send_notifications(notifications: List[Dict[str, Any]]) -> None:
    type_translation = {
        "new": "Neue Messe",
        "date_added": "Termin hinzugefügt",
        "date_changed": "Termin aktualisiert"
    }
    
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)

        for notification in notifications:
            ab = notification["abonnent"]
            changes = notification["changes"]
            
            event_html = ""
            for change in changes:
                m = change["messe"]
                event_html += EVENT_TEMPLATE.format(
                    titel=m["titel"],
                    change_type=type_translation.get(change["type"], "Update"),
                    start_datum=m.get("start_datum") or "unbekannt",
                    end_datum=m.get("end_datum") or "unbekannt",
                    stadt=m.get("stadt") or "unbekannt",
                    land=m.get("land") or "unbekannt",
                    url=m.get("url") or "#"
                )
            
            cities_str = ", ".join([c.capitalize() for c in ab["cities"]])
            
            body_html = EMAIL_TEMPLATE.format(
                name=ab["name"],
                cities=cities_str,
                events=event_html
            )

            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"Neue Messen in {cities_str}"
            msg["From"] = SMTP_USER
            msg["To"] = ab["email"]
            msg.attach(MIMEText(body_html, "html"))

            try:
                server.sendmail(SMTP_USER, ab["email"], msg.as_string())
                print(f"✓ Benachrichtigung an {ab['email']} gesendet")
            except Exception as e:
                print(f"✗ Fehler beim Senden an {ab['email']}: {e}")
                # Optional: Fehler protokollieren

# ====== Hauptprogramm ======
def main():
    try:
        print("Starte Messen-Scraping...")
        api_rows = fetch_auma_messen_de()
        print(f"AUMA: {len(api_rows)} Messen geladen.")
        
        print("Lade Datenbank-Messen...")
        db_rows = fetch_db_messen()
        db_index = index_by_id(db_rows)
        print(f"DB: {len(db_rows)} Messen vorhanden.")
        
        changes = diff_messen(api_rows, db_index)
        print(f"Änderungen: {len(changes)}")
        
        if not changes:
            print("Keine relevanten Änderungen. Ende.")
            return
        
        print("Aktualisiere Datenbank...")
        apply_changes_to_db(changes)
        print("✓ DB aktualisiert.")
        
        print("Lade Abonnenten...")
        abonnenten = fetch_abonnenten()
        print(f"{len(abonnenten)} Abonnenten gefunden.")
        
        notifications = build_notifications(changes, abonnenten)
        
        if notifications:
            print(f"Sende {len(notifications)} Benachrichtigungen...")
            send_notifications(notifications)
            print("✓ Alle Benachrichtigungen gesendet")
        else:
            print("Keine Benachrichtigungen nötig")
            
    except Exception as e:
        print(f"✗ Kritischer Fehler: {str(e)}")
        # Optional: Fehler an Admin senden

if __name__ == "__main__":
  main()