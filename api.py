import os
import re
import time
import requests
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone

# ====== Konfiguration ======
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://bunrrjnxcdsstpkvldhj.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJ1bnJyam54Y2Rzc3Rwa3ZsZGhqIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDA1Nzc1NiwiZXhwIjoyMDY5NjMzNzU2fQ.Y-lCtfIIEal5MVRtCe5ZWke_RqR3X40M2vMQVvTP3Dc")

AUMA_API_URL = "https://www.auma.de/api/TradeFairData/getWebOverviewTradeFairData"
DETAIL_URL_FMT = "https://www.auma.de/messen-finden/details/?tfd={url_param}"

# Filter: Deutschland, alle Monate, mehrere Jahre (anpassbar)
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
    """UTC-zeitbewusster ISO-String."""
    return datetime.now(timezone.utc).isoformat()

def parse_datum(date_string):
    if not date_string:
        return None, None

    # Ersetze Sonderzeichen
    date_string = date_string.replace("–", "-").replace("bis", "-")
    
    # Suche nach zwei Datumsangaben
    match = re.findall(r"\d{1,2}\.\d{1,2}\.\d{4}", date_string)
    if len(match) == 2:
        start = datetime.strptime(match[0], "%d.%m.%Y").date()
        end = datetime.strptime(match[1], "%d.%m.%Y").date()
        return start.isoformat(), end.isoformat()
    
    # Wenn nur ein Datum gefunden wird
    if len(match) == 1:
        start = datetime.strptime(match[0], "%d.%m.%Y").date()
        return start.isoformat(), start.isoformat()
    
    return None, None

def normalize_city(s: Optional[str]) -> Optional[str]:
    """Kleinschreibung, Whitespace trimmen – robustere Stadtvergleiche."""
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip().lower()

# ====== 1) AUMA: alle deutschen Messen holen (paginiert), aber nur zurückgeben (kein DB-Write) ======
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
        time.sleep(0.2)  # sanft drosseln

    return result

# ====== 2) Supabase: bestehende Messen & Abonnenten laden ======
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
    """
    Erwartet Tabelle 'Abonnenten' mit mind.:
      - name (text)
      - email (text)
      - staedte (text[] ODER text, z. B. 'Berlin,Hamburg')
    """
    url = f"{SUPABASE_URL}/rest/v1/Abonnenten?select=*"
    r = requests.get(url, headers=supabase_headers(), timeout=60)
    r.raise_for_status()
    return r.json()

# ====== 3) Vergleich: Änderungen feststellen ======
def index_by_id(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {row["id"]: row for row in rows if "id" in row}

def dates_equal(a: Optional[str], b: Optional[str]) -> bool:
    return (a or "") == (b or "")

def diff_messen(api_rows: List[Dict[str, Any]], db_index: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Liefert eine Liste von Änderungen mit strukturierter Info:
    - type: 'new' | 'date_added' | 'date_changed'
    - messe: aktueller API-Datensatz (bereits normalisiert)
    - before: vorheriger DB-Status (nur für Änderungen vorhanden)
    - changed_fields: Dict der geänderten Datumsfelder (für Updates)
    """
    changes: List[Dict[str, Any]] = []

    for cur in api_rows:
        mid = cur["id"]
        prev = db_index.get(mid)

        if prev is None:
            # komplett neu
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

        # "Datum neu hinzugefügt": vorher fehlend (None/""), jetzt vorhanden
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

        # "Datum geändert": vorher != jetzt (und beide Werte existent oder einer wurde geändert)
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

# ====== 4) DB-Updates anwenden ======
def upsert_new_messen(new_items: List[Dict[str, Any]]) -> None:
    """Neue Messen in einem Schwung hochladen."""
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
    """Nur geänderte Datumsfelder patchen (plus ggf. URL/Param)."""
    if not changed_fields:
        return
    url = f"{SUPABASE_URL}/rest/v1/Messen?id=eq.{messe_id}"
    r = requests.patch(url, headers=supabase_headers(), json=changed_fields, timeout=60)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"Patch failed for {messe_id}: {r.status_code} {r.text}")

def apply_changes_to_db(changes: List[Dict[str, Any]]) -> None:
    new_items = [c["messe"] for c in changes if c["type"] == "new"]
    updates = [c for c in changes if c["type"] in ("date_added", "date_changed")]

    # Neue Messen in Batches hochladen (z. B. 500er Batches)
    BATCH = 500
    for i in range(0, len(new_items), BATCH):
        upsert_new_messen(new_items[i:i+BATCH])
        time.sleep(0.2)

    # Updates patchen
    for upd in updates:
        mid = upd["messe"]["id"]
        # Stelle sicher, dass URL/Param/Kategorie/Ort mitgepflegt werden (falls die API das zwischenzeitlich korrigiert hat)
        base_fields = {
            "titel": upd["messe"].get("titel"),
            "stadt": upd["messe"].get("stadt"),
            "land": upd["messe"].get("land"),
            "url_param": upd["messe"].get("url_param"),
            "url": upd["messe"].get("url"),
            "kategorie": upd["messe"].get("kategorie"),
        }
        payload = {**base_fields, **upd["changed_fields"]}
        patch_messe_dates(mid, payload)
        time.sleep(0.1)

# ====== 5) Abonnenten-Verknüpfung & Benachrichtigungs-Liste ======
def parse_abonnent_staedt(eintrag: Dict[str, Any]) -> List[str]:
    """
    Akzeptiert sowohl text[] (bereits Liste) als auch CSV-Text.
    Normalisiert auf lowercase.
    """
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

def build_notifications(changes: List[Dict[str, Any]], abonnenten: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    
    # Vorindexierung: city -> Änderungen
    city_map: Dict[str, List[Dict[str, Any]]] = {}
    for c in changes:
        city = normalize_city(c["messe"].get("stadt"))
        if not city:
            continue
        city_map.setdefault(city, []).append(c)

    # Abonnenten matchen
    out: Dict[str, List[Dict[str, Any]]] = {}
    for ab in abonnenten:
        email = ab.get("email")
        if not email:
            continue
        cities = parse_abonnent_staedt(ab)
        if not cities:
            continue

        bucket: List[Dict[str, Any]] = []
        for city in cities:
            bucket.extend(city_map.get(city, []))

        # Duplikate (gleiche Messe mehrfach wegen mehrerer Städte) filtern
        seen = set()
        uniq_bucket = []
        for item in bucket:
            mid = item["messe"]["id"]
            if mid in seen:
                continue
            seen.add(mid)
            uniq_bucket.append(item)

        if uniq_bucket:
            out[email] = uniq_bucket

    return out

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_USER = "messen.infos@gmail.com"
SMTP_PASS = "gzao nytx exxb hczk"

# E-Mail Vorlage als HTML (leicht anpassbar)
EMAIL_SUBJECT_TEMPLATE = "Neuer Termin - {titel} {start_datum} - {end_datum}"

EMAIL_BODY_TEMPLATE = """
<html>
<body>
<p>Hallo {name},</p>

<p>Es gibt einen neuen Termin in {stadt}:</p>

<p><b>{titel}</b><br>
{start_datum} - {end_datum}<br>
{stadt}, {land}</p>

<p><a href="{url}" target="_blank">Mehr Infos zu dieser Messe</a></p>

<p>Viele Grüße<br>
Mino</p>
</body>
</html>
"""

def send_notifications(notify_map: Dict[str, List[Dict[str, Any]]]) -> None:
    """Versendet Benachrichtigungs-Mails über Gmail SMTP im HTML-Format."""
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)

        for email, items in notify_map.items():
            for it in items:
                messe = it["messe"]

                # Hier Name aus Abonnenten holen — falls in notify_map nur email+änderungen steht, muss vorher Mapping existieren
                # Beispiel: Name aus 'Abonnenten'-Abfrage mitgegeben
                name = messe.get("abon_name", "Abonnent")

                # Platzhalter füllen
                subject = EMAIL_SUBJECT_TEMPLATE.format(
                    titel=messe["titel"],
                    start_datum=messe.get("start_datum") or "",
                    end_datum=messe.get("end_datum") or ""
                )

                body_html = EMAIL_BODY_TEMPLATE.format(
                    name=name,
                    titel=messe["titel"],
                    start_datum=messe.get("start_datum") or "",
                    end_datum=messe.get("end_datum") or "",
                    stadt=messe.get("stadt") or "",
                    land=messe.get("land") or "",
                    url=messe.get("url") or "#"
                )

                # E-Mail bauen
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = SMTP_USER
                msg["To"] = email

                msg.attach(MIMEText(body_html, "html"))

                # Senden
                try:
                    server.sendmail(SMTP_USER, email, msg.as_string())
                    print(f"Benachrichtigung an {email} gesendet: {subject}")
                except Exception as e:
                    print(f"Fehler beim Senden an {email}: {e}")
# ====== Main-Flow ======
def main():
    # 1) Frische API-Daten
    api_rows = fetch_auma_messen_de()
    print(f"AUMA: {len(api_rows)} Messen geladen.")

    # 2) Bestehende DB-Daten
    db_rows = fetch_db_messen()
    db_index = index_by_id(db_rows)
    print(f"DB: {len(db_rows)} Messen vorhanden.")

    # 3) Änderungen ermitteln
    changes = diff_messen(api_rows, db_index)
    print(f"Änderungen: {len(changes)}")

    if not changes:
        print("Keine relevanten Änderungen. Ende.")
        return

    # 4) Änderungen in DB anwenden
    apply_changes_to_db(changes)
    print("DB aktualisiert.")

    # 5) Abonnenten laden & Benachrichtigungen bauen
    abonnenten = fetch_abonnenten()
    notify_map = build_notifications(changes, abonnenten)

    # 6) Mails senden (hier: Demo)
    if notify_map:
        send_notifications(notify_map)
    else:
        print("Keine Benachrichtigungen nötig (keine passenden Städte).")

if __name__ == "__main__":
    main()
