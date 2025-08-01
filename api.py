import requests
from datetime import datetime
import time

SUPABASE_URL = "https://bunrrjnxcdsstpkvldhj.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJ1bnJyam54Y2Rzc3Rwa3ZsZGhqIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDA1Nzc1NiwiZXhwIjoyMDY5NjMzNzU2fQ.Y-lCtfIIEal5MVRtCe5ZWke_RqR3X40M2vMQVvTP3Dc"

def parse_datum(date_string):
    try:
        parts = date_string.replace("–", "-").split("-")
        if len(parts) != 2:
            return None, None
        start = datetime.strptime(parts[0].strip() + ".2025", "%d.%m.%Y")
        end = datetime.strptime(parts[1].strip() + ".2025", "%d.%m.%Y")
        return start.date().isoformat(), end.date().isoformat()
    except Exception:
        return None, None

def lade_und_speichere_messen():
    url = "https://www.auma.de/api/TradeFairData/getWebOverviewTradeFairData"
    headers = {"Accept": "application/json"}
    params = {
        "intFilterYearFrom": 2025,
        "intFilterYearTo": 2032,
        "intFilterMonthFrom": 8,
        "intFilterMonthTo": 12,
        "strLanguage": "de",
        "intSort": 1,
        "intSeitenZahl": 1,
        "strSearchString": "Berlin"
    }

    resp = requests.get(url, headers=headers, params=params)
    daten = resp.json()
    print(f"Es wurden {len(daten)} Messen gefunden.")

    for messe in daten:
        titel = messe.get("strTitel")
        stadt = messe.get("strStadt")
        land = messe.get("strLand")
        termin = messe.get("strTermin")
        url_param = messe.get("strUrlParameter")
        kategorie = messe.get("strKategorie")
        erstellt_am = datetime.utcnow().isoformat()

        start_datum, end_datum = parse_datum(termin if termin else "")

        eintrag = {
            "id": messe.get("strMesseTerminKey"),
            "titel": titel,
            "stadt": stadt,
            "land": land,
            "start_datum": start_datum,
            "end_datum": end_datum,
            "url_param": url_param,
            "kategorie": kategorie,
            "erstellt_am": erstellt_am
        }

        # POST an Supabase
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/Messen",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates"
            },
            json=eintrag
        )

        if response.status_code in [200, 201]:
            print(f"✓ Messe eingetragen: {titel}")
        else:
            print(f"✗ Fehler bei {titel}: {response.text}")

        time.sleep(0.3)

if __name__ == "__main__":
    lade_und_speichere_messen()
