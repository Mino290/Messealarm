import requests
import json

def lade_messen():
    url = "https://www.auma.de/api/TradeFairData/getWebOverviewTradeFairData"
    params = {
        "intFilterYearFrom": 2025,
        "intFilterYearTo": 2032,
        "intFilterMonthFrom": 8,
        "intFilterMonthTo": 12,
        "strLanguage": "de",
        "intSort": 1,
        "intSeitenZahl": 1,
        "strSearchString": "Berlin"  # Stadtfilter hier
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # gibt HTTPError, wenn nicht 2xx
        daten = response.json()
        print(json.dumps(daten[0], indent=2, ensure_ascii=False))

        print(f"Es wurden {len(daten)} Messen gefunden:")
        for messe in daten:
            print(f"- {messe.get('strTitel')} ({messe.get('dtVon')} bis {messe.get('dtBis')})")

        return daten

    except requests.exceptions.Timeout:
        print("❌ Timeout: Die AUMA-API hat nicht rechtzeitig geantwortet.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim Abruf: {e}")
    except json.JSONDecodeError:
        print("❌ Antwort konnte nicht als JSON interpretiert werden.")
    return []

if __name__ == "__main__":
    lade_messen()
