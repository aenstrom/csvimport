import requests
import os
from datetime import date

# Lista med .csv-filer i din GitHub-repo
CSV_FILNAMN = [
    "nyheter_2025-06-05_3.csv",
    "nyheter_2025-06-05_4.csv",
    # Lägg till fler här om du vill
]

# GitHub-rålänk till data-mappen i ditt repo
GITHUB_USER = "aenstrom"
REPO = "csvimport"
BRANCH = "main"
DATA_MAPP = ""

BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO}/{BRANCH}/{DATA_MAPP}"

def hamta_csv_filer():
    api_url = f"https://api.github.com/repos/{GITHUB_USER}/{REPO}/contents/{DATA_MAPP}?ref={BRANCH}"
    response = requests.get(api_url)

    if response.status_code != 200:
        raise Exception(f"❌ Kunde inte hämta filerna: {response.status_code} - {response.text}")

    json_data = response.json()
    csv_filer = [f["name"] for f in json_data if f["name"].endswith(".csv")]
    return csv_filer

def skapa_sparkkod(csv_filer):
    kod = ""

    for filnamn in csv_filer:
        url = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO}/{BRANCH}/{DATA_MAPP}/{filnamn}"
        lokal_fil = f"/tmp/{filnamn}"

        kod += f"""
# Läs {filnamn} från GitHub
import requests
url = "{url}"
lokal_fil = "/dbfs{lokal_fil}"
r = requests.get(url)
with open(lokal_fil, "wb") as f:
    f.write(r.content)

df = spark.read.option("header", True).csv("{lokal_fil}")
df.write.mode("overwrite").saveAsTable("nyhetsagent_csv_import")
"""

    return kod.strip()

def exportera_notebook_kod():
    csv_filer = hamta_csv_filer()
    kod = skapa_sparkkod(csv_filer)
    filnamn = f"notebook_import_{date.today().isoformat()}.py"

    with open(filnamn, "w", encoding="utf-8") as f:
        f.write("# Auto-genererad importkod för Databricks:\n\n")
        f.write(kod)

    print(f"✅ Notebook-kod sparad som: {filnamn}")

if __name__ == "__main__":
    exportera_notebook_kod()
