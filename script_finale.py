import xml.etree.ElementTree as ET
import pandas as pd
import re
from pathlib import Path
import pdfplumber

#path = "estratto_cme.pdf"
nome_input = "PFTE_SR04_CME.pdf"

def normalizza_terzo_blocco(s: str) -> str:
    blocchi = s.split(".")
    if len(blocchi) >= 3 and blocchi[2].startswith("0"):
        # elimina solo il primo carattere "0"
        blocchi[2] = blocchi[2][1:]
    return ".".join(blocchi)

def parse_cme_pdf(path: Path):
    records = []
    # Codice completo tipo VEN25-01.40.004.b oppure VEN25-06.01.007.d
    re_code_full = re.compile(r"\b(VEN\d{2}-\d{2}\.\d{2}\.\d{3}\.[a-z0-9]{1,2})\b")
    # Prima riga spezzata: VEN25-06.0 (può avere altro testo dopo -> niente '$')
    re_code_firstline = re.compile(r"\b(VEN\d{2}-\d{2}\.\d)\b")
    # Seconda riga spezzata: 1.007.d
    re_code_secondline = re.compile(r"^(\d\.\d{3}\.[a-z0-9]{1,2})\b")
    # SOMMANO <unità> <quantità> ...
    re_sommano = re.compile(r"\bSOMMANO\s+\S+\s+([0-9][0-9\.\,]*)")

    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            lines = [ln.strip() for ln in (page.extract_text() or "").splitlines() if ln.strip()]
            i = 0
            current_code = None
            while i < len(lines):
                ln = lines[i]
                m_full = re_code_full.search(ln)
                if m_full:
                    current_code = m_full.group(1)
                    i += 1
                    continue

                m_first = re_code_firstline.search(ln)
                if m_first and (i + 1) < len(lines):
                    m_second = re_code_secondline.search(lines[i+1])
                    if m_second:
                        current_code = m_first.group(1) + m_second.group(1)
                        i += 2
                        continue

                m_q = re_sommano.search(ln)
                if m_q and current_code:
                    raw_q = m_q.group(1)
                    q = raw_q.replace(".", "").replace(",", ".")
                    try:
                        qty = float(q)
                    except ValueError:
                        qty = None
                    records.append({"Codice": current_code, "Quantità": qty})
                    current_code = None
                    i += 1
                    continue

                i += 1
    return records
    
def aggregate_df(df: pd.DataFrame, how: str) -> pd.DataFrame:
    if how == "none":
        return None
    if how == "sum":
        out = df.groupby("Codice", as_index=False)["Quantità"].sum()
        return out
    if how == "first":
        idx = df.groupby("Codice", as_index=False).head(1).index
        out = df.loc[idx, ["Codice", "Quantità"]].reset_index(drop=True)
        return out
    if how == "last":
        idx = df.groupby("Codice", as_index=False).tail(1).index
        out = df.loc[idx, ["Codice", "Quantità"]].reset_index(drop=True)
        return out
    if how == "concat":
        out = (df.assign(Quantità=df["Quantità"].map(lambda x: "" if pd.isna(x) else str(x)))
                 .groupby("Codice", as_index=False)
                 .agg({"Quantità": lambda s: ", ".join([v for v in s if v != ""])}))
        return out
    raise ValueError(f"Modalità di aggregazione non supportata: {how}")


recs = parse_cme_pdf(Path(nome_input))
df_0 = pd.DataFrame(recs, columns=["Codice", "Quantità"])

#codici_target = [
#    "VEN25-01.05.09.00",
#    "VEN25-01.19.01.00",
#]

df_0["Codice"] = df_0["Codice"].apply(lambda x: normalizza_terzo_blocco(x))

#df_0 = aggregate_df(df_0[["Codice", "Quantità"]], "concat")

codici_target = list(df_0['Codice'])

print(codici_target)

dati = []

for codice_target in codici_target:
    #print("OK")
    tree = ET.parse("analisiPrezzi2025.xml")
    root = tree.getroot()
    articoli = root.findall(f".//articolo[@cod='{codice_target}']")
    #print(articoli)

    for art in articoli:
        #print("Articolo:", art.attrib)
        prezzi_h = [
            p for p in art.findall(".//prezzo")
            if p.attrib.get("umi") == "h"
        ]
        descrizione = [
            d for d in art.findall(".//desc")
        ]
        print(descrizione)
        if prezzi_h:
            prezzo_max = max(prezzi_h, key=lambda p: float(p.attrib.get("qta", 0)))
        #    print("  Prezzo con umi='h' e qta massima:")
        #    print("    Attributi:", prezzo_max.attrib)
        #    print(f"Tempo max: {prezzo_max.attrib.get('qta')}")
        else:
            print("  Nessun prezzo con umi='h' trovato")

    #legge l'elenco prezzi per memorizzare l'unità di misura
    tree = ET.parse("elencoPrezzi2025.xml")
    root = tree.getroot()
    print(codice_target)
    prezzo = root.find(f".//prezzo[@cod='{codice_target}']")
    #print("UMI:", prezzo.attrib.get("umi"))

    dati.append({
        "cod": codice_target,
        "descrizione": descrizione[0].text,
        "durata": float(prezzo_max.attrib.get('qta')),
        'umi':prezzo.attrib.get("umi") if prezzo is not None else None,
        'quantità': df_0.loc[df_0["Codice"] == codice_target, "Quantità"].iloc[0],
    })

df = pd.DataFrame(dati)
nome_output = nome_input[0:-4] + ".xlsx"
df.to_excel(nome_output, index=False)