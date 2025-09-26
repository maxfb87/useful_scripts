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
    re_code_full = re.compile(r"\b(VEN\d{2}-\d{2}\.\d{2}\.\d{3}\.[a-z0-9]{1,2})\b")
    re_code_firstline = re.compile(r"\b(VEN\d{2}-\d{2}\.\d)\b")
    re_code_secondline = re.compile(r"^(\d\.\d{3}\.[a-z0-9]{1,2})\b")
    re_sommano = re.compile(r"\bSOMMANO\s+\S+\s+([0-9][0-9\.\,]*)")
    with pdfplumber.open(str(path)) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            lines = [ln.strip() for ln in (page.extract_text() or "").splitlines() if ln.strip()]
            i = 0; current_code = None
            while i < len(lines):
                ln = lines[i]
                m_full = re_code_full.search(ln)
                if m_full:
                    current_code = m_full.group(1); i += 1; continue
                m_first = re_code_firstline.search(ln)
                if m_first and (i+1) < len(lines):
                    m_second = re_code_secondline.search(lines[i+1])
                    if m_second:
                        current_code = m_first.group(1) + m_second.group(1); i += 2; continue
                m_q = re_sommano.search(ln)
                if m_q and current_code:
                    raw_q = m_q.group(1)
                    try:
                        qty = float(raw_q.replace('.', '').replace(',', '.'))
                    except:
                        qty = None
                    records.append({"Codice": current_code, "Quantità": qty, "Pagina": page_idx})
                    current_code = None; i += 1; continue
                i += 1
    return pd.DataFrame(records, columns=["Codice", "Quantità", "Pagina"])

def dedup_detail(df, mode):
    if mode == "none":
        return df
    if mode == "consecutive":
        mask = ~((df["Codice"].shift(0) == df["Codice"].shift(1)) & (df["Quantità"].shift(0) == df["Quantità"].shift(1)))
        return df[mask].reset_index(drop=True)
    if mode == "all":
        return df.drop_duplicates(subset=["Codice", "Quantità"]).reset_index(drop=True)
    raise ValueError("Valore --dedup non valido")

    
def aggregate_df(df: pd.DataFrame, how: str) -> pd.DataFrame:
    if how == "sum":
        return df.groupby("Codice", as_index=False)["Quantità"].sum()
    if how == "first":
        return df.groupby("Codice", as_index=False).head(1)[["Codice", "Quantità"]].reset_index(drop=True)
    if how == "last":
        return df.groupby("Codice", as_index=False).tail(1)[["Codice", "Quantità"]].reset_index(drop=True)
    if how == "concat":
        return (df.assign(Quantità=df["Quantità"].map(lambda x: "" if pd.isna(x) else str(x)))
                  .groupby("Codice", as_index=False)
                  .agg({"Quantità": lambda s: ", ".join([v for v in s if v != ""])}))
    raise ValueError("Modalità --aggregate non valida")


#recs = parse_cme_pdf(Path(nome_input))
#df_0 = pd.DataFrame(recs, columns=["Codice", "Quantità"])

df_0 = parse_cme_pdf(Path(nome_input))

#codici_target = [
#    "VEN25-01.05.09.00",
#    "VEN25-01.19.01.00",
#]

df_0["Codice"] = df_0["Codice"].apply(lambda x: normalizza_terzo_blocco(x))

#df_0 = dedup_detail(df_0, "consecutive")

#df_0 = aggregate_df(df_0[["Codice", "Quantità"]], "concat")

#df_0 = (
#    df_0
#    .assign(Quantità=df_0["Quantità"].str.split(","))  # "a,b,c" -> ["a","b","c"]
#    .explode("Quantità", ignore_index=True)        # una riga per elemento
#)

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
        "Codice": codice_target,
        "descrizione": descrizione[0].text,
        "durata": float(prezzo_max.attrib.get('qta')),
        'umi':prezzo.attrib.get("umi") if prezzo is not None else None,
        #'quantità': df_0.loc[df_0["Codice"] == codice_target, "Quantità"].iloc[0],
    })
    
df = pd.DataFrame(dati)

df_finale = pd.merge(df_0, df, on="Codice", how="left")

nome_output = nome_input[0:-4] + ".xlsx"
df_finale.to_excel(nome_output, index=False)