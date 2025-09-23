import xml.etree.ElementTree as ET
import pandas as pd

codici_target = [
    "VEN25-01.05.09.00",
    "VEN25-01.19.01.00",
]

dati = []

for codice_target in codici_target:
    tree = ET.parse("analisiPrezzi2025.xml")
    root = tree.getroot()
    articoli = root.findall(f".//articolo[@cod='{codice_target}']")

    for art in articoli:
        #print("Articolo:", art.attrib)
        prezzi_h = [
            p for p in art.findall(".//prezzo")
            if p.attrib.get("umi") == "h"
        ]
        descrizione = [
            d for d in art.findall(".//desc")
        ]
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
    prezzo = root.find(f".//prezzo[@cod='{codice_target}']")
    #print("UMI:", prezzo.attrib.get("umi"))

    dati.append({
        "cod": codice_target,
        "descrizione": descrizione[0].text,
        "durata": prezzo_max.attrib.get('qta'),
        'umi':prezzo.attrib.get("umi"),
        'quantità':"",
    })

df = pd.DataFrame(dati)
df.to_excel("prezzi.xlsx", index=False)

