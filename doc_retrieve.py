from fastapi import FastAPI, HTTPException
import sqlite3
import os
import requests

app = FastAPI()

DB_FILE = "documents.db"
TSV_FILE = "documents.tsv"
TSV_URL = "https://media.githubusercontent.com/media/simeonwetzel/N4E-IR25/refs/heads/main/docs/documents.tsv"

# --- Step 1: Create DB if not exists ---
if not os.path.exists(DB_FILE):
    print("Downloading TSV and creating SQLite DB...")
    # Download TSV
    r = requests.get(TSV_URL, stream=True)
    r.raise_for_status()  # fail early if download fails
    with open(TSV_FILE, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    # Create SQLite DB and import TSV
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("CREATE TABLE documents (doc_id INTEGER PRIMARY KEY, document TEXT)")

    with open(TSV_FILE, "r", encoding="utf-8") as f:
        header = next(f, None)  # skip header if present
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            doc_id, text = parts
            try:
                c.execute(
                    "INSERT INTO documents (doc_id, document) VALUES (?, ?)",
                    (int(doc_id), text)
                )
            except ValueError:
                continue  # skip invalid doc_id

    conn.commit()
    conn.close()
    print("DB creation complete.")

    # Optional: remove TSV to save space
    os.remove(TSV_FILE)

# --- Step 2: Open connection for API (thread-safe) ---
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
c = conn.cursor()

@app.get("/document/{doc_id}")
def get_document(doc_id: int):
    c.execute("SELECT document FROM documents WHERE doc_id = ?", (doc_id,))
    result = c.fetchone()
    if result:
        return {"doc_id": doc_id, "document": result[0]}
    else:
        raise HTTPException(status_code=404, detail="Document not found")