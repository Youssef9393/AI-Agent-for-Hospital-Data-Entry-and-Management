import os
import re
import sys
from typing import List, Dict
import oracledb as db
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("MCP-Oracledb", host="0.0.0.0", port=23000)

DB_USER = "data"
DB_PASSWORD = "1650"
DB_DSN = "localhost:1521/orcl"

def getConnection():
    return db.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)

@mcp.tool()
def execute_sql(query: str) -> str:
    try:
        with getConnection() as conn, conn.cursor() as cursor:
            cursor.execute(query)
            if query.strip().lower().startswith("select"):
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return str([dict(zip(columns, row)) for row in results])
            else:
                conn.commit()
                return str(cursor.rowcount)
    except Exception as e:
        return str(e)

@mcp.tool()
def oracle_version() -> str:
    try:
        with getConnection() as conn:
            return conn.version
    except Exception as e:
        return str(e)

@mcp.tool()
def list_tables() -> str:
    return execute_sql("SELECT table_name FROM user_tables ORDER BY table_name")

@mcp.tool()
def describe_table(table_name: str) -> str:
    query = f"""
        SELECT column_name, data_type, data_length, nullable
        FROM user_tab_columns
        WHERE table_name = UPPER('{table_name}')
        ORDER BY column_id
    """
    return execute_sql(query)

@mcp.tool()
def test_connection() -> str:
    try:
        with getConnection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 'Connexion réussie!' FROM DUAL")
            return cursor.fetchone()[0]
    except Exception as e:
        return str(e)

@mcp.tool()
def hello() -> str:
    return "Hello depuis MCP Oracle!"

@mcp.tool()
def extract_hospital_data_pdf(dir: str) -> List[Dict]:
    import pdfplumber
    results = []
    if not os.path.exists(dir):
        return [{"error": f"Dossier {dir} introuvable"}]
    email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    phone_re = re.compile(r"(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{1,4}\)[\s.-]?)?\d{2,4}(?:[\s.-]?\d{2,4}){2,4}")
    for filename in os.listdir(dir):
        path = os.path.join(dir, filename)
        if not os.path.isfile(path) or not filename.lower().endswith(".pdf"):
            continue
        try:
            text_parts = []
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    txt = page.extract_text() or ""
                    text_parts.append(txt)
                    tables = page.extract_tables() or []
                    for table in tables:
                        for row in table:
                            if row:
                                text_parts.append(" ".join([str(c).strip() for c in row if c]))
            raw_text = "\n".join(text_parts)
            values = {"email": " ", "telephone": " "}
            email_match = email_re.search(raw_text)
            if email_match:
                values["email"] = email_match.group(0)
            phone_match = phone_re.search(raw_text)
            if phone_match:
                values["telephone"] = phone_match.group(0)
            for key, patterns in {
                "nom": ["nom", "name", "hôpital", "hopital", "hospital"],
                "province": ["province", "region", "state"],
                "ville": ["ville", "city", "commune"],
                "nombre_salle": ["nombre de salle", "rooms", "number of rooms"]
            }.items():
                for p in patterns:
                    match = re.search(rf"{p}[:\-–]?\s*(.+)", raw_text, re.IGNORECASE)
                    if match:
                        values[key] = match.group(1).strip()
                        break
                values.setdefault(key, " ")
            values["file"] = filename
            results.append(values)
        except Exception as e:
            results.append({"file": filename, "error": str(e)})
    return results

@mcp.tool()
def ingest_hospital_from_txt(file_path: str) -> str:
    email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    phone_re = re.compile(r"(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{1,4}\)[\s.-]?)?\d{2,4}(?:[\s.-]?\d{2,4}){2,4}")
    def parse_line(line: str) -> Dict:
        original = line.strip()
        email, telephone = " ", " "
        m = email_re.search(original)
        if m:
            email = m.group(0)
            original = original.replace(email, "")
        m2 = phone_re.search(original)
        if m2:
            telephone = m2.group(0)
            original = original.replace(telephone, "")
        nom = original.strip()[:400] or " "
        return {"nom": nom, "ville": " ", "telephone": telephone, "email": email, "province": " ", "nombre_salle": " "}
    try:
        with getConnection() as conn, conn.cursor() as cursor:
            cursor.execute("""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE HOPITAL (
                        ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        NOM VARCHAR2(400),
                        VILLE VARCHAR2(200),
                        TELEPHONE VARCHAR2(100),
                        EMAIL VARCHAR2(200),
                        PROVINCE VARCHAR2(200),
                        NOMBRE_SALLE VARCHAR2(50)
                    )';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
            """)
            to_insert = []
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    rec = parse_line(line)
                    if any(v.strip() for v in rec.values()):
                        to_insert.append((rec["nom"], rec["ville"], rec["telephone"], rec["email"], rec["province"], rec["nombre_salle"]))
            if not to_insert:
                return "Aucune ligne valide à insérer"
            cursor.executemany("""
                INSERT INTO HOPITAL (NOM, VILLE, TELEPHONE, EMAIL, PROVINCE, NOMBRE_SALLE)
                VALUES (:1, :2, :3, :4, :5, :6)
            """, to_insert)
            conn.commit()
            return str(cursor.rowcount)
    except Exception as e:
        return str(e)

@mcp.tool()
def web_search(query: str, max_results: int = 5) -> List[str]:
    import requests
    API_KEY = os.getenv("WEB_SEARCH_API_KEY")
    if not API_KEY:
        return ["WEB_SEARCH_API_KEY non défini"]
    headers = {"Ocp-Apim-Subscription-Key": API_KEY}
    params = {"q": query, "count": max_results}
    try:
        response = requests.get("https://api.bing.microsoft.com/v7.0/search", headers=headers, params=params, timeout=10)
        data = response.json()
        results = [r["name"] + ": " + r.get("snippet", "") for r in data.get("webPages", {}).get("value", [])]
        return results
    except Exception as e:
        return [str(e)]

if __name__ == "__main__":
    print("Démarrage du serveur MCP Oracle...", file=sys.stderr)
    print(f"Base de données: {DB_DSN}", file=sys.stderr)
    mcp.run(transport="stdio")
