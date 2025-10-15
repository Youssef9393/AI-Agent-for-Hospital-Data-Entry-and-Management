from mcp.server.fastmcp import FastMCP
import oracledb as db
import sys
import os
import re
from typing import List, Dict

# =======================
# Configuration MCP
# =======================
mcp = FastMCP("MCP-Oracledb", host="0.0.0.0", port=23000)

# =======================
# Infos Base Oracle
# =======================
DB_USER = "data"
DB_PASSWORD = "1650"
DB_DSN = "localhost:1521/orcl"

def getConnection():
    """Crée et retourne une connexion à la base Oracle"""
    try:    
        return db.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
    except Exception as e:
        raise Exception(f"Erreur de connexion à Oracle: {e}")

# =======================
# Tools MCP
# =======================

@mcp.tool()
def execute_sql(query: str) -> str:
    """Exécute une requête SQL sur la base Oracle"""
    conn = None
    cursor = None
    try:
        conn = getConnection()
        cursor = conn.cursor()
        cursor.execute(query)
        
        if query.strip().lower().startswith("select"):
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            formatted_results = [dict(zip(columns, row)) for row in results]
            return str(formatted_results)
        else:
            conn.commit()
            return f"✅ {cursor.rowcount} ligne(s) affectée(s)"
    except Exception as e:
        if conn:
            conn.rollback()
        return f"❌ Erreur SQL: {str(e)}"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@mcp.tool()
def oracle_version() -> str:
    """Retourne la version d'Oracle Database"""
    conn = None
    try:
        conn = getConnection()
        return f"✅ Oracle version: {conn.version}"
    except Exception as e:
        return f"❌ Erreur: {str(e)}"
    finally:
        if conn:
            conn.close()

@mcp.tool()
def list_tables() -> str:
    """Liste toutes les tables de l'utilisateur courant"""
    query = """
    SELECT table_name 
    FROM user_tables 
    ORDER BY table_name
    """
    return execute_sql(query)

@mcp.tool()
def describe_table(table_name: str) -> str:
    """Décrit la structure d'une table"""
    query = f"""
    SELECT column_name, data_type, data_length, nullable
    FROM user_tab_columns
    WHERE table_name = UPPER('{table_name}')
    ORDER BY column_id
    """
    return execute_sql(query)

@mcp.tool()
def test_connection() -> str:
    """Teste la connexion à la base Oracle"""
    conn = None
    try:
        conn = getConnection()
        cursor = conn.cursor()
        cursor.execute("SELECT 'Connexion réussie!' FROM DUAL")
        result = cursor.fetchone()
        cursor.close()
        return f"✅ {result[0]}"
    except Exception as e:
        return f"❌ Erreur de connexion: {str(e)}"
    finally:
        if conn:
            conn.close()

@mcp.tool()
def hello() -> str:
    """Exemple simple de tool"""
    return "Hello depuis MCP Oracle!"

@mcp.tool()
def extract_hospital_data_pdf(dir: str) -> list[Dict]:
    """
    Parcourt tous les fichiers PDF d'un dossier et extrait:
      - nom, email, telephone, province, ville, nombre_salle
    Les champs non trouvés sont remplis par " ". Retourne une liste normalisée.
    """
    try:
        if not os.path.exists(dir):
            return [{"error": f"Dossier {dir} introuvable"}]

        results: List[Dict] = []

        email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
        phone_re = re.compile(r"(?:\\+?\\d{1,3}[\\s.-]?)?(?:\\(\\d{1,4}\\)[\\s.-]?)?\\d{2,4}(?:[\\s.-]?\\d{2,4}){2,4}")

        candidates = {
            "nom": ["nom", "name", "hôpital", "hopital", "hospital", "etablissement", "établissement"],
            "province": ["province", "provine", "région", "region", "state"],
            "ville": ["ville", "city", "localité", "localite", "commune"],
            "nombre_salle": ["nombre de salle", "nombre_salle", "nbre de salles", "nb salles", "rooms", "rooms count", "number of rooms"]
        }

        def make_label_regex(words: List[str]) -> re.Pattern:
            group = "|".join([re.escape(w) for w in words])
            return re.compile(rf"(?:^|\\n)\\s*(?:{group})\\s*[:\\-–]?\\s*(.+)$", re.IGNORECASE | re.MULTILINE)

        label_res = {k: make_label_regex(v) for k, v in candidates.items()}

        def normalize_out(values: Dict[str, str]) -> Dict:
            return {
                "nom": values.get("nom", " ") or " ",
                "email": values.get("email", " ") or " ",
                "telephone": values.get("telephone", " ") or " ",
                "province": values.get("province", " ") or " ",
                "ville": values.get("ville", " ") or " ",
                "nombre_salle": values.get("nombre_salle", " ") or " "
            }

        def extract_text_from_pdf(path: str) -> str:
            try:
                import pdfplumber  # type: ignore
            except Exception as imp_err:
                return f"__PDF_IMPORT_ERROR__::{imp_err}"
            try:
                text_parts: List[str] = []
                with pdfplumber.open(path) as pdf:
                    for page in pdf.pages:
                        try:
                            # Texte brut
                            txt = page.extract_text() or ""
                            if txt:
                                text_parts.append(txt)
                            # Tables -> concaténer cellules ligne par ligne
                            tables = page.extract_tables() or []
                            for table in tables:
                                for row in table:
                                    if not row:
                                        continue
                                    line = " ".join([c.strip() if isinstance(c, str) else "" for c in row if c is not None])
                                    if line:
                                        text_parts.append(line)
                        except Exception:
                            continue
                return "\n".join(text_parts)
            except Exception as read_err:
                return f"__PDF_READ_ERROR__::{read_err}"

        for filename in os.listdir(dir):
            path = os.path.join(dir, filename)
            if not os.path.isfile(path) or not filename.lower().endswith(".pdf"):
                continue
            raw_text = extract_text_from_pdf(path)

            if raw_text.startswith("__PDF_IMPORT_ERROR__::"):
                results.append({"file": filename, "error": raw_text})
                continue
            if raw_text.startswith("__PDF_READ_ERROR__::"):
                results.append({"file": filename, "error": raw_text})
                continue

            values: Dict[str, str] = {"email": " ", "telephone": " "}

            email_match = email_re.search(raw_text)
            if email_match:
                values["email"] = email_match.group(0)
            phone_match = phone_re.search(raw_text)
            if phone_match:
                values["telephone"] = phone_match.group(0)

            for key, pattern in label_res.items():
                m = pattern.search(raw_text)
                if m:
                    values[key] = m.group(1).strip()
                else:
                    values.setdefault(key, " ")

            out = normalize_out(values)
            out["file"] = filename
            results.append(out)

        return results if results else []
    except Exception as err:
        return [{"error": str(err)}]

def _ensure_hopital_table(conn) -> None:
    """Crée la table HOPITAL si elle n'existe pas (Oracle)."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM user_tables WHERE table_name = 'HOPITAL'
        """)
        exists = cursor.fetchone()[0] > 0
        if not exists:
            cursor.execute(
                """
                CREATE TABLE HOPITAL (
                  ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  NOM VARCHAR2(400),
                  VILLE VARCHAR2(200),
                  TELEPHONE VARCHAR2(100),
                  EMAIL VARCHAR2(200),
                  PROVINCE VARCHAR2(200),
                  NOMBRE_SALLE VARCHAR2(50)
                )
                """
            )
            conn.commit()
    finally:
        cursor.close()

@mcp.tool()
def ingest_hospital_from_txt(file_path: str) -> str:
    """
    Lit un fichier texte (une entrée par ligne), extrait nom/ville/téléphone/email/province
    avec heuristiques simples, remplit champs manquants par " ", et insère dans HOPITAL.
    Crée la table si nécessaire.
    """
    email_re = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}")
    phone_re = re.compile(r"(?:\\+?\\d{1,3}[\\s.-]?)?(?:\\(\\d{1,4}\\)[\\s.-]?)?\\d{2,4}(?:[\\s.-]?\\d{2,4}){2,4}")

    def parse_line(line: str) -> Dict:
        original = line.strip()
        if not original:
            return {"nom": " ", "ville": " ", "telephone": " ", "email": " ", "province": " ", "nombre_salle": " "}
        # email
        email = " "
        m = email_re.search(original)
        if m:
            email = m.group(0)
            original = original.replace(email, " ")
        # phone
        telephone = " "
        m2 = phone_re.search(original)
        if m2:
            telephone = m2.group(0)
            original = original.replace(telephone, " ")
        # very naive split: remaining as name; ville/province unknown
        nom = original.strip()
        if len(nom) > 400:
            nom = nom[:400]
        return {
            "nom": nom if nom else " ",
            "ville": " ",
            "telephone": telephone or " ",
            "email": email or " ",
            "province": " ",
            "nombre_salle": " "
        }

    conn = None
    cursor = None
    try:
        conn = getConnection()
        _ensure_hopital_table(conn)
        cursor = conn.cursor()
        to_insert: List[Dict] = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                rec = parse_line(line)
                # Skip lines that are all blanks
                if all((rec[k].strip() == "") for k in rec):
                    continue
                to_insert.append((rec["nom"], rec["ville"], rec["telephone"], rec["email"], rec["province"], rec["nombre_salle"]))

        if not to_insert:
            return "Aucune ligne valide à insérer"

        cursor.executemany(
            """
            INSERT INTO HOPITAL (NOM, VILLE, TELEPHONE, EMAIL, PROVINCE, NOMBRE_SALLE)
            VALUES (:1, :2, :3, :4, :5, :6)
            """,
            to_insert
        )
        conn.commit()
        return f"✅ {cursor.rowcount} ligne(s) insérée(s) dans HOPITAL"
    except Exception as e:
        if conn:
            conn.rollback()
        return f"❌ Erreur d'ingestion: {e}"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =======================
# Démarrage du serveur
# =======================
if __name__ == "__main__":
    print("🚀 Démarrage du serveur MCP Oracle...", file=sys.stderr)
    print(f"📊 Base de données: {DB_DSN}", file=sys.stderr)

    # Utiliser le transport stdio pour les clients MCP via stdio
    mcp.run(transport="stdio")
