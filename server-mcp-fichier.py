from mcp.server.fastmcp import FastMCP
import os
import csv
import json
from tavily import TavilyClient
from typing import List,Dict
import re
from dotenv import load_dotenv

load_dotenv()
mcp = FastMCP("acces-fichier")

@mcp.tool()
def lister_rep(dir : str) -> str:
    """
    -Afficher toutes le contenu de dossier data
    """
    try : 

        if not os.path.exists(dir):
            return f"Dossier {dir} introuvable"
        fich =[]
        """
        parcours dossier et enregistrer dans list

        """
        for f in os.listdir(dir):
            fich.append(f)
        
        if not fich:
            return "Aucune Fichier PDF Trouver dans {dir}! "
        return "\n".join(fich)
    
    except Exception as err :
        return f"erreur : {str(err)}"

@mcp.tool()
def web_search(query : str ) -> list[Dict]: 
    try :
        tavily_client = TavilyClient(api_key="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")
        response = tavily_client.search(query=query)
        return response['results']
    except :
        return "not resultat found"

@mcp.tool()
def extract_hospital_data(dir: str) -> list[Dict]:
    """
    Parcourt tous les fichiers CSV et JSON d'un dossier et extrait, pour chaque enregistrement,
    les champs suivants (vides si absents):
      - nom, email, telephone, province, ville, nombre_salle

    Retourne une liste de dictionnaires normalisés.
    """
    try:
        if not os.path.exists(dir):
            return [{"error": f"Dossier {dir} introuvable"}]

        results: List[Dict] = []

        # Aliases possibles par champ
        field_aliases: Dict[str, List[str]] = {
            "nom": ["nom", "name", "hospital", "hopital", "hôpital", "hospital_name", "organisation", "organization", "org"],
            "email": ["email", "e-mail", "mail", "contact_email"],
            "telephone": ["telephone", "téléphone", "tel", "phone", "phone_number", "contact_phone"],
            "province": ["province", "provine", "state", "region"],
            "ville": ["ville", "city", "localite", "localité", "town"],
            "nombre_salle": ["nombre_salle", "nb_salle", "nbre_salle", "rooms", "number_of_rooms", "rooms_count", "salles", "nombre_de_salles"]
        }

        def pick(record: Dict) -> Dict:
            lower_map = {str(k).strip().lower(): v for k, v in record.items()}
            out = {}
            for canonical, aliases in field_aliases.items():
                value = " "
                for alias in aliases:
                    if alias in lower_map and lower_map[alias] not in (None, ""):
                        value = lower_map[alias]
                        break
                out[canonical] = value
            return out

        def read_csv(path: str) -> List[Dict]:
            rows: List[Dict] = []
            with open(path, mode="r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(pick(row))
            return rows

        def read_json(path: str) -> List[Dict]:
            with open(path, mode="r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return [pick(item) for item in data if isinstance(item, dict)]
            if isinstance(data, dict):
                return [pick(data)]
            return []

        for filename in os.listdir(dir):
            path = os.path.join(dir, filename)
            if not os.path.isfile(path):
                continue
            name_lower = filename.lower()
            try:
                if name_lower.endswith(".csv"):
                    results.extend(read_csv(path))
                elif name_lower.endswith(".json"):
                    results.extend(read_json(path))
                else:
                    # Ignorer les types non supportés sans erreur bloquante
                    continue
            except Exception as file_err:
                results.append({"file": filename, "error": str(file_err)})

        return results if results else []
    except Exception as err:
        return [{"error": str(err)}]

x