import os
from urllib.parse import quote_plus
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()

_client = None
_db = None

#####################################################
# Função:      get_db
# Entrada:     Nenhuma (usa variáveis de ambiente)
# Saída:       Instância de Database (pymongo.database.Database)
# Descrição:   Constrói a URI do MongoDB Atlas (ou local), cria um
#              MongoClient com TLS e valida a conectividade via 'ping'.
#              Retorna o database conforme DB_NAME.
#####################################################
def get_db():
    global _client, _db
    if _db is not None:
        return _db

    host = os.getenv("DB_HOST", "cluster0.mzspjjm.mongodb.net")
    user = quote_plus(os.getenv("DB_USER", ""))
    pwd  = quote_plus(os.getenv("DB_PASS", ""))
    name = os.getenv("DB_NAME", "cti")

    uri = (
        f"mongodb+srv://{user}:{pwd}@{host}/{name}"
        "?retryWrites=true&w=majority&appName=Cluster0&authSource=admin"
    )

    _client = MongoClient(
        uri,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=30000,
    )

    _client.admin.command("ping")  # falha cedo se errado
    _db = _client[name]
    return _db