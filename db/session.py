from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import psycopg
from db.base import Base
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

#DATABASE_URL = "sqlite:///./app.db"  # persistent DB

DATABASE_URL = None

KEY_VAULT_URL = "https://reconn-vault.vault.azure.net/"

try:
    credential = ManagedIdentityCredential()
    client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    
    print("client: ", client)
    
    secret_name = "dbconnectionstring-2"
    secret = client.get_secret(secret_name)
    
    DATABASE_URL = (str(secret.value))

except Exception as e:
    print("Error:", e)
    traceback.print_exc()


engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)
