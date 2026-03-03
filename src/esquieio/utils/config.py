import os
from pathlib import Path
from dotenv import load_dotenv

def load_env_variables(env: str | None = "dev") -> dict:
    """
    Carrega variáveis de ambiente de um arquivo .env.<env>
    localizado no diretório de trabalho atual (raiz do projeto).
    """
    env_name = f".env.{env}" if env else ".env"
    env_path = Path.cwd() / env_name

    print("Carregando .env em:", env_path, "exists?", env_path.exists())

    load_dotenv(env_path)

    return dict(os.environ)
