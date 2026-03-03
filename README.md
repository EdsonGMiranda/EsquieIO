# EsquieIO

Projeto Python para **extrair arquivos (CSV/TXT)** e **carregar em banco** (principalmente **SQL Server**) usando **SQLAlchemy + ODBC**, com suporte a:
- Autenticação **Windows (Trusted_Connection)**
- Autenticação por **usuário/senha**
- Utilitários para leitura de scripts SQL e listagem de arquivos
- Carregamento de variáveis por `.env.<ambiente>`

---

## Estrutura do projeto (versão correta)

```text
EsquieIO/
└── src/
├── README.md
├── pyproject.toml
├── requirements.txt
├── arquivos/ # Entrada de arquivos brutos (CSV / ZIP etc.)
├── bd/
│ └── db_connection.py # Conexão e operações (query / truncate / load / script)
├── static_sources/
│ └── extract_csv.py # Extração CSV/TXT e Excel (helper)
└── utils/
├── file_utils.py # read_sql_file / listar_arquivos
├── config.py # load_env_variables(.env.<ambiente>)
└── .env.<ambiente> # NÃO versionar (ex: .env.dev)
```

---

## Requisitos

- Python **3.10+**
- ODBC Driver do SQL Server instalado (Windows):
  - **ODBC Driver 17 for SQL Server** (ou 18)

Dependências principais:
- `pandas`
- `sqlalchemy`
- `pyodbc`
- `python-dotenv`

---

## Instalação (Windows)

```powershell
cd Firjan.BI.DIDAD.EsquieIO\src
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

Se você estiver usando pyproject.toml para empacotar, pode instalar em modo dev:

pip install -e .

Variáveis de ambiente (.env.<ambiente>)

O arquivo .env.<ambiente> deve ficar na pasta onde você executa o script (normalmente src/).

Exemplos:

.env.dev

.env.hml

.env.prod

Exemplo .env.dev (SQL Server com Windows Auth)
DB_SGBD=mssql
DB_HOST=SRVSEDE01605D\INSTANCIA
DB_PORT=
DB_DATABASE=HUBDADOS_Corporativo
DB_DRIVER=ODBC Driver 17 for SQL Server
DB_WINDOWS_AUTH=true
DB_USER=
DB_PASSWORD=

Carregar env no código
from utils.config import load_env_variables

env = load_env_variables("dev")  # procura .env.dev

Conectar e executar query
Windows Authentication (Trusted_Connection)
from bd.db_connection import DatabaseConnection

with DatabaseConnection(
    sgbd="mssql",
    host=r"SRVSEDE01605D\INSTANCIA",
    database="HUBDADOS_Corporativo",
    driver="ODBC Driver 17 for SQL Server",
    windows_auth=True
) as db:
    df = db.execute_query("SELECT 1 AS ok")
    print(df)

Usuário/senha
from bd.db_connection import DatabaseConnection

with DatabaseConnection(
    sgbd="mssql",
    host="SRVSEDE01605D",
    port="1433",
    database="HUBDADOS_Corporativo",
    user="meu_usuario",
    password="minha_senha",
    driver="ODBC Driver 17 for SQL Server",
    windows_auth=False
) as db:
    df = db.execute_query("SELECT 1 AS ok")
    print(df)


Nota: quando você informa port, o db_connection.py força tcp:<host>,<port> (não depende do SQL Browser).

Extrair CSV/TXT e carregar no SQL Server
1) Encontrar arquivos na pasta arquivos/
from utils.file_utils import listar_arquivos

caminho = r"arquivos"
arquivos = listar_arquivos("LOG_LOCALIDADE", caminho)

2) Ler o arquivo com static_sources/extract_csv.py

O extract_data_from_csv():

busca arquivos que contenham o padrão (file_name)

usa delimitador @

tenta encoding latin-1 e windows-1252

from static_sources.extract_csv import extract_data_from_csv

colunas_localidade = [
    "LOC_NU","UFE_SG","LOC_NO","CEP","LOC_IN_SIT","LOC_IN_TIPO_LOC",
    "LOC_NU_SUB","LOC_NO_ABREV","MUN_NU"
]

df = extract_data_from_csv("arquivos", "LOG_LOCALIDADE", colunas_localidade)
print(df.head())

3) Truncar e inserir no banco
from bd.db_connection import DatabaseConnection
from static_sources.extract_csv import extract_data_from_csv

colunas_localidade = [
    "LOC_NU","UFE_SG","LOC_NO","CEP","LOC_IN_SIT","LOC_IN_TIPO_LOC",
    "LOC_NU_SUB","LOC_NO_ABREV","MUN_NU"
]

df = extract_data_from_csv("arquivos", "LOG_LOCALIDADE", colunas_localidade)

with DatabaseConnection(
    sgbd="mssql",
    host=r"SRVSEDE01605D\INSTANCIA",
    database="HUBDADOS_Corporativo",
    driver="ODBC Driver 17 for SQL Server",
    windows_auth=True
) as db:
    db.truncate_table("LOG_LOCALIDADE", schema="dbo", fallback_delete=True)
    db.create_table_to(df, "LOG_LOCALIDADE")

Executar script SQL (com GO)

Executar script SQL (com GO)

Para scripts exportados do SSMS com GO:

from bd.db_connection import DatabaseConnection
from utils.file_utils import read_sql_file

script = read_sql_file("scripts/criar_tabelas.sql")

with DatabaseConnection(
    sgbd="mssql",
    host=r"SRVSEDE01605D\INSTANCIA",
    database="HUBDADOS_Corporativo",
    windows_auth=True
) as db:
    db.execute_script(script)


O execute_script():

remove linhas USE <db>;

quebra o script por GO em linha isolada

executa em sequência

Problemas comuns
Erro 53 / 08001 (servidor não encontrado / timeout)

Host/instância incorreta (ex.: SERVIDOR\INSTANCIA)

Sem VPN / rede corporativa

Porta bloqueada no firewall

SQL Server sem TCP/IP habilitado

Se estiver usando instância nomeada, prefira configurar port quando possível

TRUNCATE falha

O truncate_table() tenta TRUNCATE e, se falhar, faz DELETE (se fallback_delete=True).
Causas comuns:

Foreign keys

Permissões insuficientes

Git

Não versionar .env.<ambiente> nem a pasta arquivos/ com dados brutos.


# Uso do CLI
load csv usando .env.dev

esquieio load csv `
  -e dev `
  --dir "arquivos" `
  --file "LOG_LOCALIDADE" `
  --columns "LOC_NU,UFE_SG,LOC_NO,CEP,LOC_IN_SIT,LOC_IN_TIPO_LOC,LOC_NU_SUB,LOC_NO_ABREV,MUN_NU" `
  --table "LOG_LOCALIDADE" `
  --truncate

run-sql usando .env.dev

esquieio sql run -e dev --file "scripts/criar_tabelas.sql"

Sobrescrevendo conexão via CLI (Windows Auth)

esquieio sql run `
  --file "scripts/criar_tabelas.sql" `
  --host "SRVSEDE01605D\INSTANCIA" `
  --database "HUBDADOS_Corporativo" `
  --windows-auth

