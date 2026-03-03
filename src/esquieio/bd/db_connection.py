import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus


class DatabaseConnection:
    def __init__(self, sgbd: str, user: str = None, password: str = None, host: str = None,
                 database: str = None, port: str = None, driver: str = None,
                 windows_auth: bool = False):
        self.sgbd = sgbd
        self.user = user
        self.password = password
        self.host = host
        self.port = str(port) if port else ""
        self.database = database
        self.driver = driver if driver else "ODBC Driver 17 for SQL Server"
        self.windows_auth = windows_auth

        self.engine = None
        self.session = None
        self.db_url = self._create_db_url()

    def _create_db_url(self):
        if self.sgbd == "mssql":
            host = self.host or ""

            # Se vier "SERVIDOR\INSTANCIA", separa
            if "\\" in host:
                host_only, instance = host.split("\\", 1)
            else:
                host_only, instance = host, None

            # Se tem porta, usa TCP explícito (não depende de SQL Browser)
            if self.port:
                server = f"tcp:{host_only},{self.port}"
            else:
                server = host if instance else host_only  # mantém instância se não tiver porta

            base = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={server};"
                f"DATABASE={self.database};"
                "TrustServerCertificate=yes;"
                "Encrypt=no;"
                "Connection Timeout=10;"
            )

            if self.windows_auth:
                parametros = base + "Trusted_Connection=yes;"
            else:
                parametros = base + f"UID={self.user};PWD={self.password};"

            return f"mssql+pyodbc:///?odbc_connect={quote_plus(parametros)}"

        db_urls={
            "postgresql": f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            if self.port else f"postgresql://{self.user}:{self.password}@{self.host}/{self.database}",

            "mysql": f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            if self.port else f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}",

            "oracle": f"oracle+cx_oracle://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.database}"
            if self.port else f"oracle+cx_oracle://{self.user}:{self.password}@{self.host}/?service_name={self.database}",
        }
        return db_urls.get(self.sgbd, None)

    def connect(self):
        if not self.db_url:
            raise ValueError("SGBD não suportado ou URL inválida.")

        try:
            print("DB URL (SQLAlchemy):", self.db_url)

            self.engine = create_engine(self.db_url, pool_pre_ping=True, future=True)

            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            Session = sessionmaker(bind=self.engine, future=True)
            self.session = Session()
            print("Conexão estabelecida com sucesso!")
        except SQLAlchemyError as e:
            self.session = None
            if self.engine:
                self.engine.dispose()
                self.engine = None
            raise RuntimeError(f"Erro ao conectar ao banco de dados: {e}") from e

    def execute_query(self, query: str, output_format: str = None, output_path: str = None):
        """
        Executa uma consulta e salva o resultado opcionalmente.
        """
        if not self.session:
            print("A sessão não está ativa. Conecte-se primeiro ao banco de dados.")
            return None

        try:
            df = pd.read_sql(query, self.engine)
            if output_format and output_path:
                if output_format == "parquet":
                    df.to_parquet(output_path, index=False)
                elif output_format == "csv":
                    df.to_csv(output_path, index=False)
                elif output_format == "excel":
                    df.to_excel(output_path, index=False)
                print(f"Consulta salva em {output_format}: {output_path}")
            return df
        except SQLAlchemyError as e:
            print(f"Erro ao executar a consulta: {e}")
            return None

    def create_table_to(self, df, table_name):
        """Insere um DataFrame em uma tabela."""
        try:
            df.to_sql(table_name, schema='dbo', con=self.engine, if_exists='append', index=False, chunksize=1000)
            print(f"Dados inseridos na tabela {table_name} com sucesso!")
        except SQLAlchemyError as e:
            print(f"Erro ao inserir dados: {e}")

    def truncate_table(
            self,
            table_name: str,
            schema: str = "dbo",
            fallback_delete: bool = True,
    ) -> None:
        """
        Trunca (ou limpa) uma tabela com validação básica.
        - Usa self.engine.begin() (transação) ao invés de session
        - Valida nome/schema para evitar injeção
        - Opcional: fallback para DELETE se TRUNCATE falhar
        """
        if not self.engine:
            raise RuntimeError("Engine não está ativa. Conecte-se primeiro ao banco de dados.")

        ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

        if not ident_re.match(table_name):
            raise ValueError(f"Nome de tabela inválido: {table_name!r}")
        if schema and not ident_re.match(schema):
            raise ValueError(f"Nome de schema inválido: {schema!r}")

        full_name = f"{schema}.{table_name}" if schema else table_name

        # 1) tenta TRUNCATE
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {full_name}"))
            print(f"Tabela {full_name} truncada com sucesso.")
            return
        except SQLAlchemyError as e_truncate:
            if not fallback_delete:
                raise RuntimeError(f"Erro ao truncar a tabela {full_name}: {e_truncate}") from e_truncate

        # 2) fallback DELETE
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {full_name}"))
            print(f"TRUNCATE falhou; tabela {full_name} limpa via DELETE com sucesso.")
        except SQLAlchemyError as e_delete:
            raise RuntimeError(
                f"Erro ao truncar/limpar a tabela {full_name}: {e_delete}"
            ) from e_delete

    def execute_script(self, script: str, *, commit: bool = True) -> None:
        """
        Executa um script SQL grande (com separadores GO do SSMS).
        - Remove/ignora linhas USE <db>;
        - Divide por linhas contendo apenas GO (case-insensitive);
        - Executa em ordem.
        """
        if not self.session:
            raise RuntimeError("A sessão não está ativa. Conecte-se primeiro ao banco de dados.")

        # 1) Remove linhas USE <db>;
        lines = []
        for line in script.splitlines():
            if re.match(r"^\s*USE\s+\w+\s*;\s*$", line, flags=re.IGNORECASE):
                continue
            lines.append(line)
        cleaned = "\n".join(lines)

        # 2) Divide por GO em linha isolada
        batches = re.split(r"^\s*GO\s*$", cleaned, flags=re.IGNORECASE | re.MULTILINE)

        try:
            for batch in batches:
                sql = batch.strip()
                if not sql:
                    continue
                self.session.execute(text(sql))

            if commit:
                self.session.commit()
        except SQLAlchemyError:
            self.session.rollback()
            raise

    def close(self):
        if self.session:
            self.session.close()
            self.session = None
        if self.engine:
            self.engine.dispose()
            self.engine = None
        print("Conexão fechada com sucesso!")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

