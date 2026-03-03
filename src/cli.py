from __future__ import annotations

from pathlib import Path
from typing import Optional, List

import typer

from esquieio.utils.config import load_env_variables
from esquieio.static_sources.extract_csv import extract_data_from_csv
from esquieio.utils.file_utils import read_sql_file
from esquieio.bd.db_connection import DatabaseConnection

app = typer.Typer(
    help="EsquieIO CLI - extrair arquivos e carregar em banco",
    no_args_is_help=True,
)

load_app = typer.Typer(help="Comandos de carga")
sql_app = typer.Typer(help="Comandos SQL")
app.add_typer(load_app, name="load")
app.add_typer(sql_app, name="sql")


def _get_env(env: Optional[str]) -> dict:
    if not env:
        return {}
    return load_env_variables(env)


def _str2bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "sim"}


def _resolve_conn_params(
    env_vars: dict,
    sgbd: Optional[str],
    host: Optional[str],
    port: Optional[str],
    database: Optional[str],
    driver: Optional[str],
    windows_auth: Optional[bool],
    user: Optional[str],
    password: Optional[str],
) -> dict:
    # CLI > .env > defaults
    resolved = {
        "sgbd": sgbd or env_vars.get("DB_SGBD", "mssql"),
        "host": host or env_vars.get("DB_HOST"),
        "port": port or env_vars.get("DB_PORT") or None,
        "database": database or env_vars.get("DB_DATABASE"),
        "driver": driver or env_vars.get("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
        "windows_auth": windows_auth
        if windows_auth is not None
        else _str2bool(env_vars.get("DB_WINDOWS_AUTH", "false")) or False,
        "user": user or env_vars.get("DB_USER"),
        "password": password or env_vars.get("DB_PASSWORD"),
    }

    if not resolved["host"] or not resolved["database"]:
        raise typer.BadParameter(
            "host e database são obrigatórios (via CLI ou .env.<ambiente>)."
        )

    return resolved


@load_app.command("csv")
def load_csv(
    # ambiente
    env: Optional[str] = typer.Option(
        None, "--env", "-e", help="Ambiente do .env (ex: dev -> .env.dev)"
    ),
    # arquivo
    dir_path: str = typer.Option(..., "--dir", help="Diretório onde estão os arquivos (ex: arquivos)"),
    file_pattern: str = typer.Option(..., "--file", help="Padrão do arquivo (ex: LOG_LOCALIDADE)"),
    columns: str = typer.Option(
        ..., "--columns", help="Colunas separadas por vírgula (ex: A,B,C)"
    ),
    # destino
    table: str = typer.Option(..., "--table", help="Tabela destino"),
    schema: str = typer.Option("dbo", "--schema", help="Schema (default: dbo)"),
    truncate: bool = typer.Option(False, "--truncate", help="Trunca/limpa a tabela antes de inserir"),
    # conexão (sobrescreve .env)
    sgbd: Optional[str] = typer.Option(None, "--sgbd", help="SGBD (default: mssql)"),
    host: Optional[str] = typer.Option(None, "--host", help=r"Host (ex: SRV\INSTANCIA)"),
    port: Optional[str] = typer.Option(None, "--port", help="Porta (opcional)"),
    database: Optional[str] = typer.Option(None, "--database", help="Database"),
    driver: Optional[str] = typer.Option(None, "--driver", help="Driver ODBC"),
    windows_auth: Optional[bool] = typer.Option(
        None,
        "--windows-auth/--no-windows-auth",
        help="Força Windows Auth (ou desliga). Se não informar, usa DB_WINDOWS_AUTH do .env.",
    ),
    user: Optional[str] = typer.Option(None, "--user", help="Usuário (SQL Auth)"),
    password: Optional[str] = typer.Option(None, "--password", help="Senha (SQL Auth)"),
):
    """
    Extrai CSV/TXT do diretório e carrega na tabela.

    Usa static_sources.extract_csv.extract_data_from_csv:
    - procura arquivos que contenham o padrão
    - delimiter '@'
    - tenta encoding latin-1 e windows-1252
    """
    env_vars = _get_env(env)

    conn = _resolve_conn_params(
        env_vars=env_vars,
        sgbd=sgbd,
        host=host,
        port=port,
        database=database,
        driver=driver,
        windows_auth=windows_auth,
        user=user,
        password=password,
    )

    cols = [c.strip() for c in columns.split(",") if c.strip()]
    if not cols:
        raise typer.BadParameter("--columns precisa ter pelo menos 1 coluna.")

    df = extract_data_from_csv(dir_path, file_pattern, cols)
    if df.empty:
        typer.echo("AVISO: DataFrame vazio. Nada a carregar.")
        raise typer.Exit(code=0)

    with DatabaseConnection(
        sgbd=conn["sgbd"],
        host=conn["host"],
        port=conn["port"],
        database=conn["database"],
        driver=conn["driver"],
        windows_auth=conn["windows_auth"],
        user=conn["user"],
        password=conn["password"],
    ) as db:
        if truncate:
            db.truncate_table(table, schema=schema, fallback_delete=True)

        db.create_table_to(df, table)

    typer.echo("OK: carga finalizada.")


@sql_app.command("run")
def run_sql(
    env: Optional[str] = typer.Option(None, "--env", "-e", help="Ambiente do .env (ex: dev -> .env.dev)"),
    file: Path = typer.Option(..., "--file", "-f", exists=True, dir_okay=False, help="Arquivo .sql para executar"),
    # conexão (sobrescreve .env)
    sgbd: Optional[str] = typer.Option(None, "--sgbd", help="SGBD (default: mssql)"),
    host: Optional[str] = typer.Option(None, "--host", help=r"Host (ex: SRV\INSTANCIA)"),
    port: Optional[str] = typer.Option(None, "--port", help="Porta (opcional)"),
    database: Optional[str] = typer.Option(None, "--database", help="Database"),
    driver: Optional[str] = typer.Option(None, "--driver", help="Driver ODBC"),
    windows_auth: Optional[bool] = typer.Option(
        None,
        "--windows-auth/--no-windows-auth",
        help="Força Windows Auth (ou desliga). Se não informar, usa DB_WINDOWS_AUTH do .env.",
    ),
    user: Optional[str] = typer.Option(None, "--user", help="Usuário (SQL Auth)"),
    password: Optional[str] = typer.Option(None, "--password", help="Senha (SQL Auth)"),
):
    """
    Executa um arquivo SQL usando DatabaseConnection.execute_script().

    Suporta scripts do SSMS com separadores `GO` em linha isolada.
    """
    env_vars = _get_env(env)

    conn = _resolve_conn_params(
        env_vars=env_vars,
        sgbd=sgbd,
        host=host,
        port=port,
        database=database,
        driver=driver,
        windows_auth=windows_auth,
        user=user,
        password=password,
    )

    script = read_sql_file(str(file))

    with DatabaseConnection(
        sgbd=conn["sgbd"],
        host=conn["host"],
        port=conn["port"],
        database=conn["database"],
        driver=conn["driver"],
        windows_auth=conn["windows_auth"],
        user=conn["user"],
        password=conn["password"],
    ) as db:
        db.execute_script(script, commit=True)

    typer.echo(f"OK: script executado ({file.name}).")


def main():
    app()


if __name__ == "__main__":
    main()
