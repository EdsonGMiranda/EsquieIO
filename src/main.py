from pathlib import Path
from esquieio import extract_data_from_csv, DatabaseConnection

CAMINHO = Path(r"C:\dados\correios")

COLUNAS_LOCALIDADE = [
    "LOC_NU",
    "UFE_SG",
    "LOC_NO",
    "CEP",
    "LOC_IN_SIT",
    "LOC_IN_TIPO_LOC",
    "LOC_NU_SUB",
    "LOC_NO_ABREV",
    "MUN_NU",
]

with DatabaseConnection(
    host="SQLSERVER01",
    database="DB_CORREIOS",
    windows_auth=True,
) as db:

    db.truncate_table("LOG_LOCALIDADE")

    df = extract_data_from_csv(
        CAMINHO,
        "LOG_LOCALIDADE.TXT",
        COLUNAS_LOCALIDADE,
    )

    db.insert_dataframe(df, "LOG_LOCALIDADE")

print("Carga finalizada com sucesso.")
