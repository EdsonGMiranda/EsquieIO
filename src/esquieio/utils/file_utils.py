
import os

def read_sql_file(filename):
    with open(filename, 'r') as s:
        sql_script = s.read()

    return sql_script




def listar_arquivos(file_name,caminho):
    arquivos_localidade = []
    for nome in os.listdir(caminho):
        if (
                os.path.isfile(os.path.join(caminho, nome))
                and nome.upper().startswith(file_name)
        ):
            arquivos_localidade.append(nome)

    print(arquivos_localidade)
    return arquivos_localidade