import os
import re
from typing import List, Optional

import pandas as pd


def extract_data_from_csv(dir_path: str, file_name: str, colunas: List[str]) -> pd.DataFrame:
    """
    Lê um arquivo CSV localizado em um diretório específico, filtrando pelo nome do arquivo
    (ou parte dele) e tentando automaticamente diferentes codificações. Retorna o primeiro
    DataFrame encontrado que consiga ser carregado com sucesso.

    Parâmetros
    ----------
    dir_path : str
        Caminho completo do diretório onde os arquivos CSV estão armazenados.
        Exemplo: "/u01/dne/static_sources/"

    file_name : str
        Padrão (nome ou parte do nome) que será buscado nos arquivos do diretório.
        É usado como expressão regular, mas protegido internamente com `re.escape` para evitar
        interpretações indevidas de caracteres especiais.
        Exemplo: "LOG_LOGRADOURO"

    colunas : list[str]
        Lista contendo os nomes das colunas que serão aplicadas ao DataFrame resultante.
        Deve ter o mesmo número de elementos que o CSV possui campos por linha.

    Comportamento
    -------------
    - Procura no diretório todos os arquivos que contenham o padrão `file_name`.
    - Para cada arquivo correspondente, tenta carregá-lo como CSV com delimitador '@'.
    - Tenta primeiro a codificação 'latin-1'; em caso de erro, tenta 'windows-1252'.
    - Caso nenhuma codificação funcione para um arquivo, ele é ignorado e o próximo é tentado.
    - Ao carregar o primeiro arquivo válido, retorna um DataFrame.
    - Caso nenhum arquivo seja compatível ou encontrado, retorna um DataFrame vazio.

    Retorno
    -------
    pandas.DataFrame
        O DataFrame carregado a partir do primeiro arquivo correspondente encontrado.
        Caso nenhum arquivo seja encontrado ou não seja possível realizar a leitura,
        retorna um DataFrame vazio.
    """
    if not os.path.isdir(dir_path):
        print(f"ERRO: O diretório informado não existe: {dir_path}")
        return pd.DataFrame()


    safe_file_name_pattern = re.escape(file_name)
    pattern = re.compile(safe_file_name_pattern)

    try:
        dir_list = os.listdir(dir_path)
    except OSError as e:
        print(f"ERRO: Não foi possível listar o diretório {dir_path}: {e}")
        return pd.DataFrame()


    matching_files = [f for f in dir_list if pattern.search(f)]

    if not matching_files:
        print(f"AVISO: Nenhum arquivo correspondente a '{file_name}' foi encontrado em {dir_path}")
        return pd.DataFrame()


    for file_ in matching_files:
        full_path = os.path.join(dir_path, file_)
        print(f"LENDO ARQUIVO: {full_path}")


        encodings_to_try = ["latin-1", "windows-1252"]

        for enc in encodings_to_try:
            try:
                dados = pd.read_csv(
                    full_path,
                    delimiter='@',
                    header=None,
                    names=colunas,
                    encoding=enc,
                    dtype=str
                )

                return pd.DataFrame(dados)
            except UnicodeDecodeError:
                print(f"AVISO: Falha ao decodificar {file_} com encoding '{enc}'. Tentando próximo...")
            except Exception as e:
                print(f"ERRO: Falha ao ler o arquivo {file_} com encoding '{enc}': {e}")

                break


        print(
            f"ERRO: Não foi possível decodificar o arquivo {file_} "
            f"usando nenhuma das codificações: {encodings_to_try}"
        )


    print(
        f"AVISO: Nenhum arquivo correspondente a '{file_name}' em {dir_path} "
        "pôde ser carregado como DataFrame."
    )
    return pd.DataFrame()


def extract_data_from_excel(dir_path: str, file_name: str) -> pd.DataFrame:
    """
    Lê o primeiro arquivo Excel encontrado em um diretório com nome que contenha o padrão informado.

    Parâmetros
    ----------
    dir_path : str
        Caminho completo do diretório onde os arquivos Excel estão armazenados.
        Exemplo: "/u01/dne/static_sources/"

    file_name : str
        Padrão (nome ou parte do nome) que será buscado dentro dos arquivos do diretório.
        A busca utiliza expressão regular (regex).

    Retorno
    -------
    pandas.DataFrame
        O DataFrame carregado a partir do primeiro arquivo correspondente encontrado.
        Caso nenhum arquivo seja encontrado ou ocorrer erro na leitura, retorna um DataFrame vazio.
    """

    if not os.path.isdir(dir_path):
        print(f"ERRO: O diretório informado não existe: {dir_path}")
        return pd.DataFrame()

    try:
        dir_list = os.listdir(dir_path)
    except OSError as e:
        print(f"ERRO: Falha ao listar o diretório '{dir_path}': {e}")
        return pd.DataFrame()


    safe_pattern = re.escape(file_name)
    regex = re.compile(safe_pattern)


    matching_files = [f for f in dir_list if regex.search(f)]

    if not matching_files:
        print(f"AVISO: Nenhum arquivo correspondente a '{file_name}' encontrado em {dir_path}")
        return pd.DataFrame()


    for file_ in matching_files:
        full_path = os.path.join(dir_path, file_)
        print(f"LENDO ARQUIVO EXCEL: {full_path}")

        try:
            df_excel = pd.read_excel(full_path)
            return df_excel
        except Exception as e:
            print(f"ERRO: Falha ao ler o arquivo Excel '{file_}': {e}")
            continue


    print(f"AVISO: Não foi possível carregar nenhum arquivo correspondente a '{file_name}'.")
    return pd.DataFrame()


