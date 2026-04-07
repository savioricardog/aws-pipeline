import json
import unicodedata
import warnings

import awswrangler as wr
import pandas as pd

# Suprime warnings para manter output limpo
warnings.filterwarnings("ignore")


def _remover_acentos(texto: str) -> str:
    """
    Remove acentos de uma string usando normalização Unicode NFD.

    A normalização NFD (Normalization Form Decomposed) separa caracteres
    acentuados em componente base + acento. Por exemplo: 'á' vira 'a' + '´'.
    Acentos pertencem à categoria Unicode 'Mn' (Mark, nonspacing).

    Referência: Unicode Standard Annex #15 - https://unicode.org/reports/tr15/
    Documentação Python: https://docs.python.org/3/library/unicodedata.html

    Args:
        texto: String com possíveis caracteres acentuados

    Returns:
        String sem acentos e sem caracteres especiais

    Examples:
        >>> _remover_acentos('São Paulo')
        'Sao Paulo'
        >>> _remover_acentos('Ação')
        'Acao'
        >>> _remover_acentos('Négociação')
        'Negociacao'
        >>> _remover_acentos('sem acentos')
        'sem acentos'
    """
    # NFD = Normalization Form Canonical Decomposition
    texto_normalizado = unicodedata.normalize("NFD", texto)

    # Filtra apenas caracteres que NÃO são acentos
    return "".join(
        char for char in texto_normalizado if unicodedata.category(char) != "Mn"
    )


def _padronizar_nome_coluna(nome: str) -> str:
    """
    Padroniza nome de coluna para formato snake_case sem acentos.

    Transformações aplicadas (em ordem):
    1. Remove acentos
    2. Remove espaços em branco no começo e no final
    3. Converte para minúsculas
    4. Substitui " / " por "_"
    5. Remove parênteses
    6. Substitui espaços em branco por "_"

    Args:
        nome: Nome original da coluna

    Returns:
        Nome padronizado em snake_case

    Examples:
        >>> _padronizar_nome_coluna('Nome da Coluna')
        'nome_da_coluna'
        >>> _padronizar_nome_coluna('Preço / Quantidade')
        'preco_quantidade'
        >>> _padronizar_nome_coluna('Valor (R$)')
        'valor_r$'
        >>> _padronizar_nome_coluna('  Ação Preferencial  ')
        'acao_preferencial'
        >>> _padronizar_nome_coluna('DATA/HORA')
        'data/hora'
    """

    nome_sem_acentos = _remover_acentos(nome)

    return (
        nome_sem_acentos.strip()
        .lower()
        .replace(" / ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "_")
    )


def _converter_mes_para_numero(nome_mes: str) -> str:
    """
    Converte nome do mês por extenso para número com 2 dígitos.

    Args:
        nome_mes: Nome do mês em português (ex: 'janeiro', 'marco')

    Returns:
        Número do mês com zero à esquerda (ex: '01', '12')

    Raises:
        ValueError: Se o nome do mês não for reconhecido

    Examples:
        >>> _converter_mes_para_numero('janeiro')
        '01'
        >>> _converter_mes_para_numero('DEZEMBRO')
        '12'
        >>> _converter_mes_para_numero('marco')
        '03'
        >>> _converter_mes_para_numero('Julho')
        '07'
        >>> _converter_mes_para_numero('invalido')
        Traceback (most recent call last):
            ...
        ValueError: Mês 'invalido' não reconhecido
    """
    meses = [
        "janeiro",
        "fevereiro",
        "marco",
        "abril",
        "maio",
        "junho",
        "julho",
        "agosto",
        "setembro",
        "outubro",
        "novembro",
        "dezembro",
    ]

    try:
        indice = meses.index(nome_mes.lower())
        return f"{indice + 1:02d}"
    except ValueError:
        raise ValueError(f"Mês '{nome_mes}' não reconhecido")


def _padronizar_nome_aba(nome_aba: str) -> str:
    """
    Padroniza nome de aba.

    Args:
        aba: Nome original da aba

    Returns:
        Nome padronizado em minúsculas separadas por hífen

    Examples:
        >>> _padronizar_nome_aba('Posição - ETF')
        'posicao-etf'
        >>> _padronizar_nome_aba('Proventos Recebidos')
        'proventos-recebidos'
        >>> _padronizar_nome_aba('Negociações')
        'negociacoes'
        >>> _padronizar_nome_aba('Renda Fixa - Brasil')
        'renda-fixa-brasil'
    """

    nome_aba_sem_acentos = _remover_acentos(nome_aba)

    return nome_aba_sem_acentos.replace(" - ", "-").replace(" ", "-").lower()


def _extrair_ano_mes(caminho_arquivo: str) -> tuple[str, str]:
    """
    Extrai ano e mês do nome do arquivo.

    Espera formato: *-AAAA-NOME_MES.xlsx
    Exemplo: relatorio-consolidado-mensal-2025-novembro.xlsx

    Args:
        caminho_arquivo: Nome do arquivo a ser processado.

    Returns:
        Tupla (ano, mes_numero) ex: ('2025', '11')

    Examples:
        >>> _extrair_ano_mes('relatorio-consolidado-mensal-2025-novembro.xlsx')
        ('2025', '11')
        >>> _extrair_ano_mes('path/to/relatorio-2024-janeiro.xlsx')
        ('2024', '01')
        >>> _extrair_ano_mes('/bucket/raw/relatorio-2023-dezembro.xlsx')
        ('2023', '12')
        >>> _extrair_ano_mes('dados-2025-marco.xlsx')
        ('2025', '03')
    """
    # TODO: adaptar para caminhos S3
    nome_arquivo = caminho_arquivo.split("/")[-1][:-5]

    partes = nome_arquivo.split("-")

    ano = partes[-2]
    mes_nome = partes[-1]
    mes_numero = _converter_mes_para_numero(mes_nome)

    return ano, mes_numero


def _processar_dataframe(df):
    """
    Aplica transformações de limpeza no DataFrame.

    Operações realizadas:
    1. Padroniza nomes das colunas
    2. Remove colunas 'unnamed'
    3. Remove totalizadores (última linha com "Total")
    4. Remove linhas completamente vazias

    Args:
        df: DataFrame original

    Returns:
        DataFrame processado

    Examples:
        >>> import pandas as pd
        >>> # Caso básico: remove colunas unnamed
        >>> df = pd.DataFrame({
        ...     'Nome Ação': ['PETR4', 'VALE3'],
        ...     'Quantidade': [100, 200],
        ...     'Unnamed: 2': [pd.NA, pd.NA]
        ... })
        >>> df_proc = _processar_dataframe(df)
        >>> list(df_proc.columns)
        ['nome_acao', 'quantidade']
        >>> len(df_proc)
        2

        >>> # Caso com Total na penúltima linha e linha vazia após
        >>> df_excel_tipico = pd.DataFrame({
        ...     'Ativo': ['PETR4', 'VALE3', pd.NA, pd.NA],
        ...     'Valor': [100, 200, 'Total', pd.NA]
        ... })
        >>> df_proc = _processar_dataframe(df_excel_tipico)
        >>> len(df_proc)
        2
        >>> df_proc['ativo'].tolist()
        ['PETR4', 'VALE3']

        >>> # Caso com linhas totalmente vazias
        >>> df_com_vazias = pd.DataFrame({
        ...     'Col1': ['A', 'B', pd.NA],
        ...     'Col2': ['X', 'Y', pd.NA]
        ... })
        >>> df_proc = _processar_dataframe(df_com_vazias)
        >>> len(df_proc)
        2
    """
    # Padroniza nomes das colunas
    df.columns = [_padronizar_nome_coluna(col) for col in df.columns]

    # Remove colunas sem nome (geradas por colunas vazias no Excel)
    colunas_unnamed = [col for col in df.columns if col.startswith("unnamed")]
    df = df.drop(columns=colunas_unnamed)

    # Remove totalizador se presente na penúltima linha
    # Algumas planilhas Excel têm uma linha "Total" antes da última linha vazia
    if len(df) >= 2 and df.iat[-2, -1] == "Total":
        df = df.head(-2)

    # Substitui "-" por NaN e remove linhas completamente vazias
    # Comum em relatórios onde "-" indica ausência de valor
    df = df.replace("-", pd.NA).dropna(how="all")

    return df


def _salvar_como_parquet_s3(
    df: pd.DataFrame, aba_nome: str, ano: str, mes: str
) -> None:
    """
    Salva DataFrame como arquivo Parquet no S3 na estrutura de pastas padronizada.

    Estrutura: s3://{trusted}/{aba}/{particao_ano}/{particao_mes}/data.parquet

    Args:
        df: DataFrame a ser salvo
        aba_nome: Nome original da aba
        ano: Ano (4 dígitos)
        mes: Mês (2 dígitos)
        bucket: Nome do bucket S3

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> _salvar_como_parquet_s3(df: pd.DataFrame, 'Posição - Fundos', '2025', '12')  # doctest: +SKIP
        ✓ Salvo: s3://bucket-name/trusted/posicao-fundos/year=2025/month=12/data.parquet
    """
    # TODO: colocar o nome do bucket da camada trusted aqui
    bucket = "project-trusted-b3"

    aba_padronizada = _padronizar_nome_aba(aba_nome)

    # Define o caminho no S3
    key = f"{aba_padronizada}/year={ano}/month={mes}/data.parquet"

    s3_trusted_path = f"s3://{bucket}/{key}"

    wr.s3.to_parquet(df=df, path=s3_trusted_path)

    print(f"✓ Salvo: s3://{bucket}/{key}")


def processar_relatorio(caminho_arquivo: str) -> None:
    """
    Processa todas as abas de um arquivo Excel e salva como Parquet.

    Args:
        caminho_arquivo: Caminho completo do arquivo Excel

    Examples:
        >>> processar_relatorio('relatorio-consolidado-mensal-2025-novembro.xlsx')  # doctest: +SKIP
        Processando arquivo: relatorio-consolidado-mensal-2025-novembro.xlsx
        Período: 11/2025
        <BLANKLINE>
        Processando aba: Posição - ETF
        ✓ Salvo: trusted/posicao-etf/year=2025/month=11/data.parquet
        ...
    """

    ABAS = [
        "Posição - ETF",
        "Posição - Fundos",
        "Posição - Renda Fixa",
        "Posição - Tesouro Direto",
        "Proventos Recebidos",
        "Negociações",
    ]
    ano, mes = _extrair_ano_mes(caminho_arquivo)

    print(f"Processando arquivo: {caminho_arquivo}")
    print(f"Período: {mes}/{ano}\n")

    for aba in ABAS:
        print(f"Processando aba: {aba}")

        try:
            # Carrega aba do Excel
            df = wr.s3.read_excel(caminho_arquivo, sheet_name=aba)

        except ValueError as e:
            print(f"✗ Aba não encontrada ou erro: {e}")
            continue

        # Aplica transformações
        df_processado = _processar_dataframe(df)

        # Salva resultado
        _salvar_como_parquet_s3(df_processado, aba, ano, mes)

        # Imprime linha vazia (organização visual)
        print()


def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    caminho_arquivo = f"s3://{bucket_name}/{key}"

    print(caminho_arquivo)

    processar_relatorio(caminho_arquivo)

    # Retorno HTTP sucesso
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processamento concluído com sucesso"}),
    }
