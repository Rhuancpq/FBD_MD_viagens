import pandas as pd
import sys
import psycopg
import numpy as np


def load_local(fileName, cursor):
    df = pd.read_csv(fileName, sep=";", encoding="latin-1")
    orig_ida = df[["País - Origem ida", "UF - Origem ida", "Cidade - Origem ida"]]
    orig_volta = df[
        ["País - Origem volta", "UF - Origem volta", "Cidade - Origem volta"]
    ]
    dest_ida = df[["País - Destino ida", "UF - Destino ida", "Cidade - Destino ida"]]
    dest_volta = df[
        ["Pais - Destino volta", "UF - Destino volta", "Cidade - Destino volta"]
    ]

    orig_ida.columns = ["pais", "estado", "cidade"]
    orig_volta.columns = ["pais", "estado", "cidade"]
    dest_ida.columns = ["pais", "estado", "cidade"]
    dest_volta.columns = ["pais", "estado", "cidade"]

    local = pd.concat([orig_ida, orig_volta, dest_ida, dest_volta])

    local = local.drop_duplicates()

    local["estado"] = np.where(local["estado"].isnull(), None, local["estado"])

    print(local.head(10))

    cursor.executemany(
        "INSERT INTO local (pais, estado, cidade) VALUES (%s, %s, %s)",
        list(zip(*map(local.get, ["pais", "estado", "cidade"]))),
    )


# CREATE TABLE "orgao" (
#   "id" integer PRIMARY KEY,
#   "data_hora_criacao" timestamp NOT NULL,
#   "nome" varchar NOT NULL,
#   "codigo" varchar UNIQUE NOT NULL,
#   "orgao_superior" integer,
#   FOREIGN KEY ("orgao_superior") REFERENCES "orgao" ("id")
# );


# pagamento.csv head:
# "Identificador do processo de viagem";"N�mero da Proposta (PCDP)";"C�digo do �rg�o superior";"Nome do �rg�o superior";"Codigo do �rg�o pagador";"Nome do �rgao pagador";"C�digo da unidade gestora pagadora";"Nome da unidade gestora pagadora";"Tipo de pagamento";"Valor"

# viagem.csv head:
# "Identificador do processo de viagem";"N�mero da Proposta (PCDP)";"Situa��o";"Viagem Urgente";"Justificativa Urg�ncia Viagem";"C�digo do �rg�o superior";"Nome do �rg�o superior";"C�digo �rg�o solicitante";"Nome �rg�o solicitante";"CPF viajante";"Nome";"Cargo";"Fun��o";"Descri��o Fun��o";"Per�odo - Data de in�cio";"Per�odo - Data de fim";"Destinos";"Motivo";"Valor di�rias";"Valor passagens";"Valor devolu��o";"Valor outros gastos"


def load_orgao(pagamentoFileName, viagemFileName, cursor):
    pagamento_df = pd.read_csv(pagamentoFileName, sep=";", encoding="latin-1")
    viagem_df = pd.read_csv(viagemFileName, sep=";", encoding="latin-1")

    org_sup = pagamento_df[["Código do órgão superior", "Nome do órgão superior"]]
    org_pag = pagamento_df[
        ["Codigo do órgão pagador", "Nome do órgao pagador", "Código do órgão superior"]
    ]
    ug_pag = pagamento_df[
        [
            "Código da unidade gestora pagadora",
            "Nome da unidade gestora pagadora",
            "Código do órgão superior",
        ]
    ]

    v_org_sup = viagem_df[["Código do órgão superior", "Nome do órgão superior"]]
    v_org_sol = viagem_df[
        [
            "Código órgão solicitante",
            "Nome órgão solicitante",
            "Código do órgão superior",
        ]
    ]

    org_sup.columns = ["codigo", "nome"]
    org_pag.columns = ["codigo", "nome", "orgao_superior"]
    ug_pag.columns = ["codigo", "nome", "orgao_superior"]
    v_org_sup.columns = ["codigo", "nome"]
    v_org_sol.columns = ["codigo", "nome", "orgao_superior"]

    org_sup["orgao_superior"] = None
    v_org_sup["orgao_superior"] = None

    merge = pd.concat([org_sup, v_org_sup, v_org_sol, org_pag])

    merge = merge.drop_duplicates()

    merge = merge.drop(merge[merge["codigo"] == -3].index)

    nulls = merge[merge["orgao_superior"].isnull()]

    print(nulls[nulls["codigo"] == -1])

    print(nulls[nulls["nome"] == "Sem informação"])

    cursor.execute("ALTER TABLE orgao DISABLE TRIGGER ALL")

    # cursor.executemany(
    #     "INSERT INTO orgao (nome, codigo) VALUES (%s, %s)",
    #     list(zip(*map(nulls.get, ["nome", "codigo"]))),
    # )

    cursor.execute("ALTER TABLE orgao ENABLE TRIGGER ALL")

    groups = merge.groupby("codigo")
    # filter groups with more than one element
    groups = groups.filter(lambda x: len(x) > 1)
    print(groups.head(10))

    merge = merge[merge["orgao_superior"].notnull()]

    # cursor.executemany(
    #     "INSERT INTO orgao (nome, codigo, orgao_superior) VALUES (%s, %s, %s)",
    #     list(zip(*map(merge.get, ["nome", "codigo", "orgao_superior"]))),
    # )

    pass


def load_cargo(fileName):
    pass


# filenames = "pagamento.csv" "passagem.csv" "trecho.csv" "viagem.csv
def load_data(fileNames, cursor):
    # load_local(fileNames[1], cursor)
    load_orgao(fileNames[0], fileNames[3], cursor)
    pass


# filenames = "pagamento.csv" "passagem.csv" "trecho.csv" "viagem.csv em ordem
# python3 load.py 2022_Pagamento.csv 2022_Passagem.csv 2022_Trecho.csv 2022_Viagem.csv postgresql://postgres:example@localhost:5432/viagens
if __name__ == "__main__":
    # get from command line
    fileNames = sys.argv[1:-1]
    conn_url = sys.argv[-1]
    cursor = {}
    with psycopg.connect(conn_url) as conn:
        with conn.cursor() as cursor:
            load_data(fileNames, cursor)
