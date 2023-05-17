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


def load_servidor(pagamentoFileName, viagemFileName, cursor):
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

    group_orgs = pd.concat([org_pag, v_org_sol, ug_pag])

    group_orgs = group_orgs.drop_duplicates()

    print("Órgãos agrupados")
    print(group_orgs[group_orgs["codigo"] == -1])
    print(group_orgs[group_orgs["codigo"] == -3])
    print(group_orgs[group_orgs["nome"].isnull()])

    group_orgs = group_orgs.drop(group_orgs[group_orgs["nome"].isnull()].index)

    sup_orgs = pd.concat([org_sup, v_org_sup])

    sup_orgs = sup_orgs.drop_duplicates()

    print(sup_orgs[sup_orgs["codigo"] == -1])

    sup_orgs.drop(sup_orgs[sup_orgs["nome"] == "Sem informação"].index, inplace=True)

    cursor.executemany(
        "INSERT INTO orgao_superior (nome, codigo) VALUES (%s, %s)",
        list(zip(*map(sup_orgs.get, ["nome", "codigo"]))),
    )

    cursor.executemany(
        "INSERT INTO orgao_subordinado (nome, codigo, orgao_superior) VALUES (%s, %s, %s)",
        list(zip(*map(group_orgs.get, ["nome", "codigo", "orgao_superior"]))),
    )

    cargo_df = viagem_df[["Cargo", "Código órgão solicitante"]]

    cargo_df.columns = ["nome", "orgao_superior"]

    cargo_df = cargo_df.drop_duplicates()

    cargo_df = cargo_df.dropna()

    cargo_df.reset_index(inplace=True, drop=True)

    funcao_df = viagem_df[["Função", "Código órgão solicitante"]]

    funcao_df.columns = ["nome", "orgao_superior"]

    funcao_df = funcao_df.drop_duplicates()

    funcao_df = funcao_df.dropna()

    funcao_df.reset_index(inplace=True, drop=True)

    cursor.executemany(
        "INSERT INTO cargo (nome, orgao_id) VALUES (%s, %s)",
        list(zip(*map(cargo_df.get, ["nome", "orgao_superior"]))),
    )

    cursor.executemany(
        "INSERT INTO funcao (nome, orgao_id) VALUES (%s, %s)",
        list(zip(*map(funcao_df.get, ["nome", "orgao_superior"]))),
    )

    cargos_db = cursor.execute("SELECT id, nome, orgao_id FROM cargo").fetchall()

    cargos_db = pd.DataFrame(cargos_db, columns=["id", "nome", "orgao_id"])

    funcoes_db = cursor.execute("SELECT id, nome, orgao_id FROM funcao").fetchall()

    funcoes_db = pd.DataFrame(funcoes_db, columns=["id", "nome", "orgao_id"])

    servidor_df = viagem_df[
        ["Nome", "CPF viajante", "Cargo", "Função", "Código órgão solicitante"]
    ]

    servidor_df.columns = ["nome", "cpf", "cargo", "funcao", "orgao_id"]

    servidor_df = servidor_df.drop_duplicates()

    reverse_cargos = cargos_db.set_index(["nome", "orgao_id"]).to_dict()["id"]

    reverse_funcoes = funcoes_db.set_index(["nome", "orgao_id"]).to_dict()["id"]

    servidor_df["cargo_id"] = servidor_df.apply(
        lambda row: reverse_cargos[(row["cargo"], row["orgao_id"])]
        if (row["cargo"], row["orgao_id"]) in reverse_cargos
        else None,
        axis=1,
    )

    servidor_df["funcao_id"] = servidor_df.apply(
        lambda row: reverse_funcoes[(row["funcao"], row["orgao_id"])]
        if (row["funcao"], row["orgao_id"]) in reverse_funcoes
        else None,
        axis=1,
    )

    servidor_df = servidor_df.replace({np.nan: None})

    cursor.executemany(
        """
        INSERT INTO servidor (nome, cpf, cargo_id, funcao_id)
        VALUES (%s, %s, %s, %s)""",
        list(
            zip(
                *map(
                    servidor_df.get,
                    ["nome", "cpf", "cargo_id", "funcao_id"],
                )
            )
        ),
    )


def load_cargo(fileName):
    pass


# filenames = "pagamento.csv" "passagem.csv" "trecho.csv" "viagem.csv
def load_data(fileNames, cursor):
    # load_local(fileNames[1], cursor)
    load_servidor(fileNames[0], fileNames[3], cursor)
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
