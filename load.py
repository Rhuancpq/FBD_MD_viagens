import pandas as pd
import sys
import psycopg
import numpy as np
from datetime import datetime
from unidecode import unidecode


# trecos.csv head:
# "Identificador do processo de viagem ";"N�mero da Proposta (PCDP)";"Sequ�ncia Trecho";"Origem - Data";"Origem - Pa�s";"Origem - UF";"Origem - Cidade";"Destino - Data";"Destino - Pa�s";"Destino - UF";"Destino - Cidade";"Meio de transporte";"N�mero Di�rias";"Missao?"
def load_local(passagensFileName, trechoFileName, cursor):
    passagens_df = pd.read_csv(passagensFileName, sep=";", encoding="latin-1")
    trecho_df = pd.read_csv(trechoFileName, sep=";", encoding="latin-1")

    orig_ida = passagens_df[
        ["País - Origem ida", "UF - Origem ida", "Cidade - Origem ida"]
    ]
    orig_volta = passagens_df[
        ["País - Origem volta", "UF - Origem volta", "Cidade - Origem volta"]
    ]
    dest_ida = passagens_df[
        ["País - Destino ida", "UF - Destino ida", "Cidade - Destino ida"]
    ]
    dest_volta = passagens_df[
        ["Pais - Destino volta", "UF - Destino volta", "Cidade - Destino volta"]
    ]

    trecho_orig = trecho_df[["Origem - País", "Origem - UF", "Origem - Cidade"]]

    trecho_dest = trecho_df[["Destino - País", "Destino - UF", "Destino - Cidade"]]

    orig_ida.columns = ["pais", "estado", "cidade"]
    orig_volta.columns = ["pais", "estado", "cidade"]
    dest_ida.columns = ["pais", "estado", "cidade"]
    dest_volta.columns = ["pais", "estado", "cidade"]
    trecho_orig.columns = ["pais", "estado", "cidade"]
    trecho_dest.columns = ["pais", "estado", "cidade"]

    local = pd.concat(
        [orig_ida, orig_volta, dest_ida, dest_volta, trecho_orig, trecho_dest]
    )

    local = local.drop_duplicates()

    local["estado"] = np.where(local["estado"].isnull(), None, local["estado"])

    cursor.executemany(
        "INSERT INTO local (pais, estado, cidade) VALUES (%s, %s, %s)",
        list(zip(*map(local.get, ["pais", "estado", "cidade"]))),
    )


def load_viagens(pagamentoFileName, viagemFileName, cursor):
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

    group_orgs.reset_index(inplace=True, drop=True)

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

    del cargo_df
    del funcao_df

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

    del servidor_df

    servidor_db = cursor.execute(
        "SELECT id, nome, cpf, cargo_id, funcao_id FROM servidor"
    ).fetchall()

    servidor_db = pd.DataFrame(
        servidor_db, columns=["id", "nome", "cpf", "cargo_id", "funcao_id"]
    )

    reverse_servidor = servidor_db.set_index(
        ["nome", "cpf", "cargo_id", "funcao_id"]
    ).to_dict()["id"]

    viagem_df.columns = [
        "id_processo",
        "pcdp",
        "situacao",
        "urgente",
        "justificativa_urgencia",
        "orgao_superior",
        "nome_orgao_superior",
        "orgao_solicitante",
        "nome_orgao_solicitante",
        "cpf",
        "nome",
        "cargo",
        "funcao",
        "descricao_funcao",
        "data_inicio",
        "data_fim",
        "destinos",
        "motivo",
        "valor_diarias",
        "valor_passagens",
        "valor_devolucao",
        "valor_outros_gastos",
    ]

    viagem_df["cargo_id"] = viagem_df.apply(
        lambda row: reverse_cargos[(row["cargo"], row["orgao_solicitante"])]
        if (row["cargo"], row["orgao_solicitante"]) in reverse_cargos
        else None,
        axis=1,
    )

    viagem_df["funcao_id"] = viagem_df.apply(
        lambda row: reverse_funcoes[(row["funcao"], row["orgao_solicitante"])]
        if (row["funcao"], row["orgao_solicitante"]) in reverse_funcoes
        else None,
        axis=1,
    )

    viagem_df["servidor_id"] = viagem_df.apply(
        lambda row: int(
            reverse_servidor[
                (row["nome"], row["cpf"], row["cargo_id"], row["funcao_id"])
            ]
        )
        if (row["nome"], row["cpf"], row["cargo_id"], row["funcao_id"])
        in reverse_servidor
        else None,
        axis=1,
    )

    viagem_df = viagem_df.dropna(subset=["servidor_id"])

    viagem_df["servidor_id"] = viagem_df["servidor_id"].astype(int)

    viagem_df["data_inicio"] = viagem_df["data_inicio"].apply(
        lambda x: datetime.strptime(x, "%d/%m/%Y").strftime("%Y-%m-%d")
    )

    viagem_df["data_fim"] = viagem_df["data_fim"].apply(
        lambda x: datetime.strptime(x, "%d/%m/%Y").strftime("%Y-%m-%d")
    )

    viagem_df["urgente"] = viagem_df["urgente"].apply(
        lambda x: True if x == "SIM" else False
    )

    viagem_df = viagem_df.replace({np.nan: None})

    viagem_df["situacao"] = viagem_df["situacao"].apply(
        lambda x: x.replace(" ", "_").upper().replace("Ã", "A")
    )

    cursor.executemany(
        """
        INSERT INTO viagem (id_processo, situacao, urgente, justificativa_urgencia,
        servidor_id, data_inicio, data_fim, motivo, numero_proposta)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        list(
            zip(
                *map(
                    viagem_df.get,
                    [
                        "id_processo",
                        "situacao",
                        "urgente",
                        "justificativa_urgencia",
                        "servidor_id",
                        "data_inicio",
                        "data_fim",
                        "motivo",
                        "pcdp",
                    ],
                )
            )
        ),
    )

    del viagem_df
    del servidor_db
    del cargos_db
    del funcoes_db


# passagem.csv
# "Identificador do processo de viagem";"N�mero da Proposta (PCDP)";"Meio de transporte";"Pa�s - Origem ida";"UF - Origem ida";"Cidade - Origem ida";"Pa�s - Destino ida";"UF - Destino ida";"Cidade - Destino ida";"Pa�s - Origem volta";"UF - Origem volta";"Cidade - Origem volta";"Pais - Destino volta";"UF - Destino volta";"Cidade - Destino volta";"Valor da passagem";"Taxa de servi�o";"Data da emiss�o/compra";"Hora da emiss�o/compra"
def load_others(passagemFileName, trechoFileName, pagamentoFileName, cursor):
    passagem_df = pd.read_csv(passagemFileName, sep=";", encoding="latin-1")

    passagem_df.columns = [
        "id_processo",
        "pcdp",
        "meio_transporte",
        "pais_origem_ida",
        "uf_origem_ida",
        "cidade_origem_ida",
        "pais_destino_ida",
        "uf_destino_ida",
        "cidade_destino_ida",
        "pais_origem_volta",
        "uf_origem_volta",
        "cidade_origem_volta",
        "pais_destino_volta",
        "uf_destino_volta",
        "cidade_destino_volta",
        "valor_passagem",
        "taxa_servico",
        "data_emissao",
        "hora_emissao",
    ]

    passagem_df = passagem_df.replace({np.nan: None})

    passagem_df["data_hora_emissao"] = passagem_df.apply(
        lambda row: datetime.strptime(
            row["data_emissao"] + " " + row["hora_emissao"], "%d/%m/%Y %H:%M"
        ).strftime("%Y-%m-%d %H:%M:%S")
        if row["data_emissao"] is not None and row["hora_emissao"] is not None
        else None,
        axis=1,
    )

    local_db = cursor.execute("SELECT id, cidade, estado, pais FROM local")

    reverse_local = {}

    for local in local_db:
        reverse_local[(local[1], local[2], local[3])] = local[0]

    passagem_df["local_origem_ida_id"] = passagem_df.apply(
        lambda row: reverse_local[
            (row["cidade_origem_ida"], row["uf_origem_ida"], row["pais_origem_ida"])
        ]
        if (
            row["cidade_origem_ida"],
            row["uf_origem_ida"],
            row["pais_origem_ida"],
        )
        in reverse_local
        else None,
        axis=1,
    )

    passagem_df["local_destino_ida_id"] = passagem_df.apply(
        lambda row: reverse_local[
            (row["cidade_destino_ida"], row["uf_destino_ida"], row["pais_destino_ida"])
        ]
        if (
            row["cidade_destino_ida"],
            row["uf_destino_ida"],
            row["pais_destino_ida"],
        )
        in reverse_local
        else None,
        axis=1,
    )

    passagem_df["local_origem_volta_id"] = passagem_df.apply(
        lambda row: reverse_local[
            (
                row["cidade_origem_volta"],
                row["uf_origem_volta"],
                row["pais_origem_volta"],
            )
        ]
        if (
            row["cidade_origem_volta"],
            row["uf_origem_volta"],
            row["pais_origem_volta"],
        )
        in reverse_local
        else None,
        axis=1,
    )

    passagem_df["local_destino_volta_id"] = passagem_df.apply(
        lambda row: reverse_local[
            (
                row["cidade_destino_volta"],
                row["uf_destino_volta"],
                row["pais_destino_volta"],
            )
        ]
        if (
            row["cidade_destino_volta"],
            row["uf_destino_volta"],
            row["pais_destino_volta"],
        )
        in reverse_local
        else None,
        axis=1,
    )

    viagem_db = cursor.execute("SELECT id, id_processo FROM viagem")

    reverse_viagem = {}

    for viagem in viagem_db:
        reverse_viagem[viagem[1]] = viagem[0]

    passagem_df["viagem_id"] = passagem_df.apply(
        lambda row: reverse_viagem[str(row["id_processo"])]
        if str(row["id_processo"]) in reverse_viagem
        else None,
        axis=1,
    )

    passagem_df = passagem_df.dropna(subset=["viagem_id"])

    passagem_df["viagem_id"] = passagem_df["viagem_id"].astype(int)

    passagem_df["valor_passagem"] = passagem_df["valor_passagem"].str.replace(",", ".")

    passagem_df["valor_passagem"] = passagem_df["valor_passagem"].astype(float)

    passagem_df["taxa_servico"] = passagem_df["taxa_servico"].str.replace(",", ".")

    passagem_df["taxa_servico"] = passagem_df["taxa_servico"].astype(float)

    passagem_df["meio_transporte"] = passagem_df["meio_transporte"].apply(
        # upper, replace spaces with underscores and remove accents
        lambda x: unidecode(x.upper()).replace(" ", "_")
    )

    passagem_df = passagem_df.replace({np.nan: None})

    cursor.executemany(
        """
        INSERT INTO passagem (viagem_id, local_origem_ida_id,
        local_destino_ida_id, local_origem_volta_id, local_destino_volta_id,
        valor, taxa_servico, data_hora_emissao, meio_transporte)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        list(
            zip(
                *map(
                    passagem_df.get,
                    [
                        "viagem_id",
                        "local_origem_ida_id",
                        "local_destino_ida_id",
                        "local_origem_volta_id",
                        "local_destino_volta_id",
                        "valor_passagem",
                        "taxa_servico",
                        "data_hora_emissao",
                        "meio_transporte",
                    ],
                )
            )
        ),
    )

    trecho_df = pd.read_csv(trechoFileName, sep=";", encoding="latin-1")

    # "Identificador do processo de viagem ";"N�mero da Proposta (PCDP)";"Sequ�ncia Trecho";"Origem - Data";"Origem - Pa�s";"Origem - UF";"Origem - Cidade";"Destino - Data";"Destino - Pa�s";"Destino - UF";"Destino - Cidade";"Meio de transporte";"N�mero Di�rias";"Missao?"

    trecho_df.columns = [
        "id_processo",
        "pcdp",
        "sequencia_trecho",
        "data_origem",
        "pais_origem",
        "uf_origem",
        "cidade_origem",
        "data_destino",
        "pais_destino",
        "uf_destino",
        "cidade_destino",
        "meio_transporte",
        "numero_diarias",
        "missao",
    ]

    trecho_df["data_origem"] = trecho_df["data_origem"].apply(
        lambda x: datetime.strptime(x, "%d/%m/%Y").date()
    )

    trecho_df["data_destino"] = trecho_df["data_destino"].apply(
        lambda x: datetime.strptime(x, "%d/%m/%Y").date()
    )

    trecho_df["meio_transporte"] = trecho_df["meio_transporte"].apply(
        # upper, replace spaces with underscores and remove accents
        lambda x: unidecode(x.upper()).replace(" ", "_")
    )

    trecho_df["missao"] = trecho_df["missao"].apply(
        lambda x: True if x == "Sim" else False
    )

    trecho_df = trecho_df.replace({np.nan: None})

    trecho_df["viagem_id"] = trecho_df.apply(
        lambda row: reverse_viagem[str(row["id_processo"])]
        if str(row["id_processo"]) in reverse_viagem
        else None,
        axis=1,
    )

    trecho_df = trecho_df.dropna(subset=["viagem_id"])

    trecho_df["viagem_id"] = trecho_df["viagem_id"].astype(int)

    trecho_df["local_origem_id"] = trecho_df.apply(
        lambda row: reverse_local[
            (
                row["cidade_origem"],
                row["uf_origem"],
                row["pais_origem"],
            )
        ]
        if (
            row["cidade_origem"],
            row["uf_origem"],
            row["pais_origem"],
        )
        in reverse_local
        else None,
        axis=1,
    )

    trecho_df["local_destino_id"] = trecho_df.apply(
        lambda row: reverse_local[
            (
                row["cidade_destino"],
                row["uf_destino"],
                row["pais_destino"],
            )
        ]
        if (
            row["cidade_destino"],
            row["uf_destino"],
            row["pais_destino"],
        )
        in reverse_local
        else None,
        axis=1,
    )

    trecho_df = trecho_df.dropna(subset=["local_origem_id", "local_destino_id"])

    trecho_df["local_origem_id"] = trecho_df["local_origem_id"].astype(int)

    trecho_df["local_destino_id"] = trecho_df["local_destino_id"].astype(int)

    trecho_df["numero_diarias"] = trecho_df["numero_diarias"].str.replace(",", ".")

    trecho_df["numero_diarias"] = trecho_df["numero_diarias"].astype(float)

    cursor.executemany(
        """
        INSERT INTO trecho (viagem_id, local_origem_id, local_destino_id,
        data_origem, data_destino, meio_transporte, numero_diarias, missao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        list(
            zip(
                *map(
                    trecho_df.get,
                    [
                        "viagem_id",
                        "local_origem_id",
                        "local_destino_id",
                        "data_origem",
                        "data_destino",
                        "meio_transporte",
                        "numero_diarias",
                        "missao",
                    ],
                )
            )
        ),
    )

    pagamento_df = pd.read_csv(pagamentoFileName, sep=";", encoding="latin-1")

    # "Identificador do processo de viagem";"N�mero da Proposta (PCDP)";"C�digo do �rg�o superior";"Nome do �rg�o superior";"Codigo do �rg�o pagador";"Nome do �rgao pagador";"C�digo da unidade gestora pagadora";"Nome da unidade gestora pagadora";"Tipo de pagamento";"Valor"

    pagamento_df.columns = [
        "id_processo",
        "pcdp",
        "codigo_orgao_superior",
        "nome_orgao_superior",
        "codigo_orgao_pagador",
        "nome_orgao_pagador",
        "codigo_unidade_gestora_pagadora",
        "nome_unidade_gestora_pagadora",
        "tipo_pagamento",
        "valor",
    ]

    pagamento_df = pagamento_df.replace({np.nan: None})

    pagamento_df["viagem_id"] = pagamento_df.apply(
        lambda row: reverse_viagem[str(row["id_processo"])]
        if str(row["id_processo"]) in reverse_viagem
        else None,
        axis=1,
    )

    pagamento_df = pagamento_df.dropna(subset=["viagem_id"])

    pagamento_df["viagem_id"] = pagamento_df["viagem_id"].astype(int)

    pagamento_df["valor"] = pagamento_df["valor"].str.replace(",", ".")

    pagamento_df["valor"] = pagamento_df["valor"].astype(float)

    pagamento_df["orgao_pagador_id"] = pagamento_df.apply(
        lambda row: row["codigo_orgao_pagador"]
        if row["codigo_orgao_pagador"] is not None
        else None,
        axis=1,
    )

    pagamento_df = pagamento_df.dropna(subset=["orgao_pagador_id"])

    pagamento_df["tipo_pagamento"] = pagamento_df["tipo_pagamento"].apply(
        # upper, replace spaces with underscores and remove accents
        lambda x: unidecode(x.upper())
        .replace(" ", "_")
        .replace(":", "")
    )

    cursor.executemany(
        """
        INSERT INTO pagamento (viagem_id, orgao_pagador_id, tipo_pagamento, valor)
        VALUES (%s, %s, %s, %s)
        """,
        list(
            zip(
                *map(
                    pagamento_df.get,
                    [
                        "viagem_id",
                        "orgao_pagador_id",
                        "tipo_pagamento",
                        "valor",
                    ],
                )
            )
        ),
    )


# filenames = "pagamento.csv" "passagem.csv" "trecho.csv" "viagem.csv
def load_data(fileNames, cursor, conn):
    load_local(fileNames[1], fileNames[2], cursor)
    conn.commit()
    load_viagens(fileNames[0], fileNames[3], cursor)
    conn.commit()
    load_others(fileNames[1], fileNames[2], fileNames[0], cursor)


# filenames = "pagamento.csv" "passagem.csv" "trecho.csv" "viagem.csv em ordem
# python3 load.py 2022_Pagamento.csv 2022_Passagem.csv 2022_Trecho.csv 2022_Viagem.csv postgresql://postgres:ppcaunb@206.189.206.44:5454/viagens 
if __name__ == "__main__":
    # get from command line
    fileNames = sys.argv[1:-1]
    conn_url = sys.argv[-1]
    cursor = {}
    with psycopg.connect(conn_url) as conn:
        with conn.cursor() as cursor:
            load_data(fileNames, cursor, conn)

# filenames = "pagamento.csv" "passagem.csv" "trecho.csv" "viagem.csv em ordem
# python3 load.py 2022_Pagamento.csv 2022_Passagem.csv 2022_Trecho.csv 2022_Viagem.csv postgresql://postgres:example@localhost:5432/viagens
# if __name__ == "__main__":
#     # get from command line
#     fileNames = sys.argv[1:-1]
#     conn_url = sys.argv[-1]
#     cursor = {}
#     with psycopg.connect(conn_url) as conn:
#         with conn.cursor() as cursor:
#             load_data(fileNames, cursor, conn)
