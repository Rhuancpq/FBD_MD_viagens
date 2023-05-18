CREATE DATABASE "viagens";

\c viagens

CREATE TYPE "situacao_viagem" AS ENUM (
  'REALIZADA',
  'NAO_REALIZADA'
);

CREATE TYPE "meio_transporte" AS ENUM (
  'AEREO',
  'RODOVIARIO',
  'FLUVIAL',
  'VEICULO_PROPRIO',
  'VEICULO_OFICIAL'
);

CREATE TYPE "tipo_pagamento" AS ENUM (
  'PASSAGEM',
  'DIARIAS',
  'SEGURO'
);

CREATE TABLE "local" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "cidade" varchar NOT NULL,
  "estado" varchar,
  "pais" varchar NOT NULL
);

CREATE TABLE "orgao_superior" (
  "codigo" integer PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "nome" varchar NOT NULL
);

CREATE TABLE "orgao_subordinado" (
  "codigo" integer PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "nome" varchar NOT NULL,
  "orgao_superior" integer,
  CONSTRAINT orgao_orgao_sup_fk FOREIGN KEY ("orgao_superior") REFERENCES "orgao_superior" ("codigo")
);

CREATE TABLE "cargo" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "nome" varchar NOT NULL,
  "orgao_id" integer NOT NULL,
  FOREIGN KEY ("orgao_id") REFERENCES "orgao_subordinado" ("codigo")
);

CREATE TABLE "funcao" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "nome" varchar NOT NULL,
  "orgao_id" integer NOT NULL,
  FOREIGN KEY ("orgao_id") REFERENCES "orgao_subordinado" ("codigo")
);

CREATE TABLE "servidor" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "cpf" varchar,
  "nome" varchar NOT NULL,
  "cargo_id" integer,
  "funcao_id" integer,
  FOREIGN KEY ("cargo_id") REFERENCES "cargo" ("id"),
  FOREIGN KEY ("funcao_id") REFERENCES "funcao" ("id")
);

CREATE TABLE "viagem" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "id_processo" varchar UNIQUE NOT NULL,
  "numero_proposta" varchar NOT NULL,
  "situacao" situacao_viagem NOT NULL,
  "urgente" boolean NOT NULL DEFAULT false,
  "justificativa_urgencia" varchar(1000),
  "servidor_id" integer NOT NULL,
  "data_inicio" date NOT NULL,
  "data_fim" date NOT NULL,
  "motivo" varchar(1000),
  FOREIGN KEY ("servidor_id") REFERENCES "servidor" ("id")
);

CREATE TABLE "passagem" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "viagem_id" integer NOT NULL,
  "local_origem_ida_id" integer,
  "local_destino_ida_id" integer,
  "local_origem_volta_id" integer,
  "local_destino_volta_id" integer,
  "valor" float NOT NULL DEFAULT 0,
  "taxa_servico" float NOT NULL DEFAULT 0,
  "data_hora_emissao" timestamp,
  FOREIGN KEY ("viagem_id") REFERENCES "viagem" ("id") ON DELETE CASCADE,
  FOREIGN KEY ("local_origem_ida_id") REFERENCES "local" ("id"),
  FOREIGN KEY ("local_origem_volta_id") REFERENCES "local" ("id"),
  FOREIGN KEY ("local_destino_ida_id") REFERENCES "local" ("id"),
  FOREIGN KEY ("local_destino_volta_id") REFERENCES "local" ("id")
);

CREATE TABLE "trecho" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "local_origem_id" integer NOT NULL,
  "data_origem" date NOT NULL,
  "local_destino_id" integer NOT NULL,
  "data_destino" date NOT NULL,
  "meio_tranposrte" meio_transporte,
  "numero_diarias" float,
  "missao" boolean NOT NULL DEFAULT false,
  FOREIGN KEY ("local_origem_id") REFERENCES "local" ("id"),
  FOREIGN KEY ("local_destino_id") REFERENCES "local" ("id")
);

CREATE TABLE "pagamento" (
  "id" SERIAL PRIMARY KEY,
  "data_hora_criacao" timestamp DEFAULT current_timestamp,
  "viagem_id" integer NOT NULL,
  "orgao_pagador_id" integer NOT NULL,
  "tipo_pagamento" tipo_pagamento,
  "valor" float NOT NULL DEFAULT 0,
  FOREIGN KEY ("viagem_id") REFERENCES "viagem" ("id") ON DELETE CASCADE,
  FOREIGN KEY ("orgao_pagador_id") REFERENCES "orgao_subordinado" ("codigo")
);
