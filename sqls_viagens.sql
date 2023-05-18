
-- CONSULTAS


-- TOP 10 ÓRGÃOS QUE MAIS GASTARAM COM PASSAGENS, DIÁRIAS E SEGUROS EMITIDOS EM 2022
select org.nome, sum(pag.valor) as gasto_total 
from pagamento pag
	inner join orgao org on pag.orgao_pagador_id = org.id
	inner join viagem viag on pag.viagem_id = viag.id
	inner join passagem pas on pas.viagem_id = viag.id
where date_part('year', pas.data_hora_emissao) = 2022
group by org.nome
having sum(pag.valor) > 0
order by gasto_total desc
limit 10;


-- TOP 10 ÓRGÃOS QUE MAIS GASTARAM COM PASSAGENS EMITIDAS EM 2022
select org.nome, sum(pag.valor) as gasto_total 
from pagamento pag
	inner join orgao org on pag.orgao_pagador_id = org.id
	inner join viagem viag on pag.viagem_id = viag.id
	inner join passagem pas on pas.viagem_id = viag.id
where pag.tipo_pagamento = 'PASSAGEM' and date_part('year', pas.data_hora_emissao) = 2022
group by org.nome
having sum(pag.valor) > 0
order by gasto_total desc
limit 10;


-- TOP 10 ÓRGÃOS QUE MAIS GASTARAM COM DIÁRIAS EMITIDAS EM 2022
select org.nome, sum(pag.valor) as gasto_total 
from pagamento pag
	inner join orgao org on pag.orgao_pagador_id = org.id
	inner join viagem viag on pag.viagem_id = viag.id
	inner join passagem pas on pas.viagem_id = viag.id
where pag.tipo_pagamento = 'DIARIAS' and date_part('year', pas.data_hora_emissao) = 2022
group by org.nome
having sum(pag.valor) > 0
order by gasto_total desc
limit 10;


-- TOP 10 ÓRGÃOS COM MAIS PASSAGENS EMITIDAS EM URGÊNCIA
select org.nome, count(viag.urgente) as total_urgencias
from viagem viag
	inner join servidor ser on viag.servidor_id = ser.id 
	inner join orgao org on ser.orgao_id = org.id 
where viag.urgente = true
group by org.nome
having count(viag.urgente) > 0
order by total_urgencias desc
limit 10;


-- TOP 10 SERVIDORES QUE MAIS REALIZARAM VIAGENS COM PASSAGENS EMITIDAS EM 2022
select ser.nome, count(viag.id) as qtd_viagens
from viagem viag
	inner join passagem pas on pas.viagem_id = viag.id
	inner join servidor ser on viag.servidor_id = ser.id
where date_part('year', pas.data_hora_emissao) = 2022
group by ser.nome
having count(viag.id) > 0
order by qtd_viagens desc
limit 10;     


-- TOP 10 SERVIDORES QUE MAIS REALIZARAM VIAGENS
select ser.nome, count(viag.id) as qtd_viagens
from viagem viag
	inner join passagem pas on pas.viagem_id = viag.id
	inner join servidor ser on viag.servidor_id = ser.id
group by ser.nome
having count(viag.id) > 0
order by qtd_viagens desc
limit 10;     


-- TOP 10 DESTINOS DE VIAGENS
select destinos.destino, count(destinos.destino) as quantidade 
from 
(
	select concat(destino_ida.cidade, ' - ',destino_ida.estado, ' - ', destino_ida.pais) as destino
	from viagem viag
		inner join passagem pas on pas.viagem_id = viag.id
		inner join "local" destino_ida on pas.local_destino_ida_id = destino_ida.id
		
	union all
	
	select concat(destino_volta.cidade, ' - ',destino_volta.estado, ' - ', destino_volta.pais) as destino
	from viagem viag
		inner join passagem pas on pas.viagem_id = viag.id
		inner join "local" destino_volta on pas.local_destino_volta_id = destino_volta.id
) as destinos
group by destinos.destino
having count(destinos.destino) > 0
order by quantidade desc
limit 10; 


-- VALOR MÉDIO DAS PASSAGENS EMITIDAS DE BRASÍLIA A SÃO PAULO EM 2022 e 2023
select avg(pass.valor) as media_valor
from viagem viag 
	inner join passagem pass on pass.viagem_id = viag.id
    inner join "local" origem_ida on pass.local_origem_ida_id = origem_ida.id 
	inner join "local" destino_ida on pass.local_origem_ida_id = destino_ida.id 
	inner join "local" origem_volta on pass.local_origem_ida_id = origem_volta.id 
	inner join "local" destino_volta on pass.local_origem_ida_id = destino_volta.id
where date_part('year', pass.data_hora_emissao) in (2022, 2023)
      and (
      	  	origem_ida.id = (select id from "local" l where l.cidade = 'Brasília' and l.estado = 'DF' and l.pais = 'Brasil')
          	and destino_ida.id = (select id from "local" l where l.cidade = 'São Paulo' and l.estado = 'SP' and l.pais = 'Brasil')
          ) 
          or
          (
      	  	origem_volta.id = (select id from "local" l where l.cidade = 'Brasília' and l.estado = 'DF' and l.pais = 'Brasil')
          	and destino_volta.id = (select id from "local" l where l.cidade = 'São Paulo' and l.estado = 'SP' and l.pais = 'Brasil')
          ); 
         

         
         
         
-- PROCEDURE


-- CRIA PROCEDURE QUE CALCULA E GRAVA A TAXA DE SERVIÇO DE UMA PASSAGEM COM BASE EM UM PERCENTUAL ESPECÍFICO DO VALOR DA PASSAGEM
create or replace procedure calcular_gravar_taxa_servico(id_passagem int, percentual int)
language plpgsql as $$
begin
    update passagem  
    set taxa_servico = valor * (percentual / 100)
    where id = id_passagem 
          and valor is not null;
    commit;
end;
$$;

call calcular_gravar_taxa_servico(20);




-- FUNCTION

-- CRIA FUNCTION QUE RETORNA OS PAGAMENTOS DE PASSAGEM, DIÁRIA OU SEGURO, O QUE HOUVER, REALIZADOS PARA UMA VIAGEM
create or replace function pagamentos_viagem(id_viagem int)
returns table(tipo_pagamento int, valor float)
language plpgsql as $$
begin
    return query select pag.tipo_pagamento, pag.valor
                 from pagamento as pag
                 where pag.viagem_id = id_viagem;
end;
$$;

-- INVOCA A FUNCTION CRIADA PARA A VIAGEM DE ID 50
call pagamentos_viagem(50); 


-- CRIA FUNCTION QUE RETORNA TRIGGER QUE DESCONSIDERA VALORES NEGATIVOS NO VALOR E TAXA DE SERVIÇO DE PASSAGENS 
create or replace function desconsiderar_valores_negativos_passagem()
returns trigger
language plpgsql as $$
begin
	update passagem  
    set valor = 0 
    where valor is not null 
          and valor < 0;
    update passagem  
    set taxa_servico = 0 
    where taxa_servico is not null 
          and taxa_servico < 0;
    commit;
end;
$$;

-- CRIA FUNCTION QUE RETORNA TRIGGER QUE DESCONSIDERA VALORES NEGATIVOS EM PAGAMENTOS
create or replace function desconsiderar_valores_negativos_pagamento()
returns trigger
language plpgsql as $$
begin
	update pagamento  
    set valor = 0 
    where valor is not null 
          and valor < 0;
    commit;
end;
$$;




-- TRIGGER

-- CRIA TRIGGER QUE INVOCA FUNCTION PARA DESCONSIDERAR VALORES NEGATIVOS 
-- DE VALOR E TAXA DE SERVIÇO SEMPRE QUE FOR INSERIDA OU ATUALIZADA UMA PASSAGEM
create trigger triguer_desconsiderar_valores_negativos_passagem
after insert or update on passagem
for each row
execute function desconsiderar_valores_negativos_passagem();


-- CRIA TRIGGER QUE INVOCA FUNCTION PARA DESCONSIDERAR VALORES NEGATIVOS 
-- DE VALOR DE PAGAMENTO SEMPRE QUE FOR INSERIDO OU ATUALIZADO UM PAGAMENTO
create trigger triguer_desconsiderar_valores_negativos_pagamento
after insert or update on pagamento
for each row
execute function desconsiderar_valores_negativos_pagamento();



-- VIEW

-- CRIA VIEW QUE EXIBE DADOS DETALHADOS DE PASSAGENS DE VIAGENS EMITIDAS PARA SERVIDORES PÚBLICOS EM VIAGEM
create or replace view viagens_servidores as
select v.id_processo,
	   v.numero_proposta,
	    case 
	   		when v.situacao = 'REALIZADA' then 'REALIZADA'
	   		when v.situacao = 'NAO_REALIZADA' then 'NÃO REALIZADA'
	   		else 'NÃO REALIZADA'
	   end as situacao,
	   case 
	   		when v.urgente = true then 'SIM'
	   		else 'NÃO'
	   end as urgente,
	   to_char(v.data_inicio, 'DD/MM/YYYY') as data_inicio,
	   to_char(v.data_fim, 'DD/MM/YYYY') as data_fim,
	   upper(s.nome) as nome_servidor,
	   upper(c.nome) as nome_cargo,
	   upper(o.nome) as nome_orgao,
	   to_char(p.data_hora_emissao, 'DD/MM/YYYY HH12:MI:SS') as data_hora_emissao_passagem,
	   concat(origem_ida.cidade, ' - ',origem_ida.estado, ' - ', origem_ida.pais) as origem_ida,
	   concat(destino_ida.cidade, ' - ',destino_ida.estado, ' - ', destino_ida.pais) as destino_ida,
	   concat(origem_volta.cidade, ' - ',origem_volta.estado, ' - ', origem_volta.pais) as origem_volta,
	   concat(destino_volta.cidade, ' - ',destino_volta.estado, ' - ', destino_volta.pais) as destino_volta,
	   p.valor,
	   p.taxa_servico,
	   p.valor + p.taxa_servico as soma_total_valores
from viagem v
	 inner join servidor s on v.servidor_id = s.id
	 left join cargo c on v.servidor_id  = c.id
	 left join orgao o on s.orgao_id  = o.id
	 inner join passagem p on p.viagem_id = v.id
	 inner join "local" origem_ida on p.local_origem_ida_id = origem_ida.id 
	 inner join "local" destino_ida on p.local_origem_ida_id = destino_ida.id 
	 inner join "local" origem_volta on p.local_origem_ida_id = origem_volta.id 
	 inner join "local" destino_volta on p.local_origem_ida_id = destino_volta.id;
