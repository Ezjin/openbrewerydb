# Projeto OpenBreweryDB com Airflow

## Visão Geral
Este projeto implementa um pipeline de dados orquestrado com Apache Airflow, simulando a ingestão e transformação de informações da [Open Brewery DB API](https://www.openbrewerydb.org/).  
O objetivo é demonstrar boas práticas na construção de um data lake por camadas (Raw, Silver e Gold), com testes automatizados e execução em ambiente conteinerizado.

---

## Como Iniciar o Projeto

### Pré-requisitos
- Docker e Docker Compose instalados.

### Subindo o ambiente
```bash
docker-compose up -d
```

Isso criará os containers do Airflow (webserver, scheduler, postgres e redis).  
O Airflow estará disponível em: [http://localhost:8080](http://localhost:8080)  
Usuário e senha padrão: `airflow / airflow`.

### Estrutura de dados
Os dados são armazenados em `data_lake_mock/`:
- `raw/` → dados brutos extraídos da API.  
- `silver/` → dados limpos e normalizados.  
- `gold/` → agregações finais para análise (número de cervejarias por cidade, estado e país).  

---

## Estrutura do Repositório

```
openbrewerydb-main/
│── dags/                  # DAGs do Airflow
│   ├── dag_extracao_brewery.py
│   ├── dag_transformation_silver.py
│   ├── dag_transformation_gold.py
│   └── utils/             # Funções auxiliares (pipelines, normalização, etc.)
│
│── data_lake_mock/        # Estrutura simulada de Data Lake
│── tests/                 # Testes unitários e de DAGs
│── Dockerfile             # Imagem base do Airflow customizada
│── docker-compose.yml     # Orquestração dos containers
```

---

## Decisões de Arquitetura

1. **Orquestração com Airflow**    
   As DAGs foram divididas de forma modular para separar extração, transformação Silver e transformação Gold.
   - Como os dados são atualizados pela comunidade, o pipeline está configurado para rodar toda semana ("@weekly").
   - Desse modo é possível agrupar diferentes updates durante a semana antes de rodar novamente.
   
   Sobre as DAGS:
   - dag_extracao_brewery.py
      - Primeiro checa quantas cervejarias estão disponíveis no metadado.
      - Faz o calculo de quantas páginas são necessárias para fazer o get de todos os dados disponíveis.
      - Com o número de páginas, faz o get de todas as páginas salvando em arquivos .json separados.
   - dag_transformation_silver.py
      - Consome os arquivos .json criado na dag anterior. Separa o processamento em batchs de 10 arquivos para evitar uso excessivo de memória.
      - Cria/Update as tabelas dimensões no diretório silver/dim. Fazendo a normalização de todas as combinações de país, estado e cidade.
      - Faz a normalização das colunas ["country", "state", "city", "brewery_type"] 
      - Salva os arquivos particionando por pais, estado e part.
   - dag_transformation_gold.py
      - Consome a Silver layer para fazer a agregação das cervejarias em um único arquivo, separados por batch.

2. **Camadas do Data Lake (Raw, Silver, Gold)**   
   - Raw: preservação dos dados brutos e históricos.
   - Silver: dados tratados e organizados.  
      - Foi feita a normalização das linhas e colunas utilizando tabelas dimensões.
         - Normalizado para caixa baixa
         - Remoção de acentos
         - Caractéres especiais
         - Espaços no meio, inicio e fim
      - Para a limpeza de duplicados foram consideradas as colunas ["name", "country", "state", "city" , "brewery_type"].
      - O critério de desempate foi a quantidade de colunas extras preenchidas.
   - Gold: dados agregados e prontos para análise.
      - Foi considerado localidade a combinação de ["country", "state", "city", "brewery_type"] ignorando diferentes unidades na mesma cidade.

3. **Particionamento por execução**
   Todas as camadas são particionadas por batch de execução para facilitar auditoria, comparação de execuções e reprocessamento.
   - A raw (bronze) está organizada raw/year=xx/month=yy/day=zz/*.json
   - A silver está organizada em:
      - silver/dim/*.parquet (com as tabelas dimensões: dim_city, dim_state, dim_country e dim_brewery_type)
      - silve/fact/batch=YYYY-MM-DD/country=yy/state=xx/part=zz/*.parquet
   - A gold está organizada gold/batch=YYYY-MM-DD/total.parquet

4. **Testes Automatizados**  
   O repositório inclui testes com `pytest`, cobrindo tanto funções utilitárias quanto DAGs.  

5. **Execução em Containers**  
   O uso de `Dockerfile` e `docker-compose.yml` garante um setup reprodutível.  

---

## Próximos Passos (Evoluções Possíveis)

- Conectar o pipeline a um data warehouse real (exemplo: Snowflake, BigQuery, Redshift).  
- Criar dashboards de consumo a partir da camada Gold.  
- Implementar monitoramento e alertas de execução.  
- Incluir versionamento de dados e metadados para maior rastreabilidade.

---

## Sobre os dados

# Colunas 
- Camada Raw (JSON)
```
   [{
        "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
        "name": "(405) Brewing Co",
        "brewery_type": "micro",
        "address_1": "1716 Topeka St",
        "address_2": null,
        "address_3": null,
        "city": "Norman",
        "state_province": "Oklahoma",
        "postal_code": "73069-8224",
        "country": "United States",
        "longitude": -97.46818222,
        "latitude": 35.25738891,
        "phone": "4058160490",
        "website_url": "http://www.405brewing.com",
        "state": "Oklahoma",
        "street": "1716 Topeka St"
    },
    
    ]
```

- Camada Silver

```
   coluna         tipo     descrição
   id	            string	identificador da brewery
   name	         string	nome
   brewery_type	string	tipo (ex.: micro, regional, brewpub)
   street	      string	logradouro
   city	         string	cidade (normalizada)
   state	         string	estado/província (normalizado)
   postal_code	   string	CEP/código postal
   country	      string	país (normalizado)
   latitude	      float64	latitude
   longitude	   float64	longitude
   phone	         string	telefone
   website_url	   string	site
```

- Camada Gold
```
   country        state      city        breweries_count
   united states  oklahoma   norman      12
   united states  california san_diego   45
   brazil         sao_paulo  campinas    10
```