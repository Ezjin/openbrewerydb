# openbrewerydb
Case Bees - https://www.openbrewerydb.org/

Pipeline para extração de dados de cervejarias

## Diretórios

```
openbrewerydb/
│
├── config/            # Configuração do Airflow
├── dags/     
|   ├── utils/           
├── data_lake_mock/    # código fonte
│   ├── raw/            # Camada Raw (Bronze) 
│   ├── silver/         # Camada Silver
│   └── gold/           # Camada Gold
├── logs/ 
├── plugins/ 
├── tests/ 
|  

```

## Comandos iniciais
Se for a primeira vez que está rodando o docker compose no projeto:
```
docker compose run airflow-cli airflow config list
```
para gerar o arquivo airflow.cfg genérico.

Depois:
```
docker compose up -d
```
Para subir os containers do airflow.