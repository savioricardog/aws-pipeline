# B3 Investor Data Lake 📉

Este projeto implementa um **pipeline de dados Serverless** na AWS para automatizar a extração, o processamento e a catalogação de dados históricos de investimentos diretamente do portal da B3.

## 🚀 Visão Geral
O objetivo principal é transformar relatórios consolidados mensais (formato Excel) em um **Data Lake particionado** e consultável via SQL. Isso permite a realização de análises históricas de custódia, dividendos e negociações com custo operacional próximo de zero.

### Principais Tecnologias
* **Linguagem:** Python (Pandas, Awswrangler)
* **Infraestrutura:** AWS Lambda (Serverless)
* **Storage:** AWS S3 (Arquitetura de Camadas: Raw e Trusted)
* **Data Catalog:** AWS Glue & AWS Athena
* **Automação:** RPA com PyAutoGUI (Extrator)

---

## 🏗️ Arquitetura do Pipeline

1. **Ingestão (Raw):** O extrator RPA realiza o download dos arquivos `.xlsx` da B3 e efetua o upload para o bucket S3 na pasta `raw/`.
2. **Processamento (Lambda):** O upload aciona automaticamente uma função **AWS Lambda**.
3. **Transformação:** A Lambda utiliza a biblioteca `awswrangler` para limpar dados e padronizar colunas para o formato `snake_case`.
4. **Armazenamento (Trusted):** Os dados são convertidos para o formato **Parquet** e salvos de forma particionada por `year` e `month`.
5. **Consumo:** O **AWS Glue** cataloga as partições, permitindo consultas via **AWS Athena**.

## 🏗️ Arquitetura do Pipeline
![Diagrama de Arquitetura](./img/aws_pipeline.png)

---

## 📂 Estrutura do Data Lake
Os dados seguem o padrão de particionamento do **Hive**, otimizando a performance e reduzindo custos:

```plaintext
| s3://layer-raw/
| └── s3://layer-trusted/
├──    └── posicao-etf/
│          └── year=2025/
│              └── month=11/
│                  └── data.parquet
```


## 📊 Exemplo de Consulta (Athena)
SQL
SELECT 
    nome_ativo, 
    SUM(quantidade) as total_cotas
FROM "layer-trusted"."posicao_etf"
GROUP BY nome_ativo;

## 📝 Autor
Savio Ricardo Garcia

### LinkedIn: https://www.linkedin.com/in/savioricardogarcia
### GitHub: https://github.com/savioricardog/aws-pipeline

---

## 📚 Créditos e Referências

Este projeto foi desenvolvido como parte de um estudo prático e aprofundamento técnico, utilizando como base as metodologias e ensinamentos de:

* **Henrique Branco:** Pela estrutura lógica do pipeline de dados e conceitos de engenharia na AWS.
* **Documentação AWS:** Referência para configuração de Roles e gatilhos Lambda.

O código contido neste repositório foi adaptado, documentado e implementado por mim para fins de consolidação de aprendizado em MLOps e Engenharia de Dados.