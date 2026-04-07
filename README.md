B3 Investor Data Lake 📉
Este projeto implementa um pipeline de dados Serverless na AWS para automatizar a extração, processamento e catalogação de dados históricos de investimentos diretamente do portal da B3.

🚀 Visão Geral
O objetivo é transformar relatórios consolidados mensais (Excel) em um Data Lake particionado e consultável via SQL, permitindo análises históricas de custódia, dividendos e negociações com custo operacional próximo de zero.

Principais Tecnologias
Linguagem: Python (Pandas, Awswrangler)

Infraestrutura: AWS Lambda (Serverless)

Storage: AWS S3 (Camadas Raw e Trusted)

Data Catalog: AWS Glue & AWS Athena

Automação: RPA com PyAutoGUI (Extrator)

🏗️ Arquitetura do Pipeline
Ingestão (Raw): O extrator RPA baixa os arquivos .xlsx da B3 e faz o upload para o bucket S3 na pasta raw/.

Processamento (Lambda): O upload dispara automaticamente uma função AWS Lambda.

Transformação: A Lambda utiliza awswrangler para limpar os dados, padronizar colunas (snake_case), remover acentos e tratar totalizadores de planilhas.

Armazenamento (Trusted): Os dados são salvos em formato Parquet, particionados por year e month no bucket trusted/.

Consumo: O AWS Glue cataloga as partições, permitindo consultas de alta performance via AWS Athena usando SQL padrão.

📂 Estrutura do Data Lake
Os dados são organizados seguindo o padrão de particionamento do Hive para otimização de custos e performance no Athena:

Plaintext
s3://project-trusted-b3/
├── posicao-etf/
│   └── year=2025/
│       └── month=11/
│           └── data.parquet
├── posicao-fundos/
├── negociacoes/
└── proventos-recebidos/
🛠️ Como Executar
Pré-requisitos
Conta ativa na AWS.

Python 3.9+ instalado.

AWS CLI configurado.

Instalação
Clone o repositório:

Bash
git clone https://github.com/seu-usuario/b3-investor-datalake.git
Instale as dependências:

Bash
pip install awswrangler pandas openpyxl
Deploy da Lambda
Crie uma Layer na AWS Lambda contendo a biblioteca awswrangler.

Configure o gatilho (Trigger) do S3 para eventos de All object create events no bucket de origem.

Certifique-se de que a Role da Lambda possua permissões de leitura/escrita nos buckets S3 envolvidos.

📊 Exemplo de Consulta (Athena)
Com os dados na camada Trusted, você pode realizar queries como esta:

SQL
SELECT 
    nome_ativo, 
    SUM(quantidade) as total_cotas,
    SUM(valor_atualizado) as patrimonio_total
FROM "trusted_db"."posicao_etf"
WHERE year = '2025' AND month = '11'
GROUP BY nome_ativo
ORDER BY patrimonio_total DESC;
⚖️ Considerações Legais
Este projeto utiliza automação de interface gráfica (GUI Automation) para extração de dados, respeitando os termos de uso ao não realizar engenharia reversa em APIs privadas e evitando sobrecarga nos servidores através de acessos mimetizando o comportamento humano.

📝 Autor
Seu Nome - Seu LinkedIn