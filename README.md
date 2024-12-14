# BEES Data Engineering – Breweries Case

## Objetivo
O objetivo deste projeto é demonstrar habilidades em consumir dados de uma API, transformá-los e persistí-los em um data lake seguindo a arquitetura medallion com três camadas: dados brutos, dados curados particionados por localização e uma camada analítica agregada.

## Stack Utilizada
- **Airflow**: Orquestração de pipeline de dados.
- **Python**: Para consumo da API, transformação e persistência de dados.
- **MinIO**: Armazenamento de dados no formato de data lake.
- **Docker**: Para containerização da aplicação.

## Arquitetura do Data Lake
O projeto segue a arquitetura medallion com as seguintes camadas:
1. **Bronze Layer (Raw Data)**: Dados brutos consumidos da API Open Brewery DB e persistidos em formato nativo.
2. **Silver Layer (Curated Data)**: Dados transformados para o formato Parquet, particionados por localização da cervejaria.
3. **Gold Layer (Aggregated Data)**: Dados agregados com a quantidade de cervejarias por tipo e localização.

## Como Rodar o Projeto

### Pré-requisitos
1. Docker
2. Python 3.x
3. MinIO

**Primeiramente, tenha o ambiente do docker instalado e configurado na sua máquina, e verifique as portas de rede sendo utilizadas**
**Será necessário ter a porta 8080 aberta para acessar o airflow**
**E a porta 9002 aberta para acessar o minio**

Clone o projeto

```bash
  git clone https://github.com/luishbastos/ABINBEV-CASE.git
```

Instale as dependências

```bash
  pip install -r requirements.txt
```

Identifique o diretório com o arquivo docker-compose e rode o comando:

```bash
  docker-compose up -d
```

### Acessando o Airflow
Para acessar o Airflow, acesse o endereço http://localhost:8080/. 

Utilize as credenciais:
- Login: airflow
- Senha: airflow

### Executando o Pipeline
O pipeline está configurado no Airflow. Após iniciar o Airflow,
você poderá executar o DAG brewery_data_pipeline que irá:
- Buscar os dados da API Open Brewery DB.
- Persistir os dados na camada Bronze.
- Transformar e particionar os dados na camada Silver.
- Agregar os dados na camada Gold.


### Monitorando o Pipeline
Para monitorar o progresso do pipeline, você pode acessar o log do Airflow.

Acesse o log do DAG no endereço http://localhost:8080/admin/airflow/dag_stats?dag_id=brewery_data_pipeline.

Você pode também monitorar o progresso do pipeline no terminal do Airflow.

Execute o comando:

docker compose logs -f airflow-scheduler

### Removendo o Ambiente
Para remover o ambiente, execute o seguinte comando:

docker compose down

### Como Acessar o MinIO
O MinIO é um serviço de armazenamento de dados distribuído.
Que está sendo utilizado para armazenar os dados brutos e transformados.


Acesse http://localhost:9002.
Faça login com as credenciais:
- login: testtamura
- senha: testtamura

Testes
O projeto inclui testes unitários para garantir que as transformações e persistências de dados estão funcionando corretamente.
Para rodar os testes, execute o seguinte comando:

**pytest**

### Escolhas de Design e Trade-offs

1. **Airflow para Orquestração**
   - **Motivo**: O Airflow foi escolhido devido à sua robustez na gestão de pipelines de dados, agendamento, gerenciamento de dependências e monitoramento. Ele também oferece suporte nativo a retries e alertas.
   - **Trade-off**: Embora o Airflow seja uma ferramenta poderosa para orquestração, ele pode ser complexo para configurar inicialmente. No entanto, sua flexibilidade para executar tarefas assíncronas e gerenciar fluxos de trabalho justifica essa complexidade.

2. **MinIO para Armazenamento**
   - **Motivo**: O MinIO foi selecionado como solução de armazenamento devido à sua compatibilidade com a API S3, o que facilita a integração com outras ferramentas e a portabilidade para ambientes de produção. Ele também é leve e fácil de configurar.
   - **Trade-off**: Embora o MinIO seja uma excelente opção para desenvolvimento e testes, para produção, soluções de armazenamento em nuvem como o Amazon S3 podem oferecer melhor escalabilidade, redundância e performance.

3. **Formato Parquet para a Camada Silver**
   - **Motivo**: O formato Parquet foi escolhido por sua eficiência em termos de compressão e desempenho na leitura de grandes volumes de dados. Ele é amplamente utilizado em ambientes de Big Data e é ideal para análise e agregação de dados.
   - **Trade-off**: A transformação para o formato Parquet exige um processamento adicional, mas os benefícios em termos de performance e armazenamento justificam essa escolha.

4. **Arquitetura Medallion**
   - **Motivo**: A Arquitetura Medallion (Bronze, Silver, Gold) foi adotada para garantir que os dados sejam processados e transformados em estágios, com cada camada representando um nível mais alto de qualidade e agregação de dados.
   - **Trade-off**: Embora essa arquitetura ofereça uma separação clara e melhor gerenciamento dos dados, ela também aumenta a complexidade do pipeline, exigindo múltiplos passos de transformação e camadas de armazenamento.
