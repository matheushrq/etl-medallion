<h1>ETL com Medallion Architecture</h1>

Projeto ETL construido com a arquitetura Medalhão, onde a divisão dos dados é feita através de camadas (bronze para dados brutos, silver para dados em tratamento e gold para dados tratados/enriquecidos).

No projeto, realizo a extração dos dados pela api do ViaCep (que todo mundo já deve conhecer), faço o tratamento transformando os arquivos para o formato parquet, que possui uma melhor leitura para grandes volumes de dados e realizo o enriquecimento dos dados subindo para o Postgres através de um container no Docker.
