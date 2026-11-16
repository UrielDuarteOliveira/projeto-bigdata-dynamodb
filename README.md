# DynamoDB × MySQL — Estudo de caso com o Olist

Projeto da disciplina DS340 (Banco de Dados 3 — UFPR). Compara o Amazon DynamoDB
(tabela única com GSIs, na AWS) e o MySQL (normalizado, local em Docker) com os
mesmos dados e as mesmas perguntas. O Anexo A do artigo é o guia de uso do
DynamoDB; este README é o passo a passo para rodar o projeto.

## Conteúdo

| Arquivo | Para que serve |
|---|---|
| `olist_bench_v2.py` | Programa principal: `carga` (cria as tabelas e carrega os dados nos dois bancos) e `consultas` (8 perguntas, média de 3 execuções) |
| `demo.py` | Demonstração rápida para a apresentação (chave, GSI e Scan) |
| `docker-compose.yml` | MySQL 8.4 (e DynamoDB Local, opcional) |
| `requirements.txt` | Bibliotecas Python |
| `dados/` | Onde ficam os CSVs do Olist (baixar do Kaggle, ver `dados/LEIA-ME.txt`) |
| `resultados_preliminares/` | Resultados da execução de 16/11/2026 (Windows 11, AWS us-east-2, sem geolocalização) |
| `artigo/` | Código LaTeX do artigo e referências para o `.bib` |

## 1. Requisitos

- Docker e Docker Compose
- Python 3.10 ou superior
- AWS CLI versão 2
- Acesso à conta AWS do projeto, com permissão para usar o DynamoDB
- Cerca de 2 GB livres em disco

## 2. Preparar o ambiente

**Linux (Ubuntu):**

```bash
cd projeto-bigdata-dynamodb
docker compose up -d
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Se o `venv` não existir no Ubuntu: `sudo apt install python3-venv`.

**Windows (Prompt de Comando):**

```bat
cd projeto-bigdata-dynamodb
docker compose up -d
pip install -r requirements.txt
```

Espere uns 30 segundos e confira o MySQL:

```bash
docker exec mysql-bigdata mysql -uroot -proot -e "SHOW DATABASES;"
```

O banco `bigdata` deve aparecer na lista.

## 3. Baixar os dados

Baixe o Olist em <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>
(conta gratuita do Kaggle). Extraia os CSVs **diretamente** na pasta `dados/`,
sem subpastas.

## 4. Configurar a AWS

```bash
aws login
aws dynamodb list-tables --region us-east-2
```

O `aws login` abre o navegador. Entre com a conta do projeto. As credenciais
nunca vão para o código, nem para o grupo da equipe ou para prints.

Em seguida, defina as variáveis de ambiente **na mesma janela do terminal** em
que vai rodar o programa:

| | Linux | Windows (cmd) |
|---|---|---|
| Usar a AWS | `export OLIST_DYNAMO=aws` | `set OLIST_DYNAMO=aws` |
| Região | `export AWS_REGION=us-east-2` | `set AWS_REGION=us-east-2` |
| Porta do MySQL (só se mudou) | `export OLIST_MYSQL_PORT=3309` | `set OLIST_MYSQL_PORT=3309` |

Sem `OLIST_DYNAMO=aws`, o programa usa o DynamoDB Local do Docker (porta 8000).
Isso é útil para testar sem custo, mas não serve para os resultados.

## 5. Rodar

```bash
python olist_bench_v2.py carga          # tudo, incluindo a geolocalização (~1,5 milhão de itens)
python olist_bench_v2.py consultas      # 8 perguntas, 3 execuções cada
```

No Linux, use `python3` se `python` não existir.

- **Primeira linha da saída:** mostra a porta do MySQL, o destino do DynamoDB e a máquina. Confira antes de esperar.
- **Durante a carga:** o progresso aparece a cada 20 mil itens. Mantenha o computador ligado na tomada e sem suspensão.
- **Tempos de referência:**
  - carga sem geolocalização: cerca de 9 minutos;
  - carga com geolocalização: cerca de 15 a 30 minutos a mais;
  - `consultas`: cerca de 5 a 10 minutos.
- **Arquivos gerados:** `resultados_carga.csv` e `resultados_consultas.csv`.
- **O que conferir:** nas consultas, as colunas `linhas M` e `linhas D` precisam ser iguais em todas as linhas. Se aparecer `contagens diferentes!`, avise a equipe.

**Opções da carga:**

| Opção | Para que serve |
|---|---|
| `--sem-geo` | Pula a geolocalização (~1 milhão de linhas, não usada pelas perguntas) |
| `--manter --entidades pagamentos avaliacoes` | Completa uma carga interrompida sem apagar o que já foi gravado |

Atenção: `carga` sem `--manter` **apaga e recria** a tabela `Olist` na AWS e as
tabelas do MySQL.

## 6. Demonstração (apresentação)

```bash
python demo.py chave    # 4 SELECTs no MySQL × 1 Query no DynamoDB
python demo.py gsi      # índice no MySQL × GSI1 no DynamoDB
python demo.py scan     # LIKE no MySQL × Scan página por página
python demo.py          # as três em sequência
```

Requer a carga feita e as mesmas variáveis de ambiente.

## 7. Problemas comuns

| Sintoma | Causa | Solução |
|---|---|---|
| `port is already allocated` ao subir o Docker | A porta 3308 já está em uso | Troque para `"3309:3306"` no `docker-compose.yml`, rode `docker compose up -d` e defina `OLIST_MYSQL_PORT=3309` |
| `Unknown database 'bigdata'` | O programa conectou em outro MySQL da máquina | Confira com `docker port mysql-bigdata` e ajuste `OLIST_MYSQL_PORT` |
| `MissingDependencyException ... botocore[crt]` | Falta um pacote para usar o `aws login` | `pip install "botocore[crt]"` |
| `security token ... invalid` ou credencial expirada | Sessão da AWS vencida | Rode `aws login` de novo |
| `Arquivo não encontrado` | CSVs fora do lugar | Coloque os CSVs direto em `dados/` |
| Carga parou no meio | Queda de rede ou suspensão do PC | Retome com `--manter --entidades <as que faltaram>` |
| Console da AWS mostra 0 itens | A contagem do console só atualiza a cada ~6 h | Confie na saída do programa |

## 8. Custos e limpeza

A tabela usa o modo sob demanda: paga-se por leitura, escrita e armazenamento. O
`Scan` é a operação mais cara, então evitem rodar `consultas` repetidamente sem
necessidade. Depois dos testes oficiais:

```bash
aws dynamodb delete-table --table-name Olist --region us-east-2
docker compose down -v
```
