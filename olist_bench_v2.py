"""
olist_bench_v2.py - Olist: MySQL x DynamoDB, no modelo descrito no artigo

  MySQL    : esquema normalizado, uma tabela por arquivo do Olist (8 tabelas),
             IDs CHAR(32), datas DATETIME, valores DECIMAL(10,2), índices extras
             em orders(order_delivered_customer_date) e
             order_payments(payment_type, payment_value); carga com LOAD DATA LOCAL INFILE.
  DynamoDB : single-table design - tabela única "Olist" com PK/SK genéricas,
             GSI1 (entregas por mês) e GSI2 (pagamentos por tipo, KEYS_ONLY);
             carga com BatchWriteItem em lotes de 25; inserção com TransactWriteItems.

Pré-requisitos:
    docker compose up -d                       (MySQL do projeto)
    CSVs do Olist em ./dados/
    pip install boto3 mysql-connector-python   (+ "botocore[crt]" se usar aws login)

Variáveis de ambiente (opcionais):
    OLIST_DYNAMO=aws        usa o DynamoDB da AWS (padrão: DynamoDB Local, porta 8000)
    AWS_REGION=us-east-2    região da AWS
    OLIST_MYSQL_PORT=3308   porta do MySQL do projeto (padrão 3308)

Uso:
    python olist_bench_v2.py carga                       # recria tudo e carrega as 8 entidades
    python olist_bench_v2.py carga --sem-geo             # idem, sem a geolocalização (~1 milhão de linhas)
    python olist_bench_v2.py carga --manter --entidades avaliacoes
                                                         # completa uma carga interrompida
    python olist_bench_v2.py consultas                   # 9 perguntas, média de 3 execuções

Resultados: tela + resultados_carga.csv / resultados_consultas.csv
"""
import argparse
import csv
import os
import platform
import statistics
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from threading import Lock

import boto3
import mysql.connector
from boto3.dynamodb.conditions import Attr, Key

# ======================================================================= configuração
PASTA = os.path.dirname(os.path.abspath(__file__))
PASTA_DADOS = os.path.join(PASTA, "dados")

MYSQL_CFG = dict(host="localhost", port=int(os.environ.get("OLIST_MYSQL_PORT", "3308")),
                 user="root", password="root", database="bigdata",
                 allow_local_infile=True)

if os.environ.get("OLIST_DYNAMO", "local").lower() == "aws":
    DYNAMO_CFG = dict(region_name=os.environ.get("AWS_REGION", "us-east-2"))
    ONDE_DYNAMO = f"AWS ({DYNAMO_CFG['region_name']})"
else:
    DYNAMO_CFG = dict(endpoint_url="http://localhost:8000", region_name="us-east-1",
                      aws_access_key_id="local", aws_secret_access_key="local")
    ONDE_DYNAMO = "DynamoDB Local"

TABELA_DYNAMO = "Olist"
EXECUCOES = 3
THREADS_DYNAMO = 8
BLOCO_DYNAMO = 100_000          # itens preparados por vez (limita o uso de memória)

# tipos de pagamento do Olist (o GSI2 é consultado um tipo por vez)
TIPOS_PAGAMENTO = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]

# ======================================================================= MySQL: esquema
DDL = {
    "customers": """CREATE TABLE customers (
        customer_id CHAR(32) PRIMARY KEY,
        customer_unique_id CHAR(32),
        customer_zip_code_prefix VARCHAR(8),
        customer_city VARCHAR(100),
        customer_state CHAR(2),
        INDEX idx_customers_zip (customer_zip_code_prefix)
    ) DEFAULT CHARSET=utf8mb4""",
    "products": """CREATE TABLE products (
        product_id CHAR(32) PRIMARY KEY,
        product_category_name VARCHAR(100),
        product_name_lenght INT,
        product_description_lenght INT,
        product_photos_qty INT,
        product_weight_g INT,
        product_length_cm INT,
        product_height_cm INT,
        product_width_cm INT
    ) DEFAULT CHARSET=utf8mb4""",
    "sellers": """CREATE TABLE sellers (
        seller_id CHAR(32) PRIMARY KEY,
        seller_zip_code_prefix VARCHAR(8),
        seller_city VARCHAR(100),
        seller_state CHAR(2),
        INDEX idx_sellers_zip (seller_zip_code_prefix)
    ) DEFAULT CHARSET=utf8mb4""",
    "geolocation": """CREATE TABLE geolocation (
        geo_id INT AUTO_INCREMENT PRIMARY KEY,
        geolocation_zip_code_prefix VARCHAR(8),
        geolocation_lat DOUBLE,
        geolocation_lng DOUBLE,
        geolocation_city VARCHAR(100),
        geolocation_state CHAR(2),
        INDEX idx_geolocation_zip (geolocation_zip_code_prefix)
    ) DEFAULT CHARSET=utf8mb4""",
    "orders": """CREATE TABLE orders (
        order_id CHAR(32) PRIMARY KEY,
        customer_id CHAR(32) NOT NULL,
        order_status VARCHAR(20),
        order_purchase_timestamp DATETIME,
        order_approved_at DATETIME,
        order_delivered_carrier_date DATETIME,
        order_delivered_customer_date DATETIME,
        order_estimated_delivery_date DATETIME,
        INDEX idx_orders_delivered (order_delivered_customer_date),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    ) DEFAULT CHARSET=utf8mb4""",
    "order_items": """CREATE TABLE order_items (
        order_id CHAR(32),
        order_item_id INT,
        product_id CHAR(32),
        seller_id CHAR(32),
        shipping_limit_date DATETIME,
        price DECIMAL(10,2),
        freight_value DECIMAL(10,2),
        PRIMARY KEY (order_id, order_item_id),
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
    ) DEFAULT CHARSET=utf8mb4""",
    "order_payments": """CREATE TABLE order_payments (
        order_id CHAR(32),
        payment_sequential INT,
        payment_type VARCHAR(20),
        payment_installments INT,
        payment_value DECIMAL(10,2),
        PRIMARY KEY (order_id, payment_sequential),
        INDEX idx_payments_type_value (payment_type, payment_value),
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    ) DEFAULT CHARSET=utf8mb4""",
    "order_reviews": """CREATE TABLE order_reviews (
        review_id CHAR(32),
        order_id CHAR(32),
        review_score INT,
        review_comment_title VARCHAR(255),
        review_comment_message TEXT,
        review_creation_date DATETIME,
        review_answer_timestamp DATETIME,
        PRIMARY KEY (review_id, order_id),
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    ) DEFAULT CHARSET=utf8mb4""",
}

# ======================================================================= DynamoDB: itens
def _sem_vazios(d):
    return {k: v for k, v in d.items() if v not in (None, "")}


def _num(v):
    return Decimal(v) if v not in (None, "") else None


def item_cliente(r, ctx):
    return _sem_vazios({
        "PK": f"CUSTOMER#{r['customer_id']}", "SK": "PROFILE",
        "customer_unique_id": r["customer_unique_id"],
        "customer_zip_code_prefix": r["customer_zip_code_prefix"],
        "customer_city": r["customer_city"], "customer_state": r["customer_state"]})


def item_produto(r, ctx):
    d = {"PK": f"PRODUCT#{r['product_id']}", "SK": "PROFILE",
         "product_category_name": r["product_category_name"]}
    for c in ("product_name_lenght", "product_description_lenght", "product_photos_qty",
              "product_weight_g", "product_length_cm", "product_height_cm",
              "product_width_cm"):
        d[c] = _num(r[c])
    return _sem_vazios(d)


def item_vendedor(r, ctx):
    return _sem_vazios({
        "PK": f"SELLER#{r['seller_id']}", "SK": "PROFILE",
        "seller_zip_code_prefix": r["seller_zip_code_prefix"],
        "seller_city": r["seller_city"], "seller_state": r["seller_state"]})


def item_geo(r, ctx):
    cep = r["geolocation_zip_code_prefix"]
    ctx["seq_geo"][cep] += 1
    return _sem_vazios({
        "PK": f"GEO#{cep}", "SK": f"{ctx['seq_geo'][cep]:05d}",
        "geolocation_lat": _num(r["geolocation_lat"]),
        "geolocation_lng": _num(r["geolocation_lng"]),
        "geolocation_city": r["geolocation_city"],
        "geolocation_state": r["geolocation_state"]})


def item_pedido(r, ctx):
    cidade, uf = ctx["clientes"].get(r["customer_id"], (None, None))
    d = {"PK": f"ORDER#{r['order_id']}", "SK": "METADATA",
         "order_status": r["order_status"],
         "order_purchase_timestamp": r["order_purchase_timestamp"],
         "order_approved_at": r["order_approved_at"],
         "order_delivered_carrier_date": r["order_delivered_carrier_date"],
         "order_delivered_customer_date": r["order_delivered_customer_date"],
         "order_estimated_delivery_date": r["order_estimated_delivery_date"],
         "customer_id": r["customer_id"],
         "customer_city": cidade, "customer_state": uf}       # desnormalizados
    entrega = r["order_delivered_customer_date"]
    if entrega:                                                 # GSI1 esparso
        d["GSI1PK"] = f"DELIVERED#{entrega[:7]}"
        d["GSI1SK"] = entrega
    return _sem_vazios(d)


def item_item(r, ctx):
    return _sem_vazios({
        "PK": f"ORDER#{r['order_id']}", "SK": f"ITEM#{int(r['order_item_id']):03d}",
        "product_id": r["product_id"], "seller_id": r["seller_id"],
        "shipping_limit_date": r["shipping_limit_date"],
        "price": _num(r["price"]), "freight_value": _num(r["freight_value"]),
        "product_category_name": ctx["produtos"].get(r["product_id"])})  # desnormalizado


def item_pagamento(r, ctx):
    valor = _num(r["payment_value"])
    return _sem_vazios({
        "PK": f"ORDER#{r['order_id']}", "SK": f"PAYMENT#{int(r['payment_sequential']):03d}",
        "payment_type": r["payment_type"],
        "payment_installments": _num(r["payment_installments"]),
        "payment_value": valor,
        "GSI2PK": f"PAYTYPE#{r['payment_type']}", "GSI2SK": valor})


def item_avaliacao(r, ctx):
    return _sem_vazios({
        "PK": f"ORDER#{r['order_id']}", "SK": f"REVIEW#{r['review_id']}",
        "review_score": _num(r["review_score"]),
        "review_comment_title": r["review_comment_title"],
        "review_comment_message": r["review_comment_message"],
        "review_creation_date": r["review_creation_date"],
        "review_answer_timestamp": r["review_answer_timestamp"]})


# ======================================================================= entidades
# ordem de carga respeita as chaves estrangeiras do MySQL
ENTIDADES = [
    dict(nome="clientes", mysql="customers", arquivo="olist_customers_dataset.csv",
         colunas=["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                  "customer_city", "customer_state"], item=item_cliente),
    dict(nome="produtos", mysql="products", arquivo="olist_products_dataset.csv",
         colunas=["product_id", "product_category_name", "product_name_lenght",
                  "product_description_lenght", "product_photos_qty", "product_weight_g",
                  "product_length_cm", "product_height_cm", "product_width_cm"],
         inteiros={"product_name_lenght", "product_description_lenght", "product_photos_qty",
                   "product_weight_g", "product_length_cm", "product_height_cm",
                   "product_width_cm"},
         item=item_produto),
    dict(nome="vendedores", mysql="sellers", arquivo="olist_sellers_dataset.csv",
         colunas=["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
         item=item_vendedor),
    dict(nome="geolocalizacao", mysql="geolocation", arquivo="olist_geolocation_dataset.csv",
         colunas=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                  "geolocation_city", "geolocation_state"], item=item_geo),
    dict(nome="pedidos", mysql="orders", arquivo="olist_orders_dataset.csv",
         colunas=["order_id", "customer_id", "order_status", "order_purchase_timestamp",
                  "order_approved_at", "order_delivered_carrier_date",
                  "order_delivered_customer_date", "order_estimated_delivery_date"],
         item=item_pedido),
    dict(nome="itens", mysql="order_items", arquivo="olist_order_items_dataset.csv",
         colunas=["order_id", "order_item_id", "product_id", "seller_id",
                  "shipping_limit_date", "price", "freight_value"], item=item_item),
    dict(nome="pagamentos", mysql="order_payments", arquivo="olist_order_payments_dataset.csv",
         colunas=["order_id", "payment_sequential", "payment_type", "payment_installments",
                  "payment_value"], item=item_pagamento),
    dict(nome="avaliacoes", mysql="order_reviews", arquivo="olist_order_reviews_dataset.csv",
         colunas=["review_id", "order_id", "review_score", "review_comment_title",
                  "review_comment_message", "review_creation_date",
                  "review_answer_timestamp"], item=item_avaliacao),
]
NOMES = [e["nome"] for e in ENTIDADES]


# ======================================================================= conexões
def conecta_mysql():
    return mysql.connector.connect(**MYSQL_CFG)


_SESSAO = None


def recurso_dynamo():
    # Uma única sessão no programa todo: as credenciais do "aws login" expiram e são
    # renovadas uma vez só, sob trava (evita o "Acesso negado" no cache do Windows).
    # Crie os recursos na thread principal.
    global _SESSAO
    if _SESSAO is None:
        _SESSAO = boto3.session.Session()
    return _SESSAO.resource("dynamodb", **DYNAMO_CFG)


# ======================================================================= leitura dos CSVs
def caminho_csv(ent):
    c = os.path.join(PASTA_DADOS, ent["arquivo"])
    if not os.path.exists(c):
        sys.exit(f"Arquivo não encontrado: {c}")
    return c


def le_csv(ent):
    with open(caminho_csv(ent), encoding="utf-8", newline="") as f:
        yield from csv.DictReader(f)


def contexto():
    """Tabelas auxiliares para a desnormalização no DynamoDB (não entra no tempo)."""
    clientes = {r["customer_id"]: (r["customer_city"], r["customer_state"])
                for r in le_csv(ENTIDADES[0])}
    produtos = {r["product_id"]: r["product_category_name"] for r in le_csv(ENTIDADES[1])}
    return {"clientes": clientes, "produtos": produtos, "seq_geo": defaultdict(int)}


# ======================================================================= carga MySQL
def _tsv(v):
    if v is None or v == "":
        return r"\N"
    return (v.replace("\\", "\\\\").replace("\t", "\\t")
             .replace("\n", "\\n").replace("\r", "\\r"))


def gera_tsv(ent):
    """Converte o CSV (com aspas e quebras de linha nos comentários) num TSV simples,
    no formato padrão do LOAD DATA. Feito antes do cronômetro."""
    inteiros = ent.get("inteiros", set())
    tmp = tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n",
                                      suffix=".tsv", delete=False)
    n = 0
    with tmp:
        for r in le_csv(ent):
            vals = []
            for c in ent["colunas"]:
                v = r.get(c, "")
                if v and c in inteiros:
                    v = str(int(float(v)))
                vals.append(_tsv(v))
            tmp.write("\t".join(vals) + "\n")
            n += 1
    return tmp.name, n


def recria_mysql(conn):
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for t in reversed(list(DDL)):
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    for t, ddl in DDL.items():
        cur.execute(ddl)
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.commit()


def carga_mysql(conn, ent):
    arquivo, n = gera_tsv(ent)
    try:
        caminho = arquivo.replace("\\", "/")
        sql = (f"LOAD DATA LOCAL INFILE '{caminho}' INTO TABLE {ent['mysql']} "
               f"CHARACTER SET utf8mb4 "
               r"FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "
               f"({', '.join(ent['colunas'])})")
        cur = conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        ini = time.perf_counter()
        cur.execute(sql)
        conn.commit()
        tempo = time.perf_counter() - ini
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        return tempo, n
    finally:
        os.remove(arquivo)


# ======================================================================= carga DynamoDB
def recria_dynamo():
    recurso = recurso_dynamo()
    cliente = recurso.meta.client
    if TABELA_DYNAMO in cliente.list_tables()["TableNames"]:
        cliente.delete_table(TableName=TABELA_DYNAMO)
        cliente.get_waiter("table_not_exists").wait(TableName=TABELA_DYNAMO)
    recurso.create_table(
        TableName=TABELA_DYNAMO,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"},
                   {"AttributeName": "SK", "KeyType": "RANGE"}],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
            {"AttributeName": "GSI2PK", "AttributeType": "S"},
            {"AttributeName": "GSI2SK", "AttributeType": "N"}],
        GlobalSecondaryIndexes=[
            {"IndexName": "GSI1",
             "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"},
                           {"AttributeName": "GSI1SK", "KeyType": "RANGE"}],
             "Projection": {"ProjectionType": "KEYS_ONLY"}},
            {"IndexName": "GSI2",
             "KeySchema": [{"AttributeName": "GSI2PK", "KeyType": "HASH"},
                           {"AttributeName": "GSI2SK", "KeyType": "RANGE"}],
             "Projection": {"ProjectionType": "KEYS_ONLY"}}])
    cliente.get_waiter("table_exists").wait(TableName=TABELA_DYNAMO)


def grava_dynamo(itens, rotulo, enviados_antes, total):
    partes = [itens[i::THREADS_DYNAMO] for i in range(THREADS_DYNAMO)]
    tabelas = [recurso_dynamo().Table(TABELA_DYNAMO) for _ in range(THREADS_DYNAMO)]
    prog = {"n": enviados_antes, "ultimo": enviados_antes}
    trava = Lock()

    def grava(args):
        t, parte = args
        with t.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as b:
            for it in parte:
                b.put_item(Item=it)
                with trava:
                    prog["n"] += 1
                    if prog["n"] - prog["ultimo"] >= 20_000:
                        prog["ultimo"] = prog["n"]
                        print(f"    {rotulo}: {prog['n']} de {total} enviados ao DynamoDB",
                              flush=True)

    ini = time.perf_counter()
    with ThreadPoolExecutor(THREADS_DYNAMO) as ex:
        list(ex.map(grava, zip(tabelas, partes)))
    return time.perf_counter() - ini


def carga_dynamo(ent, ctx, total):
    tempo, n, bloco = 0.0, 0, []
    for r in le_csv(ent):
        bloco.append(ent["item"](r, ctx))
        if len(bloco) >= BLOCO_DYNAMO:
            tempo += grava_dynamo(bloco, ent["nome"], n, total)
            n += len(bloco)
            bloco = []
    if bloco:
        tempo += grava_dynamo(bloco, ent["nome"], n, total)
        n += len(bloco)
    return tempo, n


# ======================================================================= comando carga
def cmd_carga(so, manter, sem_geo):
    print(f"MySQL: porta {MYSQL_CFG['port']} | DynamoDB: {ONDE_DYNAMO} | "
          f"máquina: {platform.platform()}, {os.cpu_count()} núcleos")
    conn = conecta_mysql()
    if manter:
        print("Completando a carga sem recriar tabelas...")
    else:
        print("Recriando as tabelas nos dois bancos...")
        recria_mysql(conn)
        recria_dynamo()

    escolhidas = [e for e in ENTIDADES
                  if (not so or e["nome"] in so)
                  and not (sem_geo and e["nome"] == "geolocalizacao")]
    print("Preparando tabelas auxiliares (não entra no tempo)...")
    ctx = contexto()

    linhas = [("entidade", "registros", "mysql_s", "dynamodb_s", "itens_dynamodb")]
    tot_m = tot_d = 0.0
    for e in escolhidas:
        tm, n = carga_mysql(conn, e)
        td, nd = carga_dynamo(e, ctx, n)
        tot_m += tm
        tot_d += td
        print(f"  {e['nome']:<15}{n:>9} registros | MySQL {tm:8.2f} s | "
              f"DynamoDB {td:8.2f} s", flush=True)
        linhas.append((e["nome"], n, f"{tm:.2f}", f"{td:.2f}", nd))
    linhas.append(("TOTAL", "", f"{tot_m:.2f}", f"{tot_d:.2f}", ""))
    print(f"  {'TOTAL':<15}{'':>9}           | MySQL {tot_m:8.2f} s | DynamoDB {tot_d:8.2f} s")

    cur = conn.cursor()
    print("\nRegistros no MySQL:")
    for e in ENTIDADES:
        cur.execute(f"SELECT COUNT(*) FROM {e['mysql']}")
        print(f"  {e['mysql']:<15}{cur.fetchone()[0]:>9}")

    nome = "resultados_carga_parcial.csv" if (so or manter) else "resultados_carga.csv"
    with open(os.path.join(PASTA, nome), "w", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerows(linhas)
    print(f"\nSalvo em {nome}")
    conn.close()


# ======================================================================= consultas
def nomes_proj(*attrs):
    """ProjectionExpression com nomes substitutos (evita palavras reservadas)."""
    mapa = {f"#p{i}": a for i, a in enumerate(attrs)}
    return ", ".join(mapa), mapa


def scan_tudo(tabela, filtro, *attrs):
    proj, nomes = nomes_proj(*attrs)
    kw = dict(FilterExpression=filtro, ProjectionExpression=proj,
              ExpressionAttributeNames=nomes)
    itens, resp = [], tabela.scan(**kw)
    itens += resp["Items"]
    while "LastEvaluatedKey" in resp:
        resp = tabela.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kw)
        itens += resp["Items"]
    return itens


def query_tudo(tabela, **kw):
    itens, resp = [], tabela.query(**kw)
    itens += resp["Items"]
    while "LastEvaluatedKey" in resp:
        resp = tabela.query(ExclusiveStartKey=resp["LastEvaluatedKey"], **kw)
        itens += resp["Items"]
    return itens


def estatisticas(grupos):
    res = [(g, statistics.mean(v), min(v), max(v)) for g, v in grupos.items() if v]
    return sorted(res, key=lambda x: -x[1])


def monta_consultas(conn, dyn, ped):
    cur = conn.cursor(buffered=True)
    tab = dyn.Table(TABELA_DYNAMO)
    cliente_baixo = dyn.meta.client
    criados = []                         # pedidos inseridos pela pergunta 2 (limpos depois)
    estado = {"n": 0}

    def proximo_status():
        estado["n"] += 1
        return "shipped" if estado["n"] % 2 else "delivered"

    def novo_pedido():
        oid = uuid.uuid4().hex                        # 32 caracteres, como no Olist
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        criados.append(oid)
        return oid, agora

    # ---------------- 1. acesso por chave
    def m_chave():
        n = 0
        for t in ("orders", "order_items", "order_payments", "order_reviews"):
            cur.execute(f"SELECT * FROM {t} WHERE order_id = %s", (ped["order_id"],))
            n += len(cur.fetchall())
        return n

    def d_chave():
        return len(query_tudo(tab, KeyConditionExpression=Key("PK").eq(f"ORDER#{ped['order_id']}")))

    # ---------------- 2. inserção de pedido (1 pedido + 2 itens + 1 pagamento)
    def m_insercao():
        oid, agora = novo_pedido()
        cur.execute("""INSERT INTO orders (order_id, customer_id, order_status,
                       order_purchase_timestamp) VALUES (%s, %s, 'created', %s)""",
                    (oid, ped["customer_id"], agora))
        cur.executemany("""INSERT INTO order_items (order_id, order_item_id, product_id,
                           seller_id, shipping_limit_date, price, freight_value)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        [(oid, i, ped["product_id"], ped["seller_id"], agora,
                          ped["price"], ped["freight"]) for i in (1, 2)])
        cur.execute("""INSERT INTO order_payments VALUES (%s, 1, 'credit_card', 1, %s)""",
                    (oid, ped["price"] * 2))
        conn.commit()
        return 4

    def d_insercao():
        oid, agora = novo_pedido()
        pk = f"ORDER#{oid}"
        itens = [{"PK": pk, "SK": "METADATA", "order_status": "created",
                  "order_purchase_timestamp": agora, "customer_id": ped["customer_id"],
                  "customer_city": ped["customer_city"], "customer_state": ped["customer_state"]}]
        for i in (1, 2):
            itens.append({"PK": pk, "SK": f"ITEM#{i:03d}", "product_id": ped["product_id"],
                          "seller_id": ped["seller_id"], "shipping_limit_date": agora,
                          "price": ped["price"], "freight_value": ped["freight"],
                          "product_category_name": ped["category"] or "sem_categoria"})
        valor = ped["price"] * 2
        itens.append({"PK": pk, "SK": "PAYMENT#001", "payment_type": "credit_card",
                      "payment_installments": Decimal(1), "payment_value": valor,
                      "GSI2PK": "PAYTYPE#credit_card", "GSI2SK": valor})
        # o client do resource já converte os tipos Python (str, Decimal) sozinho
        cliente_baixo.transact_write_items(TransactItems=[
            {"Put": {"TableName": TABELA_DYNAMO, "Item": it}} for it in itens])
        return 4

    # ---------------- 3. atualização de status
    def m_atualizacao():
        cur.execute("UPDATE orders SET order_status = %s WHERE order_id = %s",
                    (proximo_status(), ped["order_id"]))
        conn.commit()
        return cur.rowcount

    def d_atualizacao():
        tab.update_item(Key={"PK": f"ORDER#{ped['order_id']}", "SK": "METADATA"},
                        UpdateExpression="SET #s = :s",
                        ExpressionAttributeNames={"#s": "order_status"},
                        ExpressionAttributeValues={":s": proximo_status()})
        return 1

    # ---------------- 4. entregues em jan/2018 (índice x GSI1)
    def m_entregues():
        cur.execute("""SELECT order_id FROM orders
                       WHERE order_delivered_customer_date >= '2018-01-01'
                         AND order_delivered_customer_date <  '2018-02-01'""")
        return len(cur.fetchall())

    def d_entregues():
        return len(query_tudo(tab, IndexName="GSI1",
                              KeyConditionExpression=Key("GSI1PK").eq("DELIVERED#2018-01")))

    # ---------------- 5. estatísticas por forma de pagamento (GROUP BY x GSI2)
    def m_pagamentos():
        cur.execute("""SELECT payment_type, AVG(payment_value), MIN(payment_value),
                              MAX(payment_value)
                       FROM order_payments GROUP BY payment_type
                       ORDER BY AVG(payment_value) DESC""")
        return len(cur.fetchall())

    def d_pagamentos():
        grupos = {}
        for tipo in TIPOS_PAGAMENTO:
            its = query_tudo(tab, IndexName="GSI2",
                             KeyConditionExpression=Key("GSI2PK").eq(f"PAYTYPE#{tipo}"))
            grupos[tipo] = [float(i["GSI2SK"]) for i in its]
        return len(estatisticas(grupos))

    # ---------------- 6. busca de texto, 1 tabela
    def m_texto_1():
        cur.execute("""SELECT review_id, review_comment_message FROM order_reviews
                       WHERE review_comment_message COLLATE utf8mb4_bin LIKE '%atraso%'""")
        return len(cur.fetchall())

    def d_texto_1():
        f = Attr("SK").begins_with("REVIEW#") & Attr("review_comment_message").contains("atraso")
        return len(scan_tudo(tab, f, "PK", "SK", "review_comment_message"))

    # ---------------- 7. busca de texto, 2 tabelas (pedidos x clientes)
    def m_texto_2():
        cur.execute("""SELECT o.order_id, c.customer_city
                       FROM orders o JOIN customers c ON c.customer_id = o.customer_id
                       WHERE c.customer_city COLLATE utf8mb4_bin LIKE '%sao%'""")
        return len(cur.fetchall())

    def d_texto_2():
        # a cidade do cliente foi desnormalizada no item METADATA do pedido
        f = Attr("SK").eq("METADATA") & Attr("customer_city").contains("sao")
        return len(scan_tudo(tab, f, "PK", "customer_city"))

    # ---------------- 8. agregação, 3 tabelas (itens x pedidos x clientes)
    def m_agrega_uf():
        cur.execute("""SELECT c.customer_state, AVG(i.price), MIN(i.price), MAX(i.price)
                       FROM order_items i
                       JOIN orders o    ON o.order_id = i.order_id
                       JOIN customers c ON c.customer_id = o.customer_id
                       GROUP BY c.customer_state ORDER BY AVG(i.price) DESC""")
        return len(cur.fetchall())

    def d_agrega_uf():
        f = Attr("SK").begins_with("ITEM#") | Attr("SK").eq("METADATA")
        uf, precos = {}, []
        for it in scan_tudo(tab, f, "PK", "SK", "price", "customer_state"):
            if it["SK"] == "METADATA":
                uf[it["PK"]] = it.get("customer_state")
            else:
                precos.append((it["PK"], float(it["price"])))
        grupos = defaultdict(list)
        for pk, p in precos:
            if pk in uf:
                grupos[uf[pk]].append(p)
        return len(estatisticas(grupos))

    consultas = [
        ("1 Acesso por chave",                    m_chave,       d_chave),
        ("2 Inserção de pedido",                  m_insercao,    d_insercao),
        ("3 Atualização de status",               m_atualizacao, d_atualizacao),
        ("4 Entregues em jan/2018",               m_entregues,   d_entregues),
        ("5 Estatísticas por pagamento",          m_pagamentos,  d_pagamentos),
        ("6 Busca de texto (1 tabela)",           m_texto_1,     d_texto_1),
        ("7 Busca de texto (2 tabelas)",          m_texto_2,     d_texto_2),
        ("8 Agregação por UF (3 tabelas)",        m_agrega_uf,   d_agrega_uf),
    ]
    return consultas, criados


def cronometra(func):
    tempos, n = [], None
    for _ in range(EXECUCOES):
        ini = time.perf_counter()
        n = func()
        tempos.append((time.perf_counter() - ini) * 1000)
    return statistics.mean(tempos), n


def limpa_insercoes(conn, dyn, criados):
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for oid in criados:
        for t in ("order_items", "order_payments", "orders"):
            cur.execute(f"DELETE FROM {t} WHERE order_id = %s", (oid,))
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.commit()
    tab = dyn.Table(TABELA_DYNAMO)
    with tab.batch_writer() as b:
        for oid in criados:
            for sk in ("METADATA", "ITEM#001", "ITEM#002", "PAYMENT#001"):
                b.delete_item(Key={"PK": f"ORDER#{oid}", "SK": sk})


def cmd_consultas():
    print(f"MySQL: porta {MYSQL_CFG['port']} | DynamoDB: {ONDE_DYNAMO} | "
          f"máquina: {platform.platform()}, {os.cpu_count()} núcleos")
    conn = conecta_mysql()
    dyn = recurso_dynamo()
    cur = conn.cursor(buffered=True)
    # pedido de teste: tem itens, pagamento e avaliação (exercita a consulta 1 inteira)
    cur.execute("""SELECT o.order_id, o.order_status, o.customer_id, c.customer_city,
                          c.customer_state, i.product_id, i.seller_id, i.price,
                          i.freight_value, p.product_category_name
                   FROM orders o
                   JOIN customers c   ON c.customer_id = o.customer_id
                   JOIN order_items i ON i.order_id = o.order_id AND i.order_item_id = 1
                   JOIN products p    ON p.product_id = i.product_id
                   WHERE EXISTS (SELECT 1 FROM order_payments x WHERE x.order_id = o.order_id)
                     AND EXISTS (SELECT 1 FROM order_reviews r WHERE r.order_id = o.order_id)
                   ORDER BY o.order_id LIMIT 1""")
    linha = cur.fetchone()
    if not linha:
        sys.exit("MySQL vazio. Rode antes: python olist_bench_v2.py carga")
    chaves = ("order_id", "status", "customer_id", "customer_city", "customer_state",
              "product_id", "seller_id", "price", "freight", "category")
    ped = dict(zip(chaves, linha))
    ped["price"] = Decimal(str(ped["price"]))
    ped["freight"] = Decimal(str(ped["freight"]))

    consultas, criados = monta_consultas(conn, dyn, ped)
    print(f"Média de {EXECUCOES} execuções | pedido de teste: {ped['order_id']}\n")
    print(f"{'Consulta':<36}{'MySQL (ms)':>12}{'DynamoDB (ms)':>15}{'linhas M':>10}{'linhas D':>10}")
    saida = [("consulta", "mysql_ms", "dynamodb_ms", "linhas_mysql", "linhas_dynamodb")]
    try:
        for nome, fm, fd in consultas:
            tm, nm = cronometra(fm)
            td, nd = cronometra(fd)
            aviso = "" if nm == nd else "  <- contagens diferentes!"
            print(f"{nome:<36}{tm:>12.2f}{td:>15.2f}{nm:>10}{nd:>10}{aviso}", flush=True)
            saida.append((nome, f"{tm:.2f}", f"{td:.2f}", nm, nd))
    finally:
        # desfaz as alterações de teste nos dois bancos (fora do cronômetro)
        limpa_insercoes(conn, dyn, criados)
        cur.execute("UPDATE orders SET order_status = %s WHERE order_id = %s",
                    (ped["status"], ped["order_id"]))
        conn.commit()
        dyn.Table(TABELA_DYNAMO).update_item(
            Key={"PK": f"ORDER#{ped['order_id']}", "SK": "METADATA"},
            UpdateExpression="SET #s = :s", ExpressionAttributeNames={"#s": "order_status"},
            ExpressionAttributeValues={":s": ped["status"]})

    with open(os.path.join(PASTA, "resultados_consultas.csv"), "w", newline="",
              encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerows(saida)
    print("\nResultados salvos em resultados_consultas.csv")
    conn.close()


# ======================================================================= main
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Olist: MySQL x DynamoDB (single-table)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pc = sub.add_parser("carga", help="cria as tabelas e carrega os CSVs")
    pc.add_argument("--entidades", nargs="+", choices=NOMES,
                    help="carrega só estas entidades")
    pc.add_argument("--manter", action="store_true",
                    help="não recria as tabelas (completa uma carga interrompida)")
    pc.add_argument("--sem-geo", action="store_true",
                    help="não carrega a geolocalização (~1 milhão de linhas)")
    sub.add_parser("consultas", help="executa as perguntas comparativas")
    a = ap.parse_args()
    if a.cmd == "carga":
        cmd_carga(a.entidades, a.manter, a.sem_geo)
    else:
        cmd_consultas()
