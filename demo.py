"""
demo.py - Demonstração ao vivo para a defesa (roda em segundos, não em minutos)

Mostra, lado a lado, a mesma pergunta no MySQL e no DynamoDB, imprimindo o
comando usado, o tempo e o resultado. Usa as mesmas configurações e a mesma
tabela do olist_bench_v2.py (que deve estar na mesma pasta, com a carga feita).

Uso (mesmas variáveis de ambiente do olist_bench_v2.py):
    python demo.py          # as três demonstrações em sequência
    python demo.py chave    # 1) acesso por chave: 4 SELECTs x 1 Query
    python demo.py gsi      # 2) entregues em jan/2018: índice x GSI1
    python demo.py scan     # 3) busca de texto: LIKE x Scan, página por página
"""
import sys
import time

from boto3.dynamodb.conditions import Attr, Key

import olist_bench_v2 as ob


def titulo(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)


def ms(ini):
    return (time.perf_counter() - ini) * 1000


def pausa():
    if sys.stdin.isatty():
        input("\n[Enter para continuar]")


def demo_chave(cur, tab):
    titulo("1) ACESSO POR CHAVE: todos os dados de um pedido")
    cur.execute("SELECT order_id FROM order_items ORDER BY order_id LIMIT 1")
    oid = cur.fetchone()[0]
    print(f"Pedido: {oid}\n")

    print("MySQL: 4 consultas, uma por tabela")
    ini, total = time.perf_counter(), 0
    for t in ("orders", "order_items", "order_payments", "order_reviews"):
        cur.execute(f"SELECT * FROM {t} WHERE order_id = %s", (oid,))
        n = len(cur.fetchall())
        total += n
        print(f"   SELECT * FROM {t:<15} WHERE order_id = ...  -> {n} linha(s)")
    print(f"   Total: {total} linhas em {ms(ini):.1f} ms")

    print("\nDynamoDB: 1 Query na chave de partição")
    print(f"   Query(PK = 'ORDER#{oid[:8]}...')")
    ini = time.perf_counter()
    itens = ob.query_tudo(tab, KeyConditionExpression=Key("PK").eq(f"ORDER#{oid}"))
    t = ms(ini)
    for it in itens:
        print(f"   -> SK = {it['SK']}")
    rede = " (inclui a ida e volta até a AWS)" if ob.ONDE_DYNAMO.startswith("AWS") else ""
    print(f"   Total: {len(itens)} itens em {t:.1f} ms{rede}")
    print("\nO pedido, seus itens, pagamentos e avaliação compartilham a mesma PK:"
          "\nficam guardados juntos e vêm numa única requisição, sem JOIN.")


def demo_gsi(cur, tab):
    titulo("2) ENTREGUES EM JAN/2018: índice no MySQL x GSI1 no DynamoDB")
    sql = ("SELECT order_id FROM orders WHERE order_delivered_customer_date "
           ">= '2018-01-01' AND order_delivered_customer_date < '2018-02-01'")
    print(f"MySQL:\n   {sql}")
    ini = time.perf_counter()
    cur.execute(sql)
    n = len(cur.fetchall())
    print(f"   -> {n} pedidos em {ms(ini):.1f} ms")

    print("\nDynamoDB:\n   Query(IndexName='GSI1', GSI1PK = 'DELIVERED#2018-01')")
    ini, paginas, total = time.perf_counter(), 0, 0
    kw = dict(IndexName="GSI1", KeyConditionExpression=Key("GSI1PK").eq("DELIVERED#2018-01"))
    resp = tab.query(**kw)
    while True:
        paginas += 1
        total += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            break
        resp = tab.query(ExclusiveStartKey=resp["LastEvaluatedKey"], **kw)
    print(f"   -> {total} pedidos em {ms(ini):.1f} ms, {paginas} página(s)")
    print("\nO GSI1 só contém pedidos entregues, agrupados por mês: a Query lê"
          "\napenas as chaves de janeiro, sem varrer a tabela.")


def demo_scan(cur, tab):
    titulo("3) BUSCA DE TEXTO 'atraso': LIKE x Scan")
    sql = ("SELECT review_id FROM order_reviews WHERE review_comment_message "
           "COLLATE utf8mb4_bin LIKE '%atraso%'")
    print(f"MySQL:\n   {sql}")
    ini = time.perf_counter()
    cur.execute(sql)
    n = len(cur.fetchall())
    print(f"   -> {n} avaliações em {ms(ini):.1f} ms")

    print("\nDynamoDB: Scan (lê a tabela inteira e filtra depois)")
    print("   Scan(Filter: SK começa com 'REVIEW#' E contains(comentário, 'atraso'))\n")
    filtro = (Attr("SK").begins_with("REVIEW#")
              & Attr("review_comment_message").contains("atraso"))
    proj, nomes = ob.nomes_proj("PK", "SK")
    kw = dict(FilterExpression=filtro, ProjectionExpression=proj,
              ExpressionAttributeNames=nomes)
    ini, pag, lidos, achados = time.perf_counter(), 0, 0, 0
    resp = tab.scan(**kw)
    while True:
        pag += 1
        lidos += resp["ScannedCount"]
        achados += resp["Count"]
        print(f"   página {pag:>3}: {lidos:>8} itens lidos | {achados:>4} aprovados "
              f"| {ms(ini) / 1000:6.1f} s", flush=True)
        if "LastEvaluatedKey" not in resp:
            break
        resp = tab.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kw)
    print(f"\n   -> {achados} avaliações; leu {lidos} itens em {pag} páginas "
          f"({ms(ini) / 1000:.1f} s)")
    print(f"\nPara achar {achados} itens, o DynamoDB leu {lidos}: o custo do Scan"
          "\ndepende do tamanho da tabela, não do resultado.")


if __name__ == "__main__":
    escolha = sys.argv[1] if len(sys.argv) > 1 else "todas"
    print(f"MySQL: porta {ob.MYSQL_CFG['port']} | DynamoDB: {ob.ONDE_DYNAMO}")
    conn = ob.conecta_mysql()
    cur = conn.cursor(buffered=True)
    tab = ob.recurso_dynamo().Table(ob.TABELA_DYNAMO)
    tab.load()                       # abre a conexão antes de medir (aquecimento)

    demos = {"chave": demo_chave, "gsi": demo_gsi, "scan": demo_scan}
    if escolha in demos:
        demos[escolha](cur, tab)
    else:
        for i, d in enumerate(demos.values()):
            d(cur, tab)
            if i < len(demos) - 1:
                pausa()
    conn.close()
