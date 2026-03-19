#!/usr/bin/env python3
"""
add_fund.py — Adiciona um fundo novo ao histórico do site.

Uso (chamado pelo GitHub Actions):
    python scripts/add_fund.py CNPJ NOME NOME_CURTO TIPO TRIB EXPO BANCO

O script:
  1. Busca o histórico completo do fundo na CVM (desde a inception)
  2. Acrescenta ao history.json existente sem modificar os outros fundos
  3. Adiciona o fundo à lista FUNDS no fetch_data.py
  4. Adiciona a entrada FUND_META no index.html
  5. Recalcula retornos, correlações e commonDates
"""

import sys, json, zipfile, io, math, datetime, urllib.request, re
from pathlib import Path

FIRST_MONTHLY   = 2021
CVM_OLDEST_YEAR = 2005
DOCS_DIR        = Path(__file__).parent.parent / "docs"
SCRIPTS_DIR     = Path(__file__).parent
HIST_PATH       = DOCS_DIR / "history.json"
DATA_PATH       = DOCS_DIR / "data.json"
INDEX_PATH      = DOCS_DIR / "index.html"
FETCH_PATH      = SCRIPTS_DIR / "fetch_data.py"


# ── Fetch CVM ──────────────────────────────────────────────────────────────────

def fetch_zip(url: str, timeout: int = 120) -> str | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            return zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
    except Exception as e:
        print(f"  WARN: {url} — {e}")
        return None


def parse_csv(content: str) -> dict:
    lines  = content.split("\n")
    header = [h.strip().lstrip("\ufeff") for h in lines[0].split(";")]
    return {
        "lines":     lines,
        "col_cnpj":  next((i for i, h in enumerate(header) if h.startswith("CNPJ")), -1),
        "col_date":  header.index("DT_COMPTC") if "DT_COMPTC"  in header else -1,
        "col_quota": header.index("VL_QUOTA")  if "VL_QUOTA"   in header else -1,
    }


def extract_fund_from_csv(data: dict, cnpj_digits: str, cnpj_fmt: str) -> dict[str, float]:
    if not data or data["col_date"] < 0 or data["col_quota"] < 0:
        return {}
    result: dict[str, float] = {}
    for line in data["lines"][1:]:
        if cnpj_digits not in line and cnpj_fmt not in line:
            continue
        cols = line.split(";")
        try:
            if data["col_cnpj"] >= 0:
                raw = cols[data["col_cnpj"]].strip().replace(".", "").replace("/", "").replace("-", "")
                if raw != cnpj_digits:
                    continue
            d = cols[data["col_date"]].strip()
            q = float(cols[data["col_quota"]].replace(",", "."))
            if d and q > 0:
                result[d] = q
        except (ValueError, IndexError):
            continue
    return result


def fetch_full_history(cnpj_digits: str, cnpj_fmt: str) -> dict[str, float]:
    """Busca todas as cotas do fundo desde CVM_OLDEST_YEAR até hoje."""
    today = datetime.date.today()
    quotas: dict[str, float] = {}

    # Anos pré-2021
    for year in range(CVM_OLDEST_YEAR, FIRST_MONTHLY):
        url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{year}.zip"
        content = fetch_zip(url)
        if content:
            rows = extract_fund_from_csv(parse_csv(content), cnpj_digits, cnpj_fmt)
            if rows:
                quotas.update(rows)
                print(f"  {year}: +{len(rows)} cotas (total {len(quotas)})")
            elif quotas:
                # Fundo existia mas não tem dados neste ano → pode ser antes do inception
                pass

    # 2021+: mensais
    y, m = FIRST_MONTHLY, 1
    while (y, m) <= (today.year, today.month):
        url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{y}{m:02d}.zip"
        content = fetch_zip(url)
        if content:
            rows = extract_fund_from_csv(parse_csv(content), cnpj_digits, cnpj_fmt)
            if rows:
                quotas.update(rows)
                print(f"  {y}-{m:02d}: +{len(rows)} cotas")
        m += 1
        if m > 12:
            m = 1; y += 1

    return quotas


# ── Interpolação geométrica ────────────────────────────────────────────────────

def interpolate(quotas: dict[str, float], common_dates: list[str]) -> dict[str, float]:
    if not quotas:
        return quotas
    sorted_known = sorted(quotas.keys())
    fund_start   = sorted_known[0]
    fund_end     = sorted_known[-1]

    for d in common_dates:
        if d < fund_start or d > fund_end or d in quotas:
            continue
        prev_d = next((x for x in reversed(sorted_known) if x < d), None)
        next_d = next((x for x in sorted_known           if x > d), None)
        if prev_d and next_d:
            t0    = datetime.date.fromisoformat(prev_d)
            t1    = datetime.date.fromisoformat(next_d)
            td    = datetime.date.fromisoformat(d)
            alpha = (td - t0).days / max((t1 - t0).days, 1)
            quotas[d] = round(quotas[prev_d] * ((quotas[next_d] / quotas[prev_d]) ** alpha), 8)
        elif prev_d:
            quotas[d] = quotas[prev_d]

    return quotas


# ── Pearson ────────────────────────────────────────────────────────────────────

def pearson(a: list, b: list) -> float:
    n = len(a)
    if n < 30: return 0.0
    ma, mb = sum(a)/n, sum(b)/n
    num = sum((a[i]-ma)*(b[i]-mb) for i in range(n))
    sa  = math.sqrt(sum((x-ma)**2 for x in a))
    sb  = math.sqrt(sum((x-mb)**2 for x in b))
    return round(num/(sa*sb), 4) if sa*sb > 0 else 0.0


# ── Atualizar history.json ─────────────────────────────────────────────────────

def update_history(cnpj_fmt: str, nome: str, new_quotas: dict[str, float]) -> None:
    print(f"\n── Atualizando history.json")

    hist = {}
    if HIST_PATH.exists():
        hist = json.loads(HIST_PATH.read_text())

    # Adicionar o fundo novo ao dict de cotas
    all_fund_quotas: dict[str, dict[str, float]] = {}
    for cnpj, fd in hist.get("funds", {}).items():
        all_fund_quotas[cnpj] = dict(zip(fd["dates"], fd["quotas"]))
    all_fund_quotas[cnpj_fmt] = new_quotas

    # Union de todas as datas
    all_dates = sorted({d for qs in all_fund_quotas.values() for d in qs})
    print(f"  Total datas (union): {len(all_dates)} ({all_dates[0]} → {all_dates[-1]})")

    # Interpola lacunas para cada fundo
    for cnpj, qs in all_fund_quotas.items():
        all_fund_quotas[cnpj] = interpolate(qs, all_dates)

    # Datas comuns (union — cada fundo tem a sua série)
    common_dates = all_dates  # sem cutoff, sem limiar

    # Reconstruir funds_out
    funds_out = {}
    for cnpj, qs in all_fund_quotas.items():
        fund_dates  = sorted(qs.keys())
        fund_quotas = [qs[d] for d in fund_dates]
        returns = []
        for i in range(1, len(fund_dates)):
            q0, q1 = qs.get(fund_dates[i-1]), qs.get(fund_dates[i])
            returns.append((q1/q0)-1 if q0 and q1 else 0.0)
        cum=pk=1.0; mdd=0.0
        for r in returns:
            cum*=(1+r)
            if cum>pk: pk=cum
            dd=(cum-pk)/pk
            if dd<mdd: mdd=dd

        # Preservar nome existente ou usar o novo
        existing_nome = hist.get("funds", {}).get(cnpj, {}).get("nome", "")
        funds_out[cnpj] = {
            "nome":        nome if cnpj == cnpj_fmt else existing_nome,
            "dates":       fund_dates,
            "quotas":      fund_quotas,
            "returns":     returns,
            "maxDrawdown": round(mdd*100, 2),
            "nDays":       len(fund_dates),
            "start":       fund_dates[0],
            "end":         fund_dates[-1],
        }

    # Recalcular correlações por par (interseção dinâmica)
    cnpjs = list(funds_out.keys())
    corr  = {}
    for ca in cnpjs:
        corr[ca] = {}
        dates_a = set(funds_out[ca]["dates"])
        qs_a = all_fund_quotas[ca]
        for cb in cnpjs:
            if ca == cb:
                corr[ca][cb] = 1.0
                continue
            dates_b = set(funds_out[cb]["dates"])
            qs_b    = all_fund_quotas[cb]
            common  = sorted(dates_a & dates_b)
            if len(common) < 30:
                corr[ca][cb] = 0.0
                continue
            ra = [(qs_a[common[i]]/qs_a[common[i-1]])-1 for i in range(1,len(common))]
            rb = [(qs_b[common[i]]/qs_b[common[i-1]])-1 for i in range(1,len(common))]
            corr[ca][cb] = pearson(ra, rb)

    n_years = (datetime.date.fromisoformat(all_dates[-1]) -
               datetime.date.fromisoformat(all_dates[0])).days / 365.25 if all_dates else 0

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "from":        all_dates[0],
        "to":          all_dates[-1],
        "nDays":       len(common_dates),
        "nYears":      round(n_years, 2),
        "commonDates": common_dates,
        "correlation": corr,
        "funds":       funds_out,
    }

    HIST_PATH.write_text(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
    size_kb = HIST_PATH.stat().st_size // 1024
    print(f"  ✓ history.json: {len(common_dates)} datas, {n_years:.1f} anos, {size_kb} KB")


# ── Atualizar fetch_data.py ────────────────────────────────────────────────────

def update_fetch_data(cnpj_digits: str, cnpj_fmt: str, nome: str) -> None:
    print(f"\n── Atualizando fetch_data.py")
    src = FETCH_PATH.read_text()

    # Verifica se já está na lista
    if cnpj_digits in src:
        print(f"  Fundo já está em FUNDS — sem alteração")
        return

    # Encontra o último fundo na lista e insere depois
    new_entry = f'    {{"name": "{nome}", "cnpj": "{cnpj_digits}", "cnpjFmt": "{cnpj_fmt}"}},\n'

    # Insere antes do fechamento da lista FUNDS
    src = src.replace(
        ']\n\nFIRST_MONTHLY_YEAR',
        f'    {{"name": "{nome}", "cnpj": "{cnpj_digits}", "cnpjFmt": "{cnpj_fmt}"}},\n]\n\nFIRST_MONTHLY_YEAR',
        1
    )

    FETCH_PATH.write_text(src)
    print(f"  ✓ fetch_data.py: fundo adicionado à lista FUNDS")


# ── Atualizar index.html ───────────────────────────────────────────────────────

def update_index(cnpj_fmt: str, nome: str, nome_curto: str,
                 tipo: str, trib: str, expo: str, banco: str,
                 inception_date: str, initial_quota: float) -> None:
    print(f"\n── Atualizando index.html")
    src = INDEX_PATH.read_text()

    if cnpj_fmt in src:
        print(f"  Fundo já está em FUND_META — sem alteração")
        return

    # Formata CNPJ para exibição
    new_meta = (
        f'  "{cnpj_fmt}": {{ nome:"{nome_curto}", short:"{nome_curto}", '
        f'inception:"{inception_date}", initialQuota:{initial_quota}, maxQuota:{initial_quota}, '
        f'tipo:"{tipo}", trib:"{trib}", expo:"{expo}", banco:"{banco}", obs:"" }},\n'
    )

    # Insere antes do fechamento do FUND_META
    src = src.replace(
        '};\n\nconst CHART_COLORS',
        f'{new_meta}}};\n\nconst CHART_COLORS',
        1
    )

    # Adiciona à lista FUNDS do JS (para o seletor de fundos)
    new_fund_js = (
        f'  {{ cnpjFmt: "{cnpj_fmt}", name: "{nome}", short: "{nome_curto}" }},\n'
    )
    src = src.replace(
        '];\n\nlet sortKey',
        f'  {{ cnpjFmt: "{cnpj_fmt}", name: "{nome}", short: "{nome_curto}" }},\n];\n\nlet sortKey',
        1
    )

    INDEX_PATH.write_text(src)
    print(f"  ✓ index.html: FUND_META e FUNDS atualizados")
    print(f"  NOTA: Verifique manualmente a entrada em FUND_META para ajustar obs se necessário")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 8:
        print("Uso: python add_fund.py CNPJ NOME NOME_CURTO TIPO TRIB EXPO BANCO")
        sys.exit(1)

    cnpj_digits = sys.argv[1].strip().replace(".", "").replace("/", "").replace("-", "")
    nome        = sys.argv[2].strip()
    nome_curto  = sys.argv[3].strip()
    tipo        = sys.argv[4].strip()
    trib        = sys.argv[5].strip()
    expo        = sys.argv[6].strip()
    banco       = sys.argv[7].strip()

    # Formata CNPJ: 14 dígitos → XX.XXX.XXX/XXXX-XX
    d = cnpj_digits.zfill(14)
    cnpj_fmt = f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

    print(f"=== Adicionando fundo ===")
    print(f"  CNPJ:      {cnpj_fmt}")
    print(f"  Nome:      {nome}")
    print(f"  Curto:     {nome_curto}")
    print(f"  Tipo:      {tipo} | {trib} | {expo} | {banco}")

    print(f"\n1. Buscando histórico completo na CVM…")
    quotas = fetch_full_history(cnpj_digits, cnpj_fmt)

    if not quotas:
        print(f"ERRO: Nenhuma cota encontrada para {cnpj_fmt}. Verifique o CNPJ.")
        sys.exit(1)

    sorted_dates = sorted(quotas.keys())
    inception_date  = sorted_dates[0]
    initial_quota   = quotas[inception_date]
    print(f"  Inception: {inception_date} (cota inicial: {initial_quota:.4f})")
    print(f"  Total: {len(quotas)} cotas ({sorted_dates[0]} → {sorted_dates[-1]})")

    print(f"\n2. Atualizando history.json…")
    update_history(cnpj_fmt, nome, quotas)

    print(f"\n3. Atualizando fetch_data.py…")
    update_fetch_data(cnpj_digits, cnpj_fmt, nome)

    print(f"\n4. Atualizando index.html…")
    update_index(cnpj_fmt, nome, nome_curto, tipo, trib, expo, banco,
                 inception_date, initial_quota)

    print(f"\n✓ Fundo {nome_curto} adicionado com sucesso!")
    print(f"  Verifique a entrada em FUND_META no index.html e ajuste obs/maxQuota se necessário.")


if __name__ == "__main__":
    main()
