#!/usr/bin/env python3
"""
Busca dados diários de cotas da CVM e calcula métricas para o Ranking de Fundos.
- Arquivos mensais (2021+): /INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip
- Arquivos anuais (pré-2021): /INF_DIARIO/DADOS/HIST/inf_diario_fi_YYYY.zip
- IBOV: Yahoo Finance
- CDI: API Banco Central (série 12)
"""

import json, zipfile, io, math, datetime, urllib.request, calendar
from pathlib import Path

# ── Lista de fundos ────────────────────────────────────────────────────────────
FUNDS = [
    {"name": "Tarpon GT FIF Cotas FIA",                                            "cnpj": "22232927000190", "cnpjFmt": "22.232.927/0001-90"},
    {"name": "Organon FIF Cotas FIA",                                              "cnpj": "17400251000166", "cnpjFmt": "17.400.251/0001-66"},
    {"name": "Artica Long Term FIA",                                               "cnpj": "18302338000163", "cnpjFmt": "18.302.338/0001-63"},
    {"name": "Genoa Capital Arpa CIC Classe FIM RL",                               "cnpj": "37495383000126", "cnpjFmt": "37.495.383/0001-26"},
    {"name": "Itaú Artax Ultra Multimercado FIF DA CIC RL",                        "cnpj": "42698666000105", "cnpjFmt": "42.698.666/0001-05"},
    {"name": "Guepardo Long Bias RV FIM",                                          "cnpj": "24623392000103", "cnpjFmt": "24.623.392/0001-03"},
    {"name": "Kapitalo Tarkus FIF Cotas FIA",                                      "cnpj": "28747685000153", "cnpjFmt": "28.747.685/0001-53"},
    {"name": "Real Investor FIC FIF Ações RL",                                     "cnpj": "10500884000105", "cnpjFmt": "10.500.884/0001-05"},
    {"name": "Gama Schroder Gaia Contour Tech Equity L&S BRL FIF CIC Mult IE RL", "cnpj": "35744790000102", "cnpjFmt": "35.744.790/0001-02"},
    {"name": "Patria Long Biased FIF Cotas FIM",                                   "cnpj": "38954217000103", "cnpjFmt": "38.954.217/0001-03"},
    {"name": "Absolute Pace Long Biased FIC FIF Ações RL",                        "cnpj": "32073525000143", "cnpjFmt": "32.073.525/0001-43"},
    {"name": "Arbor FIC FIA",                                                      "cnpj": "21689246000192", "cnpjFmt": "21.689.246/0001-92"},
    {"name": "Charles River FIF Ações",                                            "cnpj": "14438229000117", "cnpjFmt": "14.438.229/0001-17"},
    {"name": "SPX Falcon FIF CIC Ações RL",                                        "cnpj": "17397315000117", "cnpjFmt": "17.397.315/0001-17"},
]

FIRST_MONTHLY_YEAR = 2021   # CVM passou a publicar arquivos mensais a partir daqui
CVM_OLDEST_YEAR    = 2005   # Arquivos anuais HIST disponíveis a partir daqui

MONTHLY_CACHE: dict = {}
ANNUAL_CACHE:  dict = {}


# ── Fetch e parse ──────────────────────────────────────────────────────────────

def _parse_content(content: str) -> dict:
    """Parseia CSV da CVM e retorna estrutura com linhas e índices de colunas."""
    lines = content.split("\n")
    header = [h.strip().lstrip("\ufeff") for h in lines[0].split(";")]
    # Suporte aos dois formatos de coluna CNPJ da CVM
    col_cnpj  = next((i for i, h in enumerate(header) if h.startswith("CNPJ")), -1)
    col_date  = header.index("DT_COMPTC") if "DT_COMPTC" in header else -1
    col_quota = header.index("VL_QUOTA")  if "VL_QUOTA"  in header else -1
    return {"lines": lines, "col_cnpj": col_cnpj, "col_date": col_date, "col_quota": col_quota}


def _fetch_zip(url: str, timeout: int) -> str | None:
    """Baixa e descomprime um ZIP da CVM, retorna o conteúdo como string."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            return zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
    except Exception as e:
        return None


def fetch_monthly(year: int, month: int) -> dict | None:
    key = (year, month)
    if key in MONTHLY_CACHE:
        return MONTHLY_CACHE[key]
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{year}{month:02d}.zip"
    content = _fetch_zip(url, timeout=60)
    result = _parse_content(content) if content else None
    if result:
        print(f"  ✓ mensal {year}-{month:02d} ({len(result['lines'])} linhas)")
    else:
        print(f"  ✗ mensal {year}-{month:02d}: falhou")
    MONTHLY_CACHE[key] = result
    return result


def fetch_annual(year: int) -> dict | None:
    if year in ANNUAL_CACHE:
        return ANNUAL_CACHE[year]
    url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{year}.zip"
    content = _fetch_zip(url, timeout=120)
    result = _parse_content(content) if content else None
    if result:
        print(f"  ✓ anual  {year} ({len(result['lines'])} linhas)")
    else:
        print(f"  ✗ anual  {year}: falhou")
    ANNUAL_CACHE[year] = result
    return result


def _extract_rows(data: dict | None, fund: dict) -> list:
    """Extrai linhas (date, quota) de um bloco CSV para um fundo específico."""
    if not data or data["col_date"] < 0 or data["col_quota"] < 0:
        return []
    cnpj, fmt = fund["cnpj"], fund["cnpjFmt"]
    out = []
    for line in data["lines"][1:]:  # pula header
        # Filtra rapidamente antes de fazer split
        if cnpj not in line and fmt not in line:
            continue
        cols = line.split(";")
        try:
            # Valida que o CNPJ na coluna correta bate (evita falsos positivos)
            if data["col_cnpj"] >= 0:
                raw = cols[data["col_cnpj"]].strip().replace(".", "").replace("/", "").replace("-", "")
                if raw != cnpj:
                    continue
            d = cols[data["col_date"]].strip()
            q = float(cols[data["col_quota"]].replace(",", "."))
            if d and q > 0:
                out.append({"date": d, "quota": q})
        except (ValueError, IndexError):
            continue
    out.sort(key=lambda r: r["date"])
    return out


def rows_in_month(year: int, month: int, fund: dict) -> list:
    return _extract_rows(fetch_monthly(year, month), fund)


def rows_in_year(year: int, fund: dict) -> list:
    return _extract_rows(fetch_annual(year), fund)


# ── Datas e cálculos ───────────────────────────────────────────────────────────

def subtract_months(date: datetime.date, n: int) -> datetime.date:
    """Subtrai N meses de uma data, clampando o dia ao último do mês alvo."""
    total = date.year * 12 + (date.month - 1) - n
    y, m  = divmod(total, 12)
    m    += 1
    return datetime.date(y, m, min(date.day, calendar.monthrange(y, m)[1]))


def years_apart(a: str, b: str) -> float:
    return (datetime.date.fromisoformat(b) - datetime.date.fromisoformat(a)).days / 365.25


def cagr(start: float, end: float, years: float) -> float | None:
    if not start or not end or years <= 0:
        return None
    return (math.pow(end / start, 1.0 / years) - 1) * 100


def quota_on_or_before(target_date: datetime.date, fund: dict) -> dict | None:
    """Retorna a cota mais recente na data alvo ou antes. Busca até 3 períodos atrás."""
    ts = target_date.isoformat()
    y, m = target_date.year, target_date.month

    for _ in range(3):
        rows = rows_in_month(y, m, fund) if y >= FIRST_MONTHLY_YEAR else rows_in_year(y, fund)
        candidates = [r for r in rows if r["date"] <= ts]
        if candidates:
            return candidates[-1]
        # Retrocede um período
        if y >= FIRST_MONTHLY_YEAR:
            total = y * 12 + m - 2
            y, m  = divmod(total, 12)
            m    += 1
        else:
            y -= 1
    return None


def find_anchor_date(cur_year: int, cur_month: int) -> datetime.date:
    """
    Âncora = mediana das últimas datas de cota disponíveis entre os fundos,
    exigindo quorum de pelo menos metade dos fundos no mês.
    Robusto a gaps em qualquer fundo individual.
    """
    quorum = max(2, len(FUNDS) // 2)

    for delta in range(3):
        total = cur_year * 12 + cur_month - 1 - delta
        y, m  = divmod(total, 12)
        m    += 1
        last_dates = []
        for fund in FUNDS:
            rows = rows_in_month(y, m, fund)
            if rows:
                last_dates.append(datetime.date.fromisoformat(rows[-1]["date"]))
        if len(last_dates) >= quorum:
            last_dates.sort()
            anchor = last_dates[len(last_dates) // 2]  # mediana
            print(f"Anchor date: {anchor} ({len(last_dates)}/{len(FUNDS)} fundos com dados)")
            return anchor

    return datetime.date(cur_year, cur_month, 1)


def find_inception(fund: dict, anchor_year: int) -> dict | None:
    """
    Busca a primeira cota disponível do fundo na CVM.
    Para cedo ao encontrar 2 anos consecutivos com arquivo CVM mas sem o fundo.
    """
    print(f"    inception search: {fund['cnpjFmt']}")
    oldest_year_found = anchor_year
    consecutive_misses = 0

    for y in range(anchor_year - 1, CVM_OLDEST_YEAR - 1, -1):
        if y >= FIRST_MONTHLY_YEAR:
            rows        = rows_in_month(y, 12, fund)
            file_exists = MONTHLY_CACHE.get((y, 12)) is not None
        else:
            rows        = rows_in_year(y, fund)
            file_exists = ANNUAL_CACHE.get(y) is not None

        if rows:
            oldest_year_found  = y
            consecutive_misses = 0
            print(f"      encontrado em {y}")
        elif file_exists:
            consecutive_misses += 1
            if consecutive_misses >= 2:
                break
        # Arquivo não existe na CVM — pode ser gap, continua buscando

    print(f"      ano mais antigo: {oldest_year_found}")

    # Busca mês a mês no ano mais antigo (e no anterior por segurança)
    for scan_year in [oldest_year_found - 1, oldest_year_found]:
        if scan_year < CVM_OLDEST_YEAR:
            continue
        if scan_year >= FIRST_MONTHLY_YEAR:
            for m in range(1, 13):
                rows = rows_in_month(scan_year, m, fund)
                if rows:
                    print(f"      inception: {rows[0]['date']}")
                    return rows[0]
        else:
            rows = rows_in_year(scan_year, fund)
            if rows:
                print(f"      inception: {rows[0]['date']}")
                return rows[0]
    return None


# ── Processamento por fundo ────────────────────────────────────────────────────

def process_fund(fund: dict, anchor: datetime.date, prev_max_quotas: dict) -> dict:
    print(f"\n── {fund['name']}")

    latest = quota_on_or_before(anchor, fund)
    if not latest:
        print(f"  ✗ sem dados")
        return {**fund, "error": True}

    end_quota, end_date = latest["quota"], latest["date"]
    print(f"  cota atual: {end_quota} em {end_date}")

    # Flag: fundo está atrasado em relação à âncora global
    anchor_str = anchor.isoformat()
    is_delayed = end_date < anchor_str
    delay_days = (anchor - datetime.date.fromisoformat(end_date)).days if is_delayed else 0
    if is_delayed:
        print(f"  ⚠ atrasado {delay_days}d em relação à âncora ({anchor_str})")

    # Janelas de CAGR calculadas sempre a partir da âncora global —
    # garante que todos os fundos, CDI e IBOV usam exatamente o mesmo
    # ponto de início. A cota de fim pode ficar levemente antes da âncora
    # quando o fundo está atrasado; isso fica registrado em isDelayed/delayDays.
    a12 = subtract_months(anchor, 12)
    a36 = subtract_months(anchor, 36)
    a60 = subtract_months(anchor, 60)

    q12 = quota_on_or_before(a12, fund)
    q36 = quota_on_or_before(a36, fund)
    q60 = quota_on_or_before(a60, fund)

    inception = find_inception(fund, anchor.year)
    inc_quota = inception["quota"] if inception else None
    inc_date  = inception["date"]  if inception else None

    def do_cagr(q):
        if not q:
            return None
        yrs = years_apart(q["date"], end_date)
        return cagr(q["quota"], end_quota, yrs)

    # Memoriza máxima histórica — nunca decresce
    prev      = prev_max_quotas.get(fund["cnpjFmt"], {})
    prev_max  = prev.get("maxQuota") or 0.0
    if end_quota >= prev_max:
        max_quota      = end_quota
        max_quota_date = end_date
        print(f"  nova máxima: {max_quota} em {max_quota_date}")
    else:
        max_quota      = prev_max
        max_quota_date = prev.get("maxQuotaDate", "")

    result = {
        "name":          fund["name"],
        "cnpj":          fund["cnpjFmt"],
        "cnpjFmt":       fund["cnpjFmt"],
        "latestDate":    end_date,
        "latestQuota":   end_quota,
        "isDelayed":     is_delayed,
        "delayDays":     delay_days,
        "maxQuota":      max_quota,
        "maxQuotaDate":  max_quota_date,
        "inceptionDate": inc_date,
        "anchorDate":    anchor.isoformat(),
        "anchor12m":     a12.isoformat(),
        "anchor36m":     a36.isoformat(),
        "anchor60m":     a60.isoformat(),
        "cagr12":        do_cagr(q12),
        "cagr36":        do_cagr(q36),
        "cagr60":        do_cagr(q60),
        "cagrInception": cagr(inc_quota, end_quota, years_apart(inc_date, end_date)) if inc_date else None,
        "error":         False,
    }
    def _fmt(v): return f"{v:.2f}" if v is not None else "N/D"
    print(f"  CAGR 12M={_fmt(result['cagr12'])} 36M={_fmt(result['cagr36'])} 60M={_fmt(result['cagr60'])}")
    return result


# ── Benchmarks ─────────────────────────────────────────────────────────────────

def _best_price_and_date(price_map: dict, dates: list, target: datetime.date):
    """Retorna (preço, data_str) para a data mais recente <= target."""
    tstr       = target.isoformat()
    candidates = [d for d in dates if d <= tstr]
    if not candidates:
        return None, None
    d = candidates[-1]
    return price_map[d], d


def fetch_ibov(anchor: datetime.date, a12: datetime.date, a36: datetime.date, a60: datetime.date) -> dict:
    """Busca preços do Ibovespa no Yahoo Finance e calcula CAGRs."""
    ticker  = "%5EBVSP"
    period1 = int(datetime.datetime.combine(
        a60 - datetime.timedelta(days=10), datetime.time(),
        tzinfo=datetime.timezone.utc).timestamp())
    period2 = int(datetime.datetime.combine(
        anchor + datetime.timedelta(days=5), datetime.time(),
        tzinfo=datetime.timezone.utc).timestamp())
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?interval=1d&period1={period1}&period2={period2}")
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        result     = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes     = result["indicators"]["quote"][0]["close"]
        price_map  = {
            datetime.datetime.utcfromtimestamp(ts).date().isoformat(): price
            for ts, price in zip(timestamps, closes) if price is not None
        }
        dates = sorted(price_map.keys())

        p_anchor, d_anchor = _best_price_and_date(price_map, dates, anchor)
        p12,      d12      = _best_price_and_date(price_map, dates, a12)
        p36,      d36      = _best_price_and_date(price_map, dates, a36)
        p60,      d60      = _best_price_and_date(price_map, dates, a60)

        def ibov_cagr(d_s, d_e, p_s, p_e):
            if not all([d_s, d_e, p_s, p_e]): return None
            return cagr(p_s, p_e, years_apart(d_s, d_e))

        result_ibov = {
            "cagr12": ibov_cagr(d12, d_anchor, p12, p_anchor),
            "cagr36": ibov_cagr(d36, d_anchor, p36, p_anchor),
            "cagr60": ibov_cagr(d60, d_anchor, p60, p_anchor),
        }
        vals = {k: f"{v:.2f}%" if v is not None else "N/D" for k, v in result_ibov.items()}
        print(f"  IBOV 12M={vals['cagr12']} 36M={vals['cagr36']} 60M={vals['cagr60']}")
        return result_ibov
    except Exception as e:
        print(f"  ✗ IBOV falhou: {e}")
        return {"cagr12": None, "cagr36": None, "cagr60": None}


def fetch_cdi(anchor: datetime.date, a12: datetime.date, a36: datetime.date, a60: datetime.date) -> dict:
    """Busca taxa CDI diária no Banco Central (série 12) e calcula CAGRs acumulados."""
    # Busca com margem de 10 dias antes do a60 para garantir cobertura
    start = a60 - datetime.timedelta(days=10)
    url   = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
             f"?formato=json"
             f"&dataInicial={start.strftime('%d/%m/%Y')}"
             f"&dataFinal={anchor.strftime('%d/%m/%Y')}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        if not data:
            raise ValueError("Resposta vazia do BCB")

        # Constrói índice acumulado: cada entrada é taxa diária em %
        price_map: dict = {}
        acc = 1.0
        for entry in data:
            d    = datetime.datetime.strptime(entry["data"], "%d/%m/%Y").date().isoformat()
            acc *= 1 + float(entry["valor"]) / 100
            price_map[d] = acc
        dates = sorted(price_map.keys())

        p_anchor, d_anchor = _best_price_and_date(price_map, dates, anchor)
        p12,      d12      = _best_price_and_date(price_map, dates, a12)
        p36,      d36      = _best_price_and_date(price_map, dates, a36)
        p60,      d60      = _best_price_and_date(price_map, dates, a60)

        def cdi_cagr(d_s, d_e, p_s, p_e):
            if not all([d_s, d_e, p_s, p_e]): return None
            return cagr(p_s, p_e, years_apart(d_s, d_e))

        result_cdi = {
            "cagr12": cdi_cagr(d12, d_anchor, p12, p_anchor),
            "cagr36": cdi_cagr(d36, d_anchor, p36, p_anchor),
            "cagr60": cdi_cagr(d60, d_anchor, p60, p_anchor),
        }
        vals = {k: f"{v:.2f}%" if v is not None else "N/D" for k, v in result_cdi.items()}
        print(f"  CDI  12M={vals['cagr12']} 36M={vals['cagr36']} 60M={vals['cagr60']}")
        return result_cdi
    except Exception as e:
        print(f"  ✗ CDI falhou: {e}")
        return {"cagr12": None, "cagr36": None, "cagr60": None}


# ── history.json ───────────────────────────────────────────────────────────────

def update_history(anchor: datetime.date) -> None:
    """
    Atualiza history.json incrementalmente.
    - Carrega cotas existentes
    - Adiciona apenas datas novas dos meses atual e anterior
    - Janela deslizante de 3 anos
    - Aceita datas presentes em >= 80% dos fundos (evita que um fundo com
      gap esparso encole toda a série)
    - Para fundos ausentes numa data aceita, interpola a cota em log-retorno
      a partir da cota anterior e posterior conhecidas
    - Recalcula retornos diários, correlação de Pearson e drawdown máximo
    """
    print(f"\n── Atualizando history.json")
    hist_path = Path(__file__).parent.parent / "docs" / "history.json"

    # Carregar histórico existente
    quotas: dict = {f["cnpjFmt"]: {} for f in FUNDS}
    if hist_path.exists():
        try:
            existing = json.loads(hist_path.read_text())
            for cnpj, fd in existing.get("funds", {}).items():
                if cnpj in quotas:
                    quotas[cnpj] = dict(zip(fd["dates"], fd["quotas"]))
            n_existing = len(next(iter(quotas.values()), {}))
            print(f"  Histórico existente: {n_existing} datas")
        except Exception as e:
            print(f"  Erro ao ler history.json: {e} — iniciando do zero")

    # Adicionar mês atual e anterior (reutiliza cache do main)
    prev_month = anchor.replace(day=1) - datetime.timedelta(days=1)
    for year, month in sorted({(anchor.year, anchor.month), (prev_month.year, prev_month.month)}):
        added = 0
        for fund in FUNDS:
            for row in rows_in_month(year, month, fund):
                d, q = row["date"], row["quota"]
                if d not in quotas[fund["cnpjFmt"]]:
                    quotas[fund["cnpjFmt"]][d] = q
                    added += 1
        print(f"  {year}-{month:02d}: {added} novas cotas")

    # ── Janela deslizante de 3 anos com limiar de presença ─────────────────
    # Aceita datas onde pelo menos PRESENCE_THRESHOLD dos fundos têm cota real.
    # Isso evita que um único fundo com dados esparsos encolha toda a série.
    PRESENCE_THRESHOLD = 0.80
    min_funds_required = max(2, int(len(FUNDS) * PRESENCE_THRESHOLD))

    cutoff = subtract_months(anchor, 36).isoformat()

    # Conta fundos presentes em cada data
    date_counts: dict[str, int] = {}
    for fund in FUNDS:
        for d in quotas[fund["cnpjFmt"]]:
            if d >= cutoff:
                date_counts[d] = date_counts.get(d, 0) + 1

    common_dates = sorted(d for d, cnt in date_counts.items() if cnt >= min_funds_required)

    if not common_dates:
        print("  Sem datas suficientes — history.json não atualizado")
        return

    # Conta quantas datas foram incluídas por presença parcial vs interseção estrita
    strict_count = len(set.intersection(*[
        set(d for d in quotas[f["cnpjFmt"]] if d >= cutoff) for f in FUNDS
    ])) if FUNDS else 0
    print(f"  Datas aceitas: {len(common_dates)} "
          f"(interseção estrita seria {strict_count}) "
          f"({common_dates[0]} → {common_dates[-1]})")

    # ── Interpolação para fundos ausentes numa data aceita ──────────────────
    # Usa interpolação geométrica (log-retorno linear), que é a correta para
    # séries de cotas — equivale a supor retorno diário constante no gap.
    interpolated_total = 0
    for fund in FUNDS:
        cnpj       = fund["cnpjFmt"]
        qs         = quotas[cnpj]
        all_dates  = sorted(qs.keys())

        for d in common_dates:
            if d in qs:
                continue  # dado real — não interpola

            # Vizinhos mais próximos com dado real
            prev_d = next((x for x in reversed(all_dates) if x < d), None)
            next_d = next((x for x in all_dates           if x > d), None)

            if prev_d and next_d and qs.get(prev_d) and qs.get(next_d):
                t0    = datetime.date.fromisoformat(prev_d)
                t1    = datetime.date.fromisoformat(next_d)
                td    = datetime.date.fromisoformat(d)
                alpha = (td - t0).days / max((t1 - t0).days, 1)
                # Interpolação geométrica: q(t) = q0 * (q1/q0)^alpha
                interp = qs[prev_d] * ((qs[next_d] / qs[prev_d]) ** alpha)
                qs[d]  = round(interp, 8)
                interpolated_total += 1
            elif prev_d and qs.get(prev_d):
                # Sem vizinho posterior: mantém a última cota conhecida (carry-forward)
                qs[d] = qs[prev_d]
                interpolated_total += 1

        quotas[cnpj] = qs

    if interpolated_total:
        print(f"  Interpoladas {interpolated_total} cotas ausentes")

    # ── Retornos diários ────────────────────────────────────────────────────
    returns_by_fund: dict = {}
    for fund in FUNDS:
        qs   = quotas[fund["cnpjFmt"]]
        rets = []
        for i in range(1, len(common_dates)):
            q0 = qs.get(common_dates[i-1])
            q1 = qs.get(common_dates[i])
            rets.append((q1 / q0) - 1 if q0 and q1 else 0.0)
        returns_by_fund[fund["cnpjFmt"]] = rets

    # Correlação de Pearson
    def pearson(a: list, b: list) -> float:
        n = len(a)
        if n < 2: return 0.0
        ma, mb = sum(a) / n, sum(b) / n
        num    = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
        sa     = math.sqrt(sum((x - ma) ** 2 for x in a))
        sb     = math.sqrt(sum((x - mb) ** 2 for x in b))
        return round(num / (sa * sb), 4) if sa * sb > 0 else 0.0

    cnpjs = [f["cnpjFmt"] for f in FUNDS]
    corr  = {ca: {cb: pearson(returns_by_fund[ca], returns_by_fund[cb]) for cb in cnpjs} for ca in cnpjs}

    # Drawdown máximo por fundo
    def max_dd(rets: list) -> float:
        cum = peak = 1.0
        dd_max = 0.0
        for r in rets:
            cum *= (1 + r)
            if cum > peak: peak = cum
            dd = (cum - peak) / peak
            if dd < dd_max: dd_max = dd
        return round(dd_max * 100, 2)

    funds_out = {
        fund["cnpjFmt"]: {
            "nome":        fund["name"],
            "dates":       common_dates,
            "quotas":      [quotas[fund["cnpjFmt"]].get(d, 0.0) for d in common_dates],
            "returns":     returns_by_fund[fund["cnpjFmt"]],
            "maxDrawdown": max_dd(returns_by_fund[fund["cnpjFmt"]]),
        }
        for fund in FUNDS
    }

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "from":        common_dates[0],
        "to":          common_dates[-1],
        "commonDates": common_dates,
        "correlation": corr,
        "funds":       funds_out,
    }

    hist_path.write_text(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
    print(f"  ✓ history.json atualizado ({hist_path.stat().st_size // 1024} KB, {len(common_dates)} datas)")


def reconstruct_max_quotas_from_history(hist_path: "Path") -> dict:
    """Fallback: reconstrói maxQuota varrendo todas as cotas salvas no history.json."""
    if not hist_path.exists():
        return {}
    try:
        hist = json.loads(hist_path.read_text())
        result = {}
        for cnpj, fd in hist.get("funds", {}).items():
            dates  = fd.get("dates", [])
            quotas = fd.get("quotas", [])
            if not quotas:
                continue
            max_idx = quotas.index(max(quotas))
            result[cnpj] = {
                "maxQuota":     quotas[max_idx],
                "maxQuotaDate": dates[max_idx] if max_idx < len(dates) else "",
            }
        print(f"  Reconstruídos {len(result)} maxQuotas do history.json (fallback)")
        return result
    except Exception as e:
        print(f"  Não foi possível reconstruir maxQuotas do history.json: {e}")
        return {}


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    today = datetime.date.today()
    print(f"Executando para {today.isoformat()}")

    anchor = find_anchor_date(today.year, today.month)
    a12    = subtract_months(anchor, 12)
    a36    = subtract_months(anchor, 36)
    a60    = subtract_months(anchor, 60)

    print(f"Janelas: 12M={a12} → {anchor}  36M={a36} → {anchor}  60M={a60} → {anchor}")

    # Carrega maxQuota anterior do data.json para não perder picos históricos
    out_path        = Path(__file__).parent.parent / "docs" / "data.json"
    hist_path       = Path(__file__).parent.parent / "docs" / "history.json"
    prev_max_quotas = {}
    if out_path.exists():
        try:
            prev = json.loads(out_path.read_text())
            for f in prev.get("funds", []):
                if f.get("cnpjFmt") and f.get("maxQuota"):
                    prev_max_quotas[f["cnpjFmt"]] = {
                        "maxQuota":     f["maxQuota"],
                        "maxQuotaDate": f.get("maxQuotaDate", ""),
                    }
            print(f"Carregados {len(prev_max_quotas)} maxQuotas do data.json anterior")
        except Exception as e:
            print(f"Não foi possível ler data.json anterior: {e}")

    # Fallback: reconstrói do history.json se data.json estava vazio ou corrompido
    if not prev_max_quotas:
        prev_max_quotas = reconstruct_max_quotas_from_history(hist_path)

    results = [process_fund(f, anchor, prev_max_quotas) for f in FUNDS]

    # Reporta fundos atrasados em relação à âncora
    delayed = [r for r in results if not r.get("error") and r.get("isDelayed")]
    if delayed:
        print(f"\n⚠ Fundos com cota atrasada em relação à âncora ({anchor}):")
        for r in delayed:
            print(f"  {r['name']}: última cota em {r['latestDate']} ({r['delayDays']}d de atraso)")

    print(f"\n── Ibovespa")
    ibov = fetch_ibov(anchor, a12, a36, a60)

    print(f"\n── CDI")
    cdi = fetch_cdi(anchor, a12, a36, a60)

    data_out = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "anchorDate":  anchor.isoformat(),
        "ibov":        ibov,
        "cdi":         cdi,
        "funds":       results,
    }

    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(data_out, ensure_ascii=False, indent=2))
    print(f"\n✓ data.json escrito ({len(results)} fundos)")

    update_history(anchor)


if __name__ == "__main__":
    main()
