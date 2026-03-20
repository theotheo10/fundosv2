#!/usr/bin/env python3
"""
Busca dados diários de cotas da CVM e calcula métricas para o Ranking de Fundos.

ESTRATÉGIA DE HISTÓRICO:
  - history.json NUNCA é truncado — cresce a cada execução.
  - Na primeira execução (ou se history.json estiver vazio), faz backfill completo
    desde HISTORY_START_YEAR até hoje.
  - Nas execuções seguintes, adiciona apenas meses com dados mais recentes que
    a última data já salva.
  - Sem janela deslizante: toda métrica do index.html usa o histórico completo.

Fontes:
  - Cotas: CVM /INF_DIARIO (arquivos mensais 2021+, anuais HIST pré-2021)
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
    {"name": "Absolute Pace Long Biased FIC FIF Ações RL",                         "cnpj": "32073525000143", "cnpjFmt": "32.073.525/0001-43"},
    {"name": "Arbor FIC FIA",                                                      "cnpj": "21689246000192", "cnpjFmt": "21.689.246/0001-92"},
    {"name": "Charles River FIF Ações",                                            "cnpj": "14438229000117", "cnpjFmt": "14.438.229/0001-17"},
    {"name": "SPX Falcon FIF CIC Ações RL",                                        "cnpj": "17397315000117", "cnpjFmt": "17.397.315/0001-17"},
    {"name": "Opportunity Global Equity Real Institucional FIC FIF Ações IE RL",        "cnpj": "46351969000108", "cnpjFmt": "46.351.969/0001-08"},
    {"name": "SPX Patriot FIF CIC Ações RL", "cnpj": "15334585000153", "cnpjFmt": "15.334.585/0001-53"},
    {"name": "TB FIF Cotas FIA", "cnpj": "47511351000120", "cnpjFmt": "47.511.351/0001-20"},
    {"name": "Itaú Janeiro Multimercado FIF DA Classe FIC RL ATIVO", "cnpj": "52116227000109", "cnpjFmt": "52.116.227/0001-09"},
]

FIRST_MONTHLY_YEAR = 2021   # CVM: arquivos mensais a partir daqui
CVM_OLDEST_YEAR    = 2005   # CVM: arquivos anuais HIST a partir daqui
HISTORY_START_YEAR = 2019   # Início do backfill histórico (ajuste se quiser mais)

MONTHLY_CACHE: dict = {}
ANNUAL_CACHE:  dict = {}


# ── Fetch e parse ──────────────────────────────────────────────────────────────

def _parse_content(content: str) -> dict:
    lines = content.split("\n")
    header = [h.strip().lstrip("\ufeff") for h in lines[0].split(";")]
    col_cnpj  = next((i for i, h in enumerate(header) if h.startswith("CNPJ")), -1)
    col_date  = header.index("DT_COMPTC") if "DT_COMPTC" in header else -1
    col_quota = header.index("VL_QUOTA")  if "VL_QUOTA"  in header else -1
    return {"lines": lines, "col_cnpj": col_cnpj, "col_date": col_date, "col_quota": col_quota}


def _fetch_zip(url: str, timeout: int) -> str | None:
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
    if not data or data["col_date"] < 0 or data["col_quota"] < 0:
        return []
    cnpj, fmt = fund["cnpj"], fund["cnpjFmt"]
    out = []
    for line in data["lines"][1:]:
        if cnpj not in line and fmt not in line:
            continue
        cols = line.split(";")
        try:
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
    ts = target_date.isoformat()
    y, m = target_date.year, target_date.month
    for _ in range(3):
        rows = rows_in_month(y, m, fund) if y >= FIRST_MONTHLY_YEAR else rows_in_year(y, fund)
        candidates = [r for r in rows if r["date"] <= ts]
        if candidates:
            return candidates[-1]
        if y >= FIRST_MONTHLY_YEAR:
            total = y * 12 + m - 2
            y, m  = divmod(total, 12)
            m    += 1
        else:
            y -= 1
    return None


def find_anchor_date(cur_year: int, cur_month: int) -> datetime.date:
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
            anchor = last_dates[len(last_dates) // 2]
            print(f"Anchor date: {anchor} ({len(last_dates)}/{len(FUNDS)} fundos com dados)")
            return anchor
    return datetime.date(cur_year, cur_month, 1)


def find_inception(fund: dict, anchor_year: int) -> dict | None:
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
    print(f"      ano mais antigo: {oldest_year_found}")
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


# ── Processamento por fundo (data.json) ───────────────────────────────────────

def process_fund(fund: dict, anchor: datetime.date, prev_max_quotas: dict) -> dict:
    print(f"\n── {fund['name']}")
    latest = quota_on_or_before(anchor, fund)
    if not latest:
        print(f"  ✗ sem dados")
        return {**fund, "error": True}

    end_quota, end_date = latest["quota"], latest["date"]
    print(f"  cota atual: {end_quota} em {end_date}")

    anchor_str = anchor.isoformat()
    is_delayed = end_date < anchor_str
    delay_days = (anchor - datetime.date.fromisoformat(end_date)).days if is_delayed else 0
    if is_delayed:
        print(f"  ⚠ atrasado {delay_days}d em relação à âncora ({anchor_str})")

    a12 = subtract_months(anchor, 12)
    a36 = subtract_months(anchor, 36)
    a60 = subtract_months(anchor, 60)

    q12 = quota_on_or_before(a12, fund)
    q36 = quota_on_or_before(a36, fund)
    q60 = quota_on_or_before(a60, fund)

    inception   = find_inception(fund, anchor.year)
    inc_quota   = inception["quota"] if inception else None
    inc_date    = inception["date"]  if inception else None

    def do_cagr(q):
        if not q: return None
        return cagr(q["quota"], end_quota, years_apart(q["date"], end_date))

    prev      = prev_max_quotas.get(fund["cnpjFmt"], {})
    prev_max  = prev.get("maxQuota") or 0.0
    # maxQuota = max(histórico completo, cota atual)
    # prev_max_quotas já vem do history.json (via reconstruct_max_quotas_from_history)
    # ou do data.json anterior — em ambos os casos, comparamos com a cota atual
    if end_quota > prev_max:
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
    tstr = target.isoformat()
    candidates = [d for d in dates if d <= tstr]
    if not candidates: return None, None
    d = candidates[-1]
    return price_map[d], d


def fetch_ibov(anchor: datetime.date, a12: datetime.date, a36: datetime.date, a60: datetime.date) -> dict:
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
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
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
        p12, d12 = _best_price_and_date(price_map, dates, a12)
        p36, d36 = _best_price_and_date(price_map, dates, a36)
        p60, d60 = _best_price_and_date(price_map, dates, a60)
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
        price_map: dict = {}
        acc = 1.0
        for entry in data:
            d    = datetime.datetime.strptime(entry["data"], "%d/%m/%Y").date().isoformat()
            acc *= 1 + float(entry["valor"]) / 100
            price_map[d] = acc
        dates = sorted(price_map.keys())
        p_anchor, d_anchor = _best_price_and_date(price_map, dates, anchor)
        p12, d12 = _best_price_and_date(price_map, dates, a12)
        p36, d36 = _best_price_and_date(price_map, dates, a36)
        p60, d60 = _best_price_and_date(price_map, dates, a60)
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


# ── history.json — histórico crescente ────────────────────────────────────────

def months_to_fetch(last_date_in_history: str | None, anchor: datetime.date) -> list:
    """
    Retorna lista de (year, month) a buscar na CVM.

    - Se history.json está vazio/ausente → backfill completo desde HISTORY_START_YEAR.
    - Se já tem dados → busca apenas meses a partir do mês da última data salva.
      Inclui o mês anterior ao atual para cobrir datas que chegam com atraso.
    """
    if last_date_in_history is None:
        # Backfill completo
        start_year  = HISTORY_START_YEAR
        start_month = 1
        print(f"  Backfill completo desde {start_year}-01")
    else:
        last = datetime.date.fromisoformat(last_date_in_history)
        start_year  = last.year
        start_month = last.month
        print(f"  Incremental desde {last_date_in_history}")

    result = []
    y, m = start_year, start_month
    while (y, m) <= (anchor.year, anchor.month):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def update_history(anchor: datetime.date) -> None:
    """
    Atualiza history.json de forma cumulativa — nunca trunca o histórico.

    Comportamento:
    - Carrega todas as cotas já salvas.
    - Determina quais meses ainda não foram buscados (incremental) ou
      faz backfill completo se o arquivo estiver vazio.
    - Adiciona novas cotas sem remover as antigas.
    - Reconstrói retornos, correlação e drawdown sobre o histórico completo.
    - Sem cutoff de data — o arquivo cresce indefinidamente.
    """
    print(f"\n── Atualizando history.json (histórico completo, sem truncar)")
    hist_path = Path(__file__).parent.parent / "docs" / "history.json"

    # ── Carregar histórico existente ────────────────────────────────────────
    quotas: dict = {f["cnpjFmt"]: {} for f in FUNDS}
    last_date_in_history = None
    existing_fund_cnpjs: set = set()

    if hist_path.exists():
        try:
            existing = json.loads(hist_path.read_text())
            for cnpj, fd in existing.get("funds", {}).items():
                if cnpj in quotas:
                    quotas[cnpj] = dict(zip(fd["dates"], fd["quotas"]))
                    existing_fund_cnpjs.add(cnpj)
            # Data mais recente no arquivo atual
            all_dates = sorted(existing.get("commonDates", []))
            if all_dates:
                last_date_in_history = all_dates[-1]
                print(f"  Histórico existente: {len(all_dates)} datas "
                      f"({all_dates[0]} → {all_dates[-1]})")
            else:
                print("  history.json existe mas está vazio — iniciando backfill")
        except Exception as e:
            print(f"  Erro ao ler history.json: {e} — iniciando backfill completo")

    # ── Detectar fundos novos (não presentes no history.json anterior) ───────
    all_fund_cnpjs = {f["cnpjFmt"] for f in FUNDS}
    new_funds = all_fund_cnpjs - existing_fund_cnpjs
    if new_funds:
        new_names = [f["name"] for f in FUNDS if f["cnpjFmt"] in new_funds]
        print(f"  ⚠ Fundos novos detectados (sem histórico): {', '.join(new_names)}")
        print(f"    → Forçando backfill completo para incluí-los")
        last_date_in_history = None  # força backfill completo de todos os meses

    # ── Detectar fundos com histórico esparso (add_fund rodou com CVM incompleta) ─
    # Um fundo adicionado quando a CVM tinha poucas cotas vai ter muitos zeros/gaps.
    # Se um fundo tem menos de 60% das cotas esperadas desde sua inception,
    # forçamos backfill completo para recuperar cotas que chegaram depois.
    if last_date_in_history is not None:  # só se não já forçou backfill
        sparse_funds = []
        for f in FUNDS:
            cnpj = f["cnpjFmt"]
            if cnpj not in quotas:
                continue
            qs = quotas[cnpj]
            real_cotas = sum(1 for v in qs.values() if v and v > 0)
            if real_cotas == 0:
                continue
            # Encontrar a data mais antiga de cota real
            sorted_real = sorted(d for d, v in qs.items() if v and v > 0)
            inception_str = sorted_real[0]
            # Dias de mercado esperados desde inception até hoje (aprox 252/ano)
            import datetime as _dt
            try:
                inc = _dt.date.fromisoformat(inception_str)
                today_d = _dt.date.fromisoformat(last_date_in_history)
                years = (today_d - inc).days / 365.25
                expected = int(years * 252)
                if expected > 60 and real_cotas < expected * 0.6:
                    sparse_funds.append(f["name"])
                    print(f"  ⚠ {f['name']}: apenas {real_cotas} cotas reais "
                          f"(esperado ~{expected} para {years:.1f} anos desde {inception_str})")
            except Exception:
                pass
        if sparse_funds:
            print(f"  → Fundos com histórico esparso: {', '.join(sparse_funds)}")
            print(f"    → Forçando backfill completo para recuperar cotas CVM retroativas")
            last_date_in_history = None

    # ── Determinar meses a buscar ────────────────────────────────────────────
    to_fetch = months_to_fetch(last_date_in_history, anchor)
    print(f"  Meses a buscar: {len(to_fetch)} "
          f"({to_fetch[0][0]}-{to_fetch[0][1]:02d} → "
          f"{to_fetch[-1][0]}-{to_fetch[-1][1]:02d})")

    # ── Buscar e acumular cotas ──────────────────────────────────────────────
    for year, month in to_fetch:
        added = 0
        for fund in FUNDS:
            if year >= FIRST_MONTHLY_YEAR:
                rows = rows_in_month(year, month, fund)
            else:
                # Para anos pré-2021, usa arquivo anual (já cacheado se buscado antes)
                if month == 1:  # busca o arquivo anual apenas uma vez por ano
                    rows = rows_in_year(year, fund)
                else:
                    rows = _extract_rows(ANNUAL_CACHE.get(year), fund)
                    # Filtra apenas o mês atual
                    month_str = f"{year}-{month:02d}"
                    rows = [r for r in rows if r["date"].startswith(month_str)]

            for row in rows:
                d, q = row["date"], row["quota"]
                if d not in quotas[fund["cnpjFmt"]]:
                    quotas[fund["cnpjFmt"]][d] = q
                    added += 1
        if added:
            print(f"  {year}-{month:02d}: +{added} novas cotas")

    # ── Selecionar datas comuns ──────────────────────────────────────────────
    # Aceita datas onde >= 80% dos fundos têm cota (evita que gap de um fundo
    # encole toda a série). Sem cutoff de data — usa TODO o histórico disponível.
    PRESENCE_THRESHOLD = 0.80
    min_funds_required = max(2, int(len(FUNDS) * PRESENCE_THRESHOLD))

    date_counts: dict[str, int] = {}
    for fund in FUNDS:
        for d in quotas[fund["cnpjFmt"]]:
            date_counts[d] = date_counts.get(d, 0) + 1

    common_dates = sorted(d for d, cnt in date_counts.items()
                          if cnt >= min_funds_required)

    if not common_dates:
        print("  Sem datas suficientes — history.json não atualizado")
        return

    print(f"  Datas aceitas: {len(common_dates)} ({common_dates[0]} → {common_dates[-1]})")

    # ── Interpolação para fundos ausentes numa data aceita ───────────────────
    # Interpolação geométrica (log-retorno linear) — correta para séries de cotas.
    # Equivale a supor retorno diário constante no gap.
    interpolated_total = 0
    for fund in FUNDS:
        cnpj      = fund["cnpjFmt"]
        qs        = quotas[cnpj]
        all_dates = sorted(qs.keys())

        for d in common_dates:
            if d in qs:
                continue
            prev_d = next((x for x in reversed(all_dates) if x < d), None)
            next_d = next((x for x in all_dates           if x > d), None)

            if prev_d and next_d and qs.get(prev_d) and qs.get(next_d):
                t0    = datetime.date.fromisoformat(prev_d)
                t1    = datetime.date.fromisoformat(next_d)
                td    = datetime.date.fromisoformat(d)
                alpha = (td - t0).days / max((t1 - t0).days, 1)
                qs[d] = qs[prev_d] * ((qs[next_d] / qs[prev_d]) ** alpha)
                qs[d] = round(qs[d], 8)
                interpolated_total += 1
            elif prev_d and qs.get(prev_d):
                qs[d] = qs[prev_d]
                interpolated_total += 1

        quotas[cnpj] = qs

    if interpolated_total:
        print(f"  Interpoladas {interpolated_total} cotas ausentes")

    # ── Retornos diários ─────────────────────────────────────────────────────
    returns_by_fund: dict = {}
    for fund in FUNDS:
        qs   = quotas[fund["cnpjFmt"]]
        rets = []
        for i in range(1, len(common_dates)):
            q0 = qs.get(common_dates[i-1])
            q1 = qs.get(common_dates[i])
            rets.append((q1 / q0) - 1 if q0 and q1 else None)  # None = pre-inception or gap
        returns_by_fund[fund["cnpjFmt"]] = rets

    # ── Correlação de Pearson ────────────────────────────────────────────────
    def first_real_idx(cnpj: str) -> int:
        rets = returns_by_fund[cnpj]
        for i, r in enumerate(rets):
            if r != 0.0:
                return max(0, i - 1)
        return 0

    def pearson_real(ca: str, cb: str) -> float:
        """Pearson using only dates where both funds have real (non-zero) returns."""
        ra = returns_by_fund[ca]
        rb = returns_by_fund[cb]
        # Build aligned pairs where both have real data
        pairs = [(ra[i], rb[i]) for i in range(min(len(ra), len(rb)))
                 if ra[i] is not None and rb[i] is not None]
        n = len(pairs)
        if n < 30: return 0.0
        a = [p[0] for p in pairs]
        b = [p[1] for p in pairs]
        ma, mb = sum(a) / n, sum(b) / n
        num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
        sa  = math.sqrt(sum((x - ma) ** 2 for x in a))
        sb  = math.sqrt(sum((x - mb) ** 2 for x in b))
        return round(num / (sa * sb), 4) if sa * sb > 0 else 0.0

    cnpjs = [f["cnpjFmt"] for f in FUNDS]
    corr  = {ca: {cb: (1.0 if ca == cb else pearson_real(ca, cb))
                  for cb in cnpjs} for ca in cnpjs}

    # ── Drawdown máximo ──────────────────────────────────────────────────────
    def max_dd(rets: list) -> float:
        # Skip leading zeros (pre-inception)
        start = 0
        for i, r in enumerate(rets):
            if r is not None:
                start = max(0, i - 1)
                break
        cum = peak = 1.0
        dd_max = 0.0
        for r in rets[start:]:
            if r is None: continue
            cum *= (1 + r)
            if cum > peak: peak = cum
            dd = (cum - peak) / peak
            if dd < dd_max: dd_max = dd
        return round(dd_max * 100, 2)

    # ── Serializar ───────────────────────────────────────────────────────────
    funds_out = {
        fund["cnpjFmt"]: {
            "nome":        fund["name"],
            "dates":       common_dates,
            "quotas":      [quotas[fund["cnpjFmt"]].get(d) for d in common_dates],  # None = pre-inception
            "returns":     returns_by_fund[fund["cnpjFmt"]],
            "maxDrawdown": max_dd(returns_by_fund[fund["cnpjFmt"]]),
        }
        for fund in FUNDS
    }

    n_days  = len(common_dates)
    n_years = (datetime.date.fromisoformat(common_dates[-1]) -
               datetime.date.fromisoformat(common_dates[0])).days / 365.25

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "from":        common_dates[0],
        "to":          common_dates[-1],
        "nDays":       n_days,
        "nYears":      round(n_years, 2),
        "commonDates": common_dates,
        "correlation": corr,
        "funds":       funds_out,
    }

    hist_path.write_text(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
    size_kb = hist_path.stat().st_size // 1024
    print(f"  ✓ history.json: {n_days} pregões, {n_years:.1f} anos, {size_kb} KB")


def reconstruct_max_quotas_from_history(hist_path: Path) -> dict:
    if not hist_path.exists():
        return {}
    try:
        hist = json.loads(hist_path.read_text())
        result = {}
        for cnpj, fd in hist.get("funds", {}).items():
            dates  = fd.get("dates", [])
            quotas = fd.get("quotas", [])
            if not quotas: continue
            max_idx = quotas.index(max(quotas))
            result[cnpj] = {
                "maxQuota":     quotas[max_idx],
                "maxQuotaDate": dates[max_idx] if max_idx < len(dates) else "",
            }
        print(f"  Reconstruídos {len(result)} maxQuotas do history.json (fallback)")
        return result
    except Exception as e:
        print(f"  Não foi possível reconstruir maxQuotas: {e}")
        return {}


# ── Main ───────────────────────────────────────────────────────────────────────


def fetch_sp500(anchor: datetime.date, a12: datetime.date, a36: datetime.date, a60: datetime.date) -> dict:
    """Busca S&P 500 (^GSPC) e câmbio USD/BRL (BRL=X) no Yahoo Finance.
    Retorna CAGRs em BRL: converte preços do índice pela taxa de câmbio de cada data.
    """
    def _yahoo(ticker, period1, period2):
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
               f"?interval=1d&period1={period1}&period2={period2}")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        result     = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes     = result["indicators"]["quote"][0]["close"]
        return {
            datetime.datetime.utcfromtimestamp(ts).date().isoformat(): price
            for ts, price in zip(timestamps, closes) if price is not None
        }

    period1 = int(datetime.datetime.combine(
        a60 - datetime.timedelta(days=10), datetime.time(),
        tzinfo=datetime.timezone.utc).timestamp())
    period2 = int(datetime.datetime.combine(
        anchor + datetime.timedelta(days=5), datetime.time(),
        tzinfo=datetime.timezone.utc).timestamp())

    try:
        sp_map  = _yahoo("%5EGSPC", period1, period2)
        fx_map  = _yahoo("BRL%3DX", period1, period2)  # USD/BRL

        # S&P em BRL = preço_SP500 × câmbio_USD/BRL
        def sp_brl(date_str):
            sp  = sp_map.get(date_str)
            fx  = fx_map.get(date_str)
            if sp and fx and fx > 0:
                return sp * fx
            # Fallback: busca data mais próxima disponível nos dois
            sp_dates = sorted(sp_map.keys())
            fx_dates = sorted(fx_map.keys())
            sp_cands = [d for d in sp_dates if d <= date_str]
            fx_cands = [d for d in fx_dates if d <= date_str]
            if not sp_cands or not fx_cands:
                return None
            sp_v = sp_map[sp_cands[-1]]
            fx_v = fx_map[fx_cands[-1]]
            return sp_v * fx_v if sp_v and fx_v and fx_v > 0 else None

        p_anchor = sp_brl(anchor.isoformat())
        p12      = sp_brl(a12.isoformat())
        p36      = sp_brl(a36.isoformat())
        p60      = sp_brl(a60.isoformat())

        def sp_cagr(p_s, p_e, d_s, d_e):
            if not p_s or not p_e: return None
            return cagr(p_s, p_e, years_apart(d_s, d_e))

        result_sp = {
            "cagr12": sp_cagr(p12,  p_anchor, a12.isoformat(),  anchor.isoformat()),
            "cagr36": sp_cagr(p36,  p_anchor, a36.isoformat(),  anchor.isoformat()),
            "cagr60": sp_cagr(p60,  p_anchor, a60.isoformat(),  anchor.isoformat()),
        }
        vals = {k: f"{v:.2f}%" if v is not None else "N/D" for k, v in result_sp.items()}
        print(f"  S&P500 BRL 12M={vals['cagr12']} 36M={vals['cagr36']} 60M={vals['cagr60']}")
        return result_sp
    except Exception as e:
        print(f"  ✗ S&P500 falhou: {e}")
        return {"cagr12": None, "cagr36": None, "cagr60": None}



def fetch_daily_index_returns(anchor: datetime.date, history_start_year: int) -> dict:
    """
    Fetches full daily return series for IBOV and S&P500 BRL from history_start_year to anchor.
    Used for beta regression against fund daily returns in history.json.
    Returns: {"ibov": {date: return}, "sp500_brl": {date: return}}
    """
    start = datetime.date(history_start_year, 1, 1) - datetime.timedelta(days=5)
    period1 = int(datetime.datetime.combine(start, datetime.time(), tzinfo=datetime.timezone.utc).timestamp())
    period2 = int(datetime.datetime.combine(anchor + datetime.timedelta(days=5), datetime.time(), tzinfo=datetime.timezone.utc).timestamp())

    def _yahoo_prices(ticker):
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
               f"?interval=1d&period1={period1}&period2={period2}")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        result = data["chart"]["result"][0]
        ts     = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        return {datetime.datetime.utcfromtimestamp(t).date().isoformat(): p
                for t, p in zip(ts, closes) if p is not None}

    def prices_to_returns(prices: dict) -> dict:
        dates = sorted(prices.keys())
        rets  = {}
        for i in range(1, len(dates)):
            d0, d1 = dates[i-1], dates[i]
            if prices[d0] and prices[d1] and prices[d0] > 0:
                rets[d1] = prices[d1] / prices[d0] - 1
        return rets

    try:
        ibov_px   = _yahoo_prices("%5EBVSP")
        sp_px     = _yahoo_prices("%5EGSPC")
        fx_px     = _yahoo_prices("BRL%3DX")   # USD/BRL

        ibov_rets = prices_to_returns(ibov_px)

        # S&P in BRL = SP_price * USD/BRL rate
        sp_brl_px = {}
        for d in sp_px:
            sp = sp_px[d]
            fx = fx_px.get(d)
            if fx is None:
                # fallback to nearest available FX
                fx_dates = sorted(fx_px.keys())
                cands = [x for x in fx_dates if x <= d]
                fx = fx_px[cands[-1]] if cands else None
            if sp and fx and fx > 0:
                sp_brl_px[d] = sp * fx
        sp_brl_rets = prices_to_returns(sp_brl_px)

        print(f"  Daily returns: IBOV {len(ibov_rets)}d, S&P BRL {len(sp_brl_rets)}d")
        return {"ibov": ibov_rets, "sp500_brl": sp_brl_rets}
    except Exception as e:
        print(f"  ✗ daily index returns failed: {e}")
        return {"ibov": {}, "sp500_brl": {}}


def compute_fund_betas(history_path: Path, index_rets: dict) -> dict:
    """
    OLS regression: R_fund = alpha + beta_ibov * R_ibov + beta_sp500 * R_sp500_brl + epsilon
    Uses fund daily returns from history.json aligned with index returns.
    Returns per-fund beta dict saved into data.json.
    """
    if not history_path.exists() or not index_rets["ibov"]:
        return {}

    try:
        hist = json.loads(history_path.read_text())
    except Exception:
        return {}

    ibov_r   = index_rets["ibov"]
    sp500_r  = index_rets["sp500_brl"]
    results  = {}

    for cnpj, fd in hist.get("funds", {}).items():
        dates   = fd.get("dates", [])
        returns = fd.get("returns", [])
        if len(dates) < 120 or len(returns) < 120:
            continue

        # Align fund returns with index returns
        # returns[i] corresponds to dates[i] (return FROM dates[i-1] TO dates[i])
        X_ibov, X_sp, Y = [], [], []
        for i in range(1, len(dates)):
            d    = dates[i]
            r_f  = returns[i - 1]
            r_i  = ibov_r.get(d)
            r_s  = sp500_r.get(d)
            if r_i is None or r_s is None:
                continue
            if r_f is None:
                continue  # skip pre-inception (fund not yet active)
            X_ibov.append(r_i)
            X_sp.append(r_s)
            Y.append(r_f)

        n = len(Y)
        if n < 60:
            results[cnpj] = {"beta_ibov": None, "beta_sp500": None, "alpha": None, "r2": None, "n": n}
            continue

        # OLS with two factors: Y = a + b1*X1 + b2*X2
        # Normal equations: [X'X] [b] = [X'Y]
        # X matrix: [1, X_ibov, X_sp500]
        n_f = float(n)
        s1  = sum(X_ibov)
        s2  = sum(X_sp)
        sy  = sum(Y)
        s11 = sum(x*x for x in X_ibov)
        s22 = sum(x*x for x in X_sp)
        s12 = sum(X_ibov[i]*X_sp[i] for i in range(n))
        s1y = sum(X_ibov[i]*Y[i] for i in range(n))
        s2y = sum(X_sp[i]*Y[i] for i in range(n))

        # 3x3 system via Cramer / direct solve
        # [n,   s1,  s2 ] [a ]   [sy ]
        # [s1,  s11, s12] [b1] = [s1y]
        # [s2,  s12, s22] [b2]   [s2y]
        A = [[n_f, s1,  s2 ],
             [s1,  s11, s12],
             [s2,  s12, s22]]
        b = [sy, s1y, s2y]

        # Gaussian elimination
        import copy
        M = [row[:] + [b[i]] for i, row in enumerate(A)]
        for col in range(3):
            pivot = max(range(col, 3), key=lambda r: abs(M[r][col]))
            M[col], M[pivot] = M[pivot], M[col]
            if abs(M[col][col]) < 1e-12:
                break
            for row in range(col+1, 3):
                f = M[row][col] / M[col][col]
                M[row] = [M[row][j] - f*M[col][j] for j in range(4)]
        # Back substitution
        sol = [0.0]*3
        for row in range(2, -1, -1):
            sol[row] = (M[row][3] - sum(M[row][j]*sol[j] for j in range(row+1, 3))) / (M[row][row] if abs(M[row][row]) > 1e-12 else 1e-12)

        alpha_d, b_ibov, b_sp = sol
        alpha_ann = (math.pow(1 + alpha_d, 252) - 1) * 100

        # R-squared
        y_mean = sy / n_f
        ss_tot = sum((y - y_mean)**2 for y in Y)
        y_hat  = [alpha_d + b_ibov*X_ibov[i] + b_sp*X_sp[i] for i in range(n)]
        ss_res = sum((Y[i] - y_hat[i])**2 for i in range(n))
        r2     = 1 - ss_res/ss_tot if ss_tot > 0 else 0

        results[cnpj] = {
            "beta_ibov":  round(b_ibov, 4),
            "beta_sp500": round(b_sp,   4),
            "alpha_ann":  round(alpha_ann, 2),
            "r2":         round(r2, 4),
            "n_obs":      n,
        }
        print(f"  {cnpj[-14:]}: β_ibov={b_ibov:.3f} β_sp={b_sp:.3f} α={alpha_ann:.1f}% R²={r2:.3f} n={n}")

    return results

def main() -> None:
    today = datetime.date.today()
    print(f"Executando para {today.isoformat()}")

    anchor = find_anchor_date(today.year, today.month)
    a12    = subtract_months(anchor, 12)
    a36    = subtract_months(anchor, 36)
    a60    = subtract_months(anchor, 60)

    print(f"Janelas: 12M={a12} 36M={a36} 60M={a60} → {anchor}")

    out_path  = Path(__file__).parent.parent / "docs" / "data.json"
    hist_path = Path(__file__).parent.parent / "docs" / "history.json"

    # Sempre usa history.json como fonte primária para maxQuota —
    # é o único lugar com o histórico completo de cotas.
    # data.json anterior é usado apenas para fundos ausentes do history.
    prev_max_quotas = reconstruct_max_quotas_from_history(hist_path)

    if out_path.exists():
        try:
            prev = json.loads(out_path.read_text())
            for f in prev.get("funds", []):
                cnpj = f.get("cnpjFmt")
                if cnpj and cnpj not in prev_max_quotas and f.get("maxQuota"):
                    prev_max_quotas[cnpj] = {
                        "maxQuota":     f["maxQuota"],
                        "maxQuotaDate": f.get("maxQuotaDate", ""),
                    }
            print(f"Carregados {len(prev_max_quotas)} maxQuotas (history + data.json)")
        except Exception as e:
            print(f"Não foi possível ler data.json anterior: {e}")

    results = [process_fund(f, anchor, prev_max_quotas) for f in FUNDS]

    delayed = [r for r in results if not r.get("error") and r.get("isDelayed")]
    if delayed:
        print(f"\n⚠ Fundos atrasados em relação à âncora ({anchor}):")
        for r in delayed:
            print(f"  {r['name']}: última cota {r['latestDate']} ({r['delayDays']}d)")

    print(f"\n── Ibovespa")
    ibov = fetch_ibov(anchor, a12, a36, a60)

    print(f"\n── CDI")
    cdi = fetch_cdi(anchor, a12, a36, a60)

    print(f"\n── S&P 500")
    sp500 = fetch_sp500(anchor, a12, a36, a60)

    print(f"\n── Betas (regressão OLS vs IBOV e S&P BRL)")
    index_rets = fetch_daily_index_returns(anchor, HISTORY_START_YEAR)
    fund_betas = compute_fund_betas(hist_path, index_rets)
    print(f"  Betas calculados: {len(fund_betas)} fundos")

    data_out = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "anchorDate":  anchor.isoformat(),
        "ibov":        ibov,
        "cdi":         cdi,
        "sp500":       sp500,
        "fund_betas":  fund_betas,
        "funds":       results,
    }

    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(data_out, ensure_ascii=False, indent=2))
    print(f"\n✓ data.json escrito ({len(results)} fundos)")

    update_history(anchor)


if __name__ == "__main__":
    main()
