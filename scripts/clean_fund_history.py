#!/usr/bin/env python3
"""
clean_fund_history.py — Remove um ou mais fundos do history.json para forçar
backfill completo com dados corretos no próximo workflow.

Uso:
    python scripts/clean_fund_history.py 29.726.133/0001-21
    python scripts/clean_fund_history.py 29.726.133/0001-21 35.828.684/0001-07
"""
import sys, json
from pathlib import Path

HIST_PATH = Path(__file__).parent.parent / "docs" / "history.json"

def main():
    if len(sys.argv) < 2:
        print("Uso: python scripts/clean_fund_history.py <cnpj> [cnpj2 ...]")
        sys.exit(1)

    cnpjs_to_remove = set(sys.argv[1:])
    print(f"Removendo {len(cnpjs_to_remove)} fundo(s): {cnpjs_to_remove}")

    if not HIST_PATH.exists():
        print("history.json não encontrado")
        sys.exit(1)

    hist = json.loads(HIST_PATH.read_text())

    # Remove de funds
    removed_funds = []
    for cnpj in cnpjs_to_remove:
        if cnpj in hist.get("funds", {}):
            del hist["funds"][cnpj]
            removed_funds.append(cnpj)
            print(f"  ✓ removido de funds: {cnpj}")
        else:
            print(f"  ✗ não encontrado em funds: {cnpj}")

    # Remove de correlation
    for cnpj in cnpjs_to_remove:
        if cnpj in hist.get("correlation", {}):
            del hist["correlation"][cnpj]
        for other in hist.get("correlation", {}).values():
            other.pop(cnpj, None)

    if removed_funds:
        HIST_PATH.write_text(json.dumps(hist, ensure_ascii=False, separators=(",", ":")))
        print(f"\n✓ history.json atualizado — {len(removed_funds)} fundo(s) removido(s)")
        print("  Execute o workflow de update para regenerar o histórico completo.")
    else:
        print("Nenhum fundo removido.")

if __name__ == "__main__":
    main()
