"""Interactive/CLI helper to run IBGE update jobs or adjust workflow schedules."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent


class TableInfo(Dict[str, str]):
    """Typed alias for per-table metadata."""


CategoryKey = str


TABLE_REGISTRY: Dict[CategoryKey, Dict[str, object]] = {
    'cnt': {
        'label': 'CNT (Quarterly National Accounts)',
        'workflow': REPO_ROOT / '.github/workflows/update_ibge_cnt.yaml',
        'run_all': 'run_all_tables.py',
        'verify': 'verify_structure.py',
        'tables': {
            '1620': TableInfo(
                script='ibge_CNT.py',
                description='Table 1620 - Chained quarterly volume index (1995=100)',
            ),
            '1621': TableInfo(
                script='ibge_1621.py',
                description='Table 1621 - Chained quarterly volume index with seasonal adjustment',
            ),
            '1846': TableInfo(
                script='ibge_1846.py',
                description='Table 1846 - Current price values',
            ),
            '2072': TableInfo(
                script='ibge_2072.py',
                description='Table 2072 - Quarterly economic accounts (multi-sheet)',
            ),
            '5932': TableInfo(
                script='ibge_5932.py',
                description='Table 5932 - Quarterly volume change rates',
            ),
            '6612': TableInfo(
                script='ibge_6612.py',
                description='Table 6612 - Chained values at 1995 prices',
            ),
            '6613': TableInfo(
                script='ibge_6613.py',
                description='Table 6613 - Chained 1995-price values with seasonal adjustment',
            ),
            '6726': TableInfo(
                script='ibge_6726.py',
                description='Table 6726 - Savings rate',
            ),
            '6727': TableInfo(
                script='ibge_6727.py',
                description='Table 6727 - Investment rate',
            ),
        },
    },
    'pms': {
        'label': 'PMS (Monthly Services Survey)',
        'workflow': REPO_ROOT / '.github/workflows/update_ibge_pms.yaml',
        'run_all': 'run_all_pms.py',
        'verify': 'verify_pms_structure.py',
        'tables': {
            '5906_receita': TableInfo(
                script='ibge_5906_receita.py',
                description='Table 5906 (revenue) - Index and variation (2022=100)',
            ),
            '5906_volume': TableInfo(
                script='ibge_5906_volume.py',
                description='Table 5906 (volume) - Index and variation (2022=100)',
            ),
            '8163': TableInfo(
                script='ibge_8163.py',
                description='Table 8163 - Revenue and volume by service segments',
            ),
            '8688': TableInfo(
                script='ibge_8688.py',
                description='Table 8688 - Revenue and volume by service activities',
            ),
        },
    },
    'pmc': {
        'label': 'PMC (Monthly Trade Survey)',
        'workflow': REPO_ROOT / '.github/workflows/update_ibge_pmc.yaml',
        'run_all': 'run_all_pmc.py',
        'verify': 'verify_structure.py',
        'tables': {
            '8190': TableInfo(
                script='ibge_8190.py',
                description='Table 8190 - Wholesale of food/beverages/tobacco (revenue & volume)',
            ),
            '8757': TableInfo(
                script='ibge_8757.py',
                description='Table 8757 - Building materials retail (revenue & volume)',
            ),
            '8880': TableInfo(
                script='ibge_8880.py',
                description='Table 8880 - Core retail trade (revenue & volume)',
            ),
            '8881': TableInfo(
                script='ibge_8881.py',
                description='Table 8881 - Extended retail trade (revenue & volume)',
            ),
            '8882': TableInfo(
                script='ibge_8882.py',
                description='Table 8882 - Retail by activities (revenue & volume)',
            ),
            '8883': TableInfo(
                script='ibge_8883.py',
                description='Table 8883 - Extended retail by activities (revenue & volume)',
            ),
            '8884': TableInfo(
                script='ibge_8884.py',
                description='Table 8884 - Vehicles, motorcycles, parts & pieces (revenue & volume)',
            ),
        },
    },
}


def print_categories() -> None:
    print("\nAvailable categories:")
    for key, info in TABLE_REGISTRY.items():
        print(f"  - {key}: {info['label']}")


def print_tables(category: CategoryKey) -> None:
    info = TABLE_REGISTRY[category]
    print(f"\nTables for {info['label']}:")
    tables: Dict[str, TableInfo] = info['tables']  # type: ignore[assignment]
    for table_key, meta in tables.items():
        print(f"  - {table_key}: {meta['description']} ({meta['script']})")


def execute_script(script_name: str) -> bool:
    script_path = SCRIPT_DIR / script_name
    if not script_path.exists():
        print(f"[ERROR] Missing script: {script_name}")
        return False

    print(f"\n{'=' * 80}")
    print(f"Running {script_name}")
    print(f"{'=' * 80}")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(SCRIPT_DIR),
        check=False,
    )
    if result.returncode == 0:
        print(f"[SUCCESS] {script_name}")
        return True

    print(f"[FAIL] {script_name} exited with code {result.returncode}")
    return False


def run_category(category: CategoryKey, tables: Optional[Sequence[str]] = None, verify: bool = False) -> None:
    info = TABLE_REGISTRY[category]
    table_map: Dict[str, TableInfo] = info['tables']  # type: ignore[assignment]

    scripts_to_run: List[str]
    if tables:
        scripts_to_run = []
        for table_key in tables:
            key = table_key.strip()
            if key not in table_map:
                print(f"[WARN] Unknown table for {category}: {table_key}")
                continue
            scripts_to_run.append(table_map[key]['script'])
        if not scripts_to_run:
            print("[INFO] No valid script selected.")
            return
    else:
        run_all_script = info.get('run_all')
        if run_all_script:
            scripts_to_run = [str(run_all_script)]
        else:
            scripts_to_run = [table_map[key]['script'] for key in table_map]

    results = [execute_script(script) for script in scripts_to_run]
    success = sum(1 for ok in results if ok)
    failure = len(results) - success
    print("\n" + "=" * 80)
    print(f"Summary: {success} succeeded, {failure} failed")
    print("=" * 80)

    if verify and info.get('verify'):
        verify_script = str(info['verify'])
        print("\nRunning post-update verification...")
        execute_script(verify_script)


def update_schedule(category: CategoryKey, cron: Optional[str]) -> None:
    info = TABLE_REGISTRY[category]
    workflow_path: Path = info['workflow']  # type: ignore[assignment]
    data = yaml.safe_load(workflow_path.read_text(encoding='utf-8')) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Formato inesperado em {workflow_path}")

    triggers = data.get('on') or {}
    if not isinstance(triggers, dict):
        triggers = {}

    triggers['workflow_dispatch'] = triggers.get('workflow_dispatch') or {}

    if cron:
        triggers['schedule'] = [{'cron': cron}]
        print(f"[INFO] Schedule set for {category}: {cron}")
    else:
        if 'schedule' in triggers:
            triggers.pop('schedule')
            print(f"[INFO] Schedule removed for {category}")
        else:
            print(f"[INFO] No schedule to remove for {category}")

    data['on'] = triggers
    workflow_path.write_text(
        yaml.safe_dump(data, sort_keys=False, default_flow_style=False),
        encoding='utf-8',
    )


def interactive_menu() -> None:
    print("=== IBGE Database Update Console ===")
    while True:
        print(
            "\nChoose an option:\n"
            " 1) List categories\n"
            " 2) List tables in a category\n"
            " 3) Run update (entire category)\n"
            " 4) Run update (specific tables)\n"
            " 5) Set or remove workflow schedule\n"
            " 6) Exit\n"
        )
        choice = input("Option: ").strip()

        if choice == '1':
            print_categories()
        elif choice == '2':
            key = input("Category (cnt/pms/pmc): ").strip().lower()
            if key in TABLE_REGISTRY:
                print_tables(key)
            else:
                print("[ERROR] Invalid category.")
        elif choice == '3':
            key = input("Category to run (cnt/pms/pmc): ").strip().lower()
            if key in TABLE_REGISTRY:
                run_category(key, verify=True)
            else:
                print("[ERROR] Invalid category.")
        elif choice == '4':
            key = input("Category (cnt/pms/pmc): ").strip().lower()
            if key not in TABLE_REGISTRY:
                print("[ERROR] Invalid category.")
                continue
            print_tables(key)
            table_input = input("Table IDs (comma separated): ").strip()
            selected = [item.strip() for item in table_input.split(',') if item.strip()]
            run_category(key, tables=selected, verify=True)
        elif choice == '5':
            key = input("Workflow category (cnt/pms/pmc): ").strip().lower()
            if key not in TABLE_REGISTRY:
                print("[ERROR] Invalid category.")
                continue
            cron_value = input("Cron expression (GitHub format) or leave blank to remove: ").strip()
            update_schedule(key, cron_value or None)
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("[ERROR] Invalid option.")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ferramenta para atualizar tabelas IBGE manualmente ou ajustar cron."
    )
    subparsers = parser.add_subparsers(dest='command')

    subparsers.add_parser('list', help='Lista as categorias disponíveis.')

    list_tables_parser = subparsers.add_parser('list-tables', help='Lista tabelas de uma categoria.')
    list_tables_parser.add_argument('category', choices=TABLE_REGISTRY.keys())

    run_parser = subparsers.add_parser('run', help='Executa atualizações.')
    run_parser.add_argument('category', choices=TABLE_REGISTRY.keys())
    run_parser.add_argument(
        '--table',
        action='append',
        dest='tables',
        help='ID de tabela para executar (use múltiplas vezes). Ausente = categoria inteira.',
    )
    run_parser.add_argument('--no-verify', action='store_true', help='Não executa verificação após atualização.')

    sched_parser = subparsers.add_parser('schedule', help='Define ou remove cron de um workflow.')
    sched_parser.add_argument('category', choices=TABLE_REGISTRY.keys())
    sched_parser.add_argument(
        '--cron',
        help="Expressão cron GitHub Actions. Omitir para remover agenda.",
    )

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    if not args.command:
        interactive_menu()
        return

    if args.command == 'list':
        print_categories()
    elif args.command == 'list-tables':
        print_tables(args.category)
    elif args.command == 'run':
        run_category(args.category, tables=args.tables, verify=not args.no_verify)
    elif args.command == 'schedule':
        update_schedule(args.category, args.cron)
    else:
        raise ValueError(f"Comando desconhecido: {args.command}")


if __name__ == '__main__':
    main()

