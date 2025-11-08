"""
Script to fetch IBGE Table 8190 - PMC atacado especializado (receita e volume).
"""
from pmc_base import SubBranchConfig, upload_simple_table

TABLE_NUMBER = 8190
PERIOD_RANGE = 'janeiro 2022 a agosto 2025'

SUBBRANCHES = {
    'receita': SubBranchConfig(
        name='receita',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8190.xlsx&terr=N&rank=-&query=t/8190/n1/all/v/all/p/all/c11046/56739/d/v7169%205,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índices de receita nominal de atacado especializado em produtos alimentícios, bebidas e fumo (2022 = 100) - Receita (nº8190)',
    ),
    'volume': SubBranchConfig(
        name='volume',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8190.xlsx&terr=N&rank=-&query=t/8190/n1/all/v/all/p/all/c11046/56740/d/v7169%205,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índices de volume de atacado especializado em produtos alimentícios, bebidas e fumo (2022 = 100) - Volume (nº8190)',
    ),
}


def fetch_and_upload_ibge_data() -> None:
    upload_simple_table(TABLE_NUMBER, PERIOD_RANGE, SUBBRANCHES)


if __name__ == '__main__':
    fetch_and_upload_ibge_data()
