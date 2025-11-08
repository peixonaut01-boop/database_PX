"""
Script to fetch IBGE Table 8757 - PMC materiais de construção (receita e volume).
"""
from pmc_base import SubBranchConfig, upload_simple_table

TABLE_NUMBER = 8757
PERIOD_RANGE = 'janeiro 2003 a agosto 2025'

SUBBRANCHES = {
    'receita': SubBranchConfig(
        name='receita',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8757.xlsx&terr=N&rank=-&query=t/8757/n1/all/v/all/p/all/c11046/56731/d/v7169%205,v7170%205,v11708%201,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índice e variação da receita nominal e do volume de vendas de materiais de construção (2022 = 100) - Receita (nº8757)',
    ),
    'volume': SubBranchConfig(
        name='volume',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8757.xlsx&terr=N&rank=-&query=t/8757/n1/all/v/all/p/all/c11046/56732/d/v7169%205,v7170%205,v11708%201,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índice e variação do volume de vendas de materiais de construção (2022 = 100) - Volume (nº8757)',
    ),
}


def fetch_and_upload_ibge_data() -> None:
    upload_simple_table(TABLE_NUMBER, PERIOD_RANGE, SUBBRANCHES)


if __name__ == '__main__':
    fetch_and_upload_ibge_data()
