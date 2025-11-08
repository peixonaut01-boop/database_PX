"""
Script to fetch IBGE Table 8884 - PMC veículos, motocicletas, partes e peças (receita e volume).
"""
from pmc_base import SubBranchConfig, upload_simple_table

TABLE_NUMBER = 8884
PERIOD_RANGE = 'janeiro 2000 a agosto 2025'

SUBBRANCHES = {
    'receita': SubBranchConfig(
        name='receita',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8884.xlsx&terr=N&rank=-&query=t/8884/n1/all/v/all/p/all/c11046/56737/d/v7169%205,v7170%205,v11708%201,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índice e variação da receita nominal e do volume de vendas de veículos, motocicletas, partes e peças (2022 = 100) - Receita (nº8884)',
    ),
    'volume': SubBranchConfig(
        name='volume',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8884.xlsx&terr=N&rank=-&query=t/8884/n1/all/v/all/p/all/c11046/56738/d/v7169%205,v7170%205,v11708%201,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bp',
        table_name='PMC - Índice e variação do volume de vendas de veículos, motocicletas, partes e peças (2022 = 100) - Volume (nº8884)',
    ),
}


def fetch_and_upload_ibge_data() -> None:
    upload_simple_table(TABLE_NUMBER, PERIOD_RANGE, SUBBRANCHES)


if __name__ == '__main__':
    fetch_and_upload_ibge_data()
