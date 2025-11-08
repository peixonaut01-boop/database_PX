"""
Script to fetch IBGE Table 8882 - PMC comércio varejista por atividades (receita e volume).
"""
from pmc_base import SubBranchConfig, upload_activity_table

TABLE_NUMBER = 8882
PERIOD_RANGE = 'janeiro 2000 a agosto 2025'

SUBBRANCHES = {
    'receita': SubBranchConfig(
        name='receita',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8882.xlsx&terr=N&rank=-&query=t/8882/n1/all/v/all/p/all/c11046/56733/c85/all/d/v7169%205,v7170%205,v11708%201,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bc85%2Bp',
        table_name='PMC - Índice e variação da receita nominal e do volume de vendas no comércio varejista, por atividades (2022 = 100) - Receita (nº8882)',
    ),
    'volume': SubBranchConfig(
        name='volume',
        url='https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8882.xlsx&terr=N&rank=-&query=t/8882/n1/all/v/all/p/all/c11046/56734/c85/all/d/v7169%205,v7170%205,v11708%201,v11709%201,v11710%201,v11711%201/l/v,c11046,t%2Bc85%2Bp',
        table_name='PMC - Índice e variação do volume de vendas no comércio varejista, por atividades (2022 = 100) - Volume (nº8882)',
    ),
}


def fetch_and_upload_ibge_data() -> None:
    upload_activity_table(TABLE_NUMBER, PERIOD_RANGE, SUBBRANCHES)


if __name__ == '__main__':
    fetch_and_upload_ibge_data()
