"""
Ingestion helper for IBGE PNAD Contínua (PNADCT) tables.

This script fetches a curated list of PNADCT tables, cleans their content,
and uploads the results to Firebase while preserving multi-sheet structures.
"""

import argparse
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from ibge_base import (
    clean_and_structure_data,
    fetch_all_sheets,
    upload_multiple_sheets_to_firebase,
    upload_table_data,
)

# --- Configuration ---------------------------------------------------------

CATEGORY = "pnadct"

# Table metadata: table number -> configuration
PNADCT_TABLES: Dict[int, Dict[str, object]] = {
    1616: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, desocupadas na semana de referência, "
            "por tempo de procura de trabalho (nº1616)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela1616.xlsx&terr=N&rank=-"
            "&query=t/1616/n1/all/v/all/p/all/c1965/all/d/v4093%201,v4110%201,v4111%201/l/v,c1965,t%2Bp"
        ),
        "multi_sheet": True,
    },
    4092: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, por condição em relação à força de trabalho "
            "e condição de ocupação (nº4092)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4092.xlsx&terr=N&rank=-"
            "&query=t/4092/n1/all/v/all/p/all/c629/all/d/v4087%201,v4104%201,v4105%201/l/v,c629,t%2Bp"
        ),
        "multi_sheet": True,
    },
    4093: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, total, na força de trabalho, ocupadas, "
            "desocupadas, fora da força de trabalho, em situação de informalidade e respectivas "
            "taxas e níveis, por sexo (nº4093)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4093.xlsx&terr=N&rank=-"
            "&query=t/4093/n1/all/v/1641/p/all/c2/all/l/v,c2,t%2Bp"
        ),
    },
    4094: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, total, na força de trabalho, ocupadas, "
            "desocupadas, fora da força de trabalho, em situação de informalidade e respectivas "
            "taxas e níveis, por grupo de idade (nº4094)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4094.xlsx&terr=N&rank=-"
            "&query=t/4094/n1/all/v/1641/p/all/c58/all/l/v,c58,t%2Bp"
        ),
    },
    4095: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, total, na força de trabalho, ocupadas, "
            "desocupadas, fora da força de trabalho, em situação de informalidade e respectivas "
            "taxas e níveis, por nível de instrução (nº4095)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4095.xlsx&terr=N&rank=-"
            "&query=t/4095/n1/all/v/1641/p/all/c1568/all/l/v,c1568,t%2Bp"
        ),
    },
    4096: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência, por posição na "
            "ocupação no trabalho principal (nº4096)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4096.xlsx&terr=N&rank=-"
            "&query=t/4096/n1/all/v/4090/p/all/c12029/all/l/v,c12029,t%2Bp"
        ),
    },
    4097: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência, por posição na "
            "ocupação e categoria do emprego no trabalho principal (nº4097)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4097.xlsx&terr=N&rank=-"
            "&query=t/4097/n1/all/v/4090/p/all/c11913/all/l/v,c11913,t%2Bp"
        ),
    },
    4099: {
        "name": (
            "Taxas de desocupação e de subutilização da força de trabalho, na semana de referência, "
            "das pessoas de 14 anos ou mais de idade (nº4099)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4099.xlsx&terr=N&rank=-"
            "&query=t/4099/n1/all/v/4099/p/all/d/v4099%201/l/v,,t%2Bp"
        ),
    },
    4100: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, por tipo de medida de subutilização da força de "
            "trabalho na semana de referência (nº4100)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela4100.xlsx&terr=N&rank=-"
            "&query=t/4100/n1/all/v/1641/p/all/c604/all/l/v,c604,t%2Bp"
        ),
    },
    5434: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência, por grupamento "
            "de atividade no trabalho principal (Vide Notas) (nº5434)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5434.xlsx&terr=N&rank=-"
            "&query=t/5434/n1/all/v/4090/p/all/c888/all/l/v,c888,t%2Bp"
        ),
    },
    5435: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência, por grupamento "
            "ocupacional no trabalho principal (Vide Notas) (nº5435)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5435.xlsx&terr=N&rank=-"
            "&query=t/5435/n1/all/v/4090/p/all/c694/all/l/v,c694,t%2Bp"
        ),
    },
    5436: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos no trabalho principal e em todos os trabalhos, por sexo (Vide Notas) (nº5436)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5436.xlsx&terr=N&rank=-"
            "&query=t/5436/n1/all/v/5932/p/all/c2/all/l/v,c2,t%2Bp"
        ),
    },
    5437: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos no trabalho principal e em todos os trabalhos, por grupo de idade "
            "(Vide Notas) (nº5437)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5437.xlsx&terr=N&rank=-"
            "&query=t/5437/n1/all/v/5932/p/all/c58/all/l/v,c58,t%2Bp"
        ),
    },
    5438: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos no trabalho principal e em todos os trabalhos, por nível de instrução "
            "(Vide Notas) (nº5438)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5438.xlsx&terr=N&rank=-"
            "&query=t/5438/n1/all/v/5932/p/all/c1568/all/l/v,c1568,t%2Bp"
        ),
    },
    5439: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos no trabalho principal, por posição na ocupação no trabalho principal "
            "(Vide Notas) (nº5439)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5439.xlsx&terr=N&rank=-"
            "&query=t/5439/n1/all/v/5932/p/all/c12029/all/l/v,c12029,t%2Bp"
        ),
    },
    5440: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos no trabalho principal, por posição na ocupação e categoria do emprego "
            "no trabalho principal (Vide Notas) (nº5440)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5440.xlsx&terr=N&rank=-"
            "&query=t/5440/n1/all/v/5932/p/all/c11913/all/l/v,c11913,t%2Bp"
        ),
    },
    5442: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos no trabalho principal, por grupamento de atividade no trabalho principal "
            "(Vide Notas) (nº5442)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5442.xlsx&terr=N&rank=-"
            "&query=t/5442/n1/all/v/5932/p/all/c888/all/l/v,c888,t%2Bp"
        ),
    },
    5444: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos no trabalho principal, por grupamento ocupacional no trabalho principal "
            "(Vide Notas) (nº5444)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5444.xlsx&terr=N&rank=-"
            "&query=t/5444/n1/all/v/5932/p/all/c694/all/l/v,c694,t%2Bp"
        ),
    },
    5606: {
        "name": (
            "Massa de rendimento mensal real das pessoas de 14 anos ou mais de idade ocupadas "
            "na semana de referência com rendimento de trabalho, habitualmente e efetivamente "
            "recebidos em todos os trabalhos (Vide Notas) (nº5606)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5606.xlsx&terr=N&rank=-"
            "&query=t/5606/n1/all/v/6293/p/all/l/v,,t%2Bp"
        ),
    },
    5917: {
        "name": "População, por sexo (Vide Notas) (nº5917)",
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5917.xlsx&terr=N&rank=-"
            "&query=t/5917/n1/all/v/606/p/all/c2/all/l/v,c2,t%2Bp"
        ),
    },
    5918: {
        "name": "População, por grupo de idade (Vide Notas) (nº5918)",
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5918.xlsx&terr=N&rank=-"
            "&query=t/5918/n1/all/v/606/p/all/c58/all/l/v,c58,t%2Bp"
        ),
    },
    5919: {
        "name": "População, por nível de instrução (Vide Notas) (nº5919)",
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5919.xlsx&terr=N&rank=-"
            "&query=t/5919/n1/all/v/606/p/all/c1568/all/l/v,c1568,t%2Bp"
        ),
    },
    5947: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência, por contribuição "
            "para instituto de previdência em qualquer trabalho (Vide Notas) (nº5947)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela5947.xlsx&terr=N&rank=-"
            "&query=t/5947/n1/all/v/4090/p/all/c12027/all/l/v,c12027,t%2Bp"
        ),
    },
    6371: {
        "name": (
            "Média de horas habitualmente trabalhadas por semana e efetivamente trabalhadas na "
            "semana de referência, no trabalho principal e em todos os trabalhos, das pessoas de "
            "14 anos ou mais de idade, por sexo (Vide Notas) (nº6371)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6371.xlsx&terr=N&rank=-"
            "&query=t/6371/n1/all/v/8186/p/all/c2/all/d/v8186%201/l/v,c2,t%2Bp"
        ),
    },
    6372: {
        "name": (
            "Média de horas habitualmente trabalhadas por semana e efetivamente trabalhadas na "
            "semana de referência, no trabalho principal e em todos os trabalhos, das pessoas de "
            "14 anos ou mais de idade, por grupo de idade (Vide Notas) (nº6372)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6372.xlsx&terr=N&rank=-"
            "&query=t/6372/n1/all/v/8186/p/all/c58/all/d/v8186%201/l/v,c58,t%2Bp"
        ),
    },
    6373: {
        "name": (
            "Média de horas habitualmente trabalhadas por semana e efetivamente trabalhadas na "
            "semana de referência, no trabalho principal e em todos os trabalhos, das pessoas de "
            "14 anos ou mais de idade, por nível de instrução (Vide Notas) (nº6373)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6373.xlsx&terr=N&rank=-"
            "&query=t/6373/n1/all/v/8186/p/all/c1568/all/d/v8186%201/l/v,c1568,t%2Bp"
        ),
    },
    6374: {
        "name": (
            "Média de horas habitualmente trabalhadas por semana e efetivamente trabalhadas na "
            "semana de referência, no trabalho principal, das pessoas de 14 anos ou mais de idade, "
            "por posição na ocupação (Vide Notas) (nº6374)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6374.xlsx&terr=N&rank=-"
            "&query=t/6374/n1/all/v/8186/p/all/c2399/all/d/v8186%201/l/v,c2399,t%2Bp"
        ),
    },
    6382: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência como militares "
            "ou empregados do setor público no trabalho principal, por área do emprego (Vide Notas) "
            "(nº6382)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6382.xlsx&terr=N&rank=-"
            "&query=t/6382/n1/all/v/8332/p/all/c11381/all/l/v,c11381,t%2Bp"
        ),
    },
    6383: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência como trabalhadores "
            "domésticos no trabalho principal, por número de domicílios em que trabalhavam (Vide Notas) "
            "(nº6383)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6383.xlsx&terr=N&rank=-"
            "&query=t/6383/n1/all/v/8336/p/all/c785/40276/l/v,c785,t%2Bp"
        ),
    },
    6385: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência, por tempo de "
            "permanência no trabalho principal (Vide Notas) (nº6385)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6385.xlsx&terr=N&rank=-"
            "&query=t/6385/n1/all/v/4090/p/all/c12043/all/l/v,c12043,t%2Bp"
        ),
    },
    6386: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência, por número de "
            "trabalhos (Vide Notas) (nº6386)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6386.xlsx&terr=N&rank=-"
            "&query=t/6386/n1/all/v/4090/p/all/c12031/all/l/v,c12031,t%2Bp"
        ),
    },
    6396: {
        "name": (
            "Taxas de desocupação e de subutilização da força de trabalho, na semana de referência, "
            "das pessoas de 14 anos ou mais de idade, por sexo (Vide Notas) (nº6396)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6396.xlsx&terr=N&rank=-"
            "&query=t/6396/n1/all/v/4099/p/all/c2/all/d/v4099%201/l/,p%2Bc2,t%2Bv"
        ),
    },
    6397: {
        "name": (
            "Taxas de desocupação e de subutilização da força de trabalho, na semana de referência, "
            "das pessoas de 14 anos ou mais de idade, por grupo de idade (Vide Notas) (nº6397)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6397.xlsx&terr=N&rank=-"
            "&query=t/6397/n1/all/v/4099/p/all/c58/all/d/v4099%201/l/v,c58,t%2Bp"
        ),
    },
    6398: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, por tipo de medida de subutilização da força de "
            "trabalho na semana de referência e sexo (Vide Notas) (nº6398)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6398.xlsx&terr=N&rank=-"
            "&query=t/6398/n1/all/v/4092/p/all/c2/all/l/v,c2,t%2Bp"
        ),
    },
    6399: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, por tipo de medida de subutilização da força de "
            "trabalho na semana de referência e grupo de idade (Vide Notas) (nº6399)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6399.xlsx&terr=N&rank=-"
            "&query=t/6399/n1/all/v/4092/p/all/c58/all/l/v,c58,t%2Bp"
        ),
    },
    6402: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, total, na força de trabalho, ocupadas, desocupadas, "
            "fora da força de trabalho, em situação de informalidade e respectivas taxas e níveis, por "
            "cor ou raça (Vide Notas) (nº6402)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6402.xlsx&terr=N&rank=-"
            "&query=t/6402/n1/all/v/1641/p/all/c86/all/l/v,c86,t%2Bp"
        ),
    },
    6403: {
        "name": "População, por cor ou raça (Vide Notas) (nº6403)",
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6403.xlsx&terr=N&rank=-"
            "&query=t/6403/n1/all/v/606/p/all/c86/all/l/v,c86,t%2Bp"
        ),
    },
    6405: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana "
            "de referência com rendimento de trabalho, habitualmente e efetivamente recebidos no "
            "trabalho principal e em todos os trabalhos, por cor ou raça (Vide Notas) (nº6405)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6405.xlsx&terr=N&rank=-"
            "&query=t/6405/n1/all/v/5932/p/all/c86/all/l/v,c86,t%2Bp"
        ),
    },
    6406: {
        "name": (
            "Média de horas habitualmente trabalhadas por semana e efetivamente trabalhadas na semana "
            "de referência, no trabalho principal e em todos os trabalhos, das pessoas de 14 anos ou "
            "mais de idade, por cor ou raça (Vide Notas) (nº6406)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6406.xlsx&terr=N&rank=-"
            "&query=t/6406/n1/all/v/8186/p/all/c86/all/d/v8186%201/l/v,c86,t%2Bp"
        ),
    },
    6421: {
        "name": (
            "Massa de rendimento mensal real das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente e efetivamente recebidos "
            "no trabalho principal, por posição na ocupação no trabalho principal (Vide Notas) (nº6421)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6421.xlsx&terr=N&rank=-"
            "&query=t/6421/n1/all/v/8745/p/all/c12029/all/l/v,c12029,t%2Bp"
        ),
    },
    6459: {
        "name": (
            "Pessoas de 14 anos ou mais de idade ocupadas na semana de referência - Total, coeficiente "
            "de variação, variações percentuais e absolutas em relação ao trimestre anterior e ao mesmo "
            "trimestre do ano anterior - por contribuição para instituto de previdência em qualquer trabalho "
            "(Vide Notas) (nº6459)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6459.xlsx&terr=N&rank=-"
            "&query=t/6459/n1/all/v/4090/p/all/c12027/all/l/v,c12027,t%2Bp"
        ),
    },
    6460: {
        "name": (
            "Percentual de pessoas contribuintes de instituto de previdência em qualquer trabalho, na "
            "população de 14 anos ou mais de idade, ocupada na semana de referência - Total, coeficiente "
            "de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano anterior "
            "(Vide Notas) (nº6460)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6460.xlsx&terr=N&rank=-"
            "&query=t/6460/n1/all/v/8463/p/all/d/v8463%201/l/v,,t%2Bp"
        ),
    },
    6461: {
        "name": (
            "Taxa de participação na força de trabalho, na semana de referência, das pessoas de 14 anos "
            "ou mais de idade - Total, coeficiente de variação, variações em relação ao trimestre anterior "
            "e ao mesmo trimestre do ano anterior (Vide Notas) (nº6461)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6461.xlsx&terr=N&rank=-"
            "&query=t/6461/n1/all/v/4096/p/all/d/v4096%201/l/v,,t%2Bp"
        ),
    },
    6462: {
        "name": (
            "População - Total, coeficiente de variação, variações percentuais e absolutas em relação ao "
            "trimestre anterior e ao mesmo trimestre do ano anterior (Vide Notas) (nº6462)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6462.xlsx&terr=N&rank=-"
            "&query=t/6462/n1/all/v/606/p/all/l/v,,t%2Bp"
        ),
    },
    6463: {
        "name": (
            "Pessoas de 14 anos ou mais de idade - Total, coeficiente de variação, variações percentuais "
            "e absolutas em relação ao trimestre anterior e ao mesmo trimestre do ano anterior, por condição "
            "em relação à força de trabalho e condição de ocupação (Vide Notas) (nº6463)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6463.xlsx&terr=N&rank=-"
            "&query=t/6463/n1/all/v/1641/p/all/c629/all/l/v,c629,t%2Bp"
        ),
    },
    6464: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - Total, coeficiente "
            "de variação, variações percentuais e absolutas em relação ao trimestre anterior e ao mesmo "
            "trimestre do ano anterior - por posição na ocupação e categoria do emprego no trabalho principal "
            "(Vide Notas) (nº6464)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6464.xlsx&terr=N&rank=-"
            "&query=t/6464/n1/all/v/4090/p/all/c11913/all/l/v,c11913,t%2Bp"
        ),
    },
    6465: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - Total, coeficiente "
            "de variação, variações percentuais e absolutas em relação ao trimestre anterior e ao mesmo "
            "trimestre do ano anterior - por grupamento de atividade no trabalho principal (Vide Notas) "
            "(nº6465)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6465.xlsx&terr=N&rank=-"
            "&query=t/6465/n1/all/v/4090/p/all/c888/all/l/v,c888,t%2Bp"
        ),
    },
    6466: {
        "name": (
            "Nível da ocupação, na semana de referência, das pessoas de 14 anos ou mais de idade - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior (Vide Notas) (nº6466)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6466.xlsx&terr=N&rank=-"
            "&query=t/6466/n1/all/v/4097/p/all/d/v4097%201/l/v,,t%2Bp"
        ),
    },
    6467: {
        "name": (
            "Nível da desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior (Vide Notas) (nº6467)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6467.xlsx&terr=N&rank=-"
            "&query=t/6467/n1/all/v/4098/p/all/d/v4098%201/l/v,,t%2Bp"
        ),
    },
    6468: {
        "name": (
            "Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior (Vide Notas) (nº6468)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6468.xlsx&terr=N&rank=-"
            "&query=t/6468/n1/all/v/4099/p/all/d/v4099%201/l/v,,t%2Bp"
        ),
    },
    6469: {
        "name": (
            "Rendimento médio mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, efetivamente recebido em todos os trabalhos - "
            "Total, coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre "
            "do ano anterior (Vide Notas) (nº6469)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6469.xlsx&terr=N&rank=-"
            "&query=t/6469/n1/all/v/5935/p/all/l/v,,t%2Bp"
        ),
    },
    6470: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de "
            "referência com rendimento de trabalho, efetivamente recebido no trabalho principal - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior (Vide Notas) (nº6470)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6470.xlsx&terr=N&rank=-"
            "&query=t/6470/n1/all/v/5934/p/all/l/v,,t%2Bp"
        ),
    },
    6471: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de "
            "referência com rendimento de trabalho, habitualmente recebido no trabalho principal - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior - por posição na ocupação e categoria do emprego no trabalho principal (Vide Notas) "
            "(nº6471)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6471.xlsx&terr=N&rank=-"
            "&query=t/6471/n1/all/v/5932/p/last%201/c11913/all/l/v,c11913,t%2Bp"
        ),
    },
    6472: {
        "name": (
            "Rendimento médio mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente recebido em todos os trabalhos - "
            "Total, coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre "
            "do ano anterior (Vide Notas) (nº6472)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6472.xlsx&terr=N&rank=-"
            "&query=t/6472/n1/all/v/5933/p/all/l/v,,t%2Bp"
        ),
    },
    6473: {
        "name": (
            "Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de "
            "referência com rendimento de trabalho, habitualmente recebido no trabalho principal - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior - por grupamento de atividade no trabalho principal (Vide Notas) (nº6473)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6473.xlsx&terr=N&rank=-"
            "&query=t/6473/n1/all/v/5932/p/last%201/c888/all/l/v,c888,t%2Bp"
        ),
    },
    6474: {
        "name": (
            "Massa de rendimento mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, habitualmente recebido em todos os trabalhos - "
            "Total, coeficiente de variação, variações percentuais e absolutas em relação ao trimestre anterior "
            "e ao mesmo trimestre do ano anterior (Vide Notas) (nº6474)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6474.xlsx&terr=N&rank=-"
            "&query=t/6474/n1/all/v/6293/p/all/l/v,,t%2Bp"
        ),
    },
    6475: {
        "name": (
            "Massa de rendimento mensal real e nominal das pessoas de 14 anos ou mais de idade ocupadas na "
            "semana de referência com rendimento de trabalho, efetivamente recebido em todos os trabalhos - "
            "Total, coeficiente de variação, variações percentuais e absolutas em relação ao trimestre anterior "
            "e ao mesmo trimestre do ano anterior (Vide Notas) (nº6475)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6475.xlsx&terr=N&rank=-"
            "&query=t/6475/n1/all/v/6295/p/all/l/v,,t%2Bp"
        ),
    },
    6482: {
        "name": (
            "Pessoas de 14 anos ou mais de idade - Total, coeficiente de variação, variações percentuais e "
            "absolutas em relação ao trimestre anterior e ao mesmo trimestre do ano anterior - por tipo de medida "
            "de subutilização da força de trabalho na semana de referência (Vide Notas) (nº6482)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6482.xlsx&terr=N&rank=-"
            "&query=t/6482/n1/all/v/1641/p/all/c604/all/l/v,c604,t%2Bp"
        ),
    },
    6483: {
        "name": (
            "Taxa combinada de desocupação e de subocupação por insuficiência de horas trabalhadas - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior (Vide Notas) (nº6483)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6483.xlsx&terr=N&rank=-"
            "&query=t/6483/n1/all/v/4114/p/all/d/v4114%201/l/v,,t%2Bp"
        ),
    },
    6484: {
        "name": (
            "Taxa combinada da desocupação e da força de trabalho potencial - Total, coeficiente de variação, "
            "variações em relação ao trimestre anterior e ao mesmo trimestre do ano anterior (Vide Notas) (nº6484)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6484.xlsx&terr=N&rank=-"
            "&query=t/6484/n1/all/v/4116/p/all/d/v4116%201/l/v,,t%2Bp"
        ),
    },
    6485: {
        "name": (
            "Taxa composta da subutilização da força de trabalho - Total, coeficiente de variação, variações "
            "em relação ao trimestre anterior e ao mesmo trimestre do ano anterior (Vide Notas) (nº6485)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6485.xlsx&terr=N&rank=-"
            "&query=t/6485/n1/all/v/4118/p/all/d/v4118%201/l/v,,t%2Bp"
        ),
    },
    6808: {
        "name": (
            "Taxa de subocupação por insuficiência de horas trabalhadas - Total, coeficiente de variação, "
            "variações em relação ao trimestre anterior e ao mesmo trimestre do ano anterior (Vide Notas) (nº6808)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6808.xlsx&terr=N&rank=-"
            "&query=t/6808/n1/all/v/9819/p/all/d/v9819%201/l/v,,t%2Bp"
        ),
    },
    6813: {
        "name": (
            "Percentual de pessoas desalentadas na população na força de trabalho ou desalentada - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano "
            "anterior (Vide Notas) (nº6813)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela6813.xlsx&terr=N&rank=-"
            "&query=t/6813/n1/all/v/9869/p/all/d/v9869%201/l/v,,t%2Bp"
        ),
    },
    8517: {
        "name": (
            "Pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - Total, coeficiente de variação, "
            "variações percentuais e absolutas em relação ao trimestre anterior e ao mesmo trimestre do ano anterior - "
            "por situação de informalidade no trabalho principal (Vide Notas) (nº8517)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8517.xlsx&terr=N&rank=-"
            "&query=t/8517/n1/all/v/4090/p/all/c1350/all/l/v,c1350,t%2Bp"
        ),
    },
    8529: {
        "name": (
            "Taxa de informalidade das pessoas de 14 anos ou mais de idade, ocupadas na semana de referência - Total, "
            "coeficiente de variação, variações em relação ao trimestre anterior e ao mesmo trimestre do ano anterior "
            "(Vide Notas) (nº8529)"
        ),
        "url": (
            "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela8529.xlsx&terr=N&rank=-"
            "&query=t/8529/n1/all/v/12466/p/all/d/v12466%201/l/v,,t%2Bp"
        ),
    },
}

# --- Helpers ----------------------------------------------------------------


def detect_period_column(df: pd.DataFrame) -> str:
    """Return the most likely period column name."""
    for col in df.columns:
        col_lower = str(col).lower()
        if "trimestre" in col_lower or "periodo" in col_lower or "período" in col_lower:
            return col
    # Fallback to the first column
    return df.columns[0]


def determine_period_range_from_dataframe(df: pd.DataFrame) -> Tuple[str, str]:
    """Determine the start and end period (string) from a DataFrame."""
    if df.empty:
        return None, None

    period_col = detect_period_column(df)
    series = df[period_col].dropna()
    if series.empty:
        return None, None

    start = str(series.iloc[0])
    end = str(series.iloc[-1])
    return start, end


def determine_period_range_from_sheets(sheets: Dict[str, pd.DataFrame]) -> Tuple[str, str]:
    """Determine start/end period from the first sheet containing timeline data."""
    for df in sheets.values():
        start, end = determine_period_range_from_dataframe(df)
        if start and end:
            return start, end
    return None, None


def fetch_and_clean_all_sheets(url: str) -> Dict[str, pd.DataFrame]:
    """Fetch all sheets for a table and return cleaned DataFrames keyed by sheet name."""
    sheets_raw = fetch_all_sheets(url)
    cleaned = {}
    for sheet_name, (df_raw, sector_names) in sheets_raw.items():
        if sheet_name and sheet_name.lower() in {"notas", "notes", "metadata"}:
            continue
        try:
            df_clean = clean_and_structure_data(df_raw, sector_names)
        except Exception as exc:
            print(f"  [WARNING] Failed to clean sheet '{sheet_name}': {exc}")
            continue
        if not df_clean.empty:
            cleaned[sheet_name] = df_clean
    return cleaned


def upload_single_table(
    table_number: int, name: str, df: pd.DataFrame, period_range: Tuple[str, str]
) -> bool:
    """Upload a single-sheet table to Firebase."""
    start, end = period_range
    period = None
    if start and end:
        period = f"{start} a {end}"
        print(f"   Detected period range: {period}")

    return upload_table_data(
        df,
        table_number,
        name,
        period_range=period,
        category=CATEGORY,
    )


def upload_multi_sheet_table(
    table_number: int,
    name: str,
    sheets: Dict[str, pd.DataFrame],
    period_range: Tuple[str, str],
) -> Dict[str, bool]:
    """Upload a multi-sheet table to Firebase."""
    start, end = period_range
    period = None
    if start and end:
        period = f"{start} a {end}"
        print(f"   Detected period range: {period}")

    return upload_multiple_sheets_to_firebase(
        sheets,
        table_number,
        name,
        period_range=period,
        category=CATEGORY,
    )


def process_table(table_number: int, config: Dict[str, object]) -> None:
    """Fetch, process, and upload a PNADCT table."""
    name = config["name"]
    url = config["url"]
    explicit_multi = config.get("multi_sheet", False)

    print(f"\n{'='*80}")
    print(f"Processing table {table_number} - {name}")
    print(f"URL: {url}")

    if explicit_multi:
        print("-> Multi-sheet table (configured)")
        sheets = fetch_and_clean_all_sheets(url)
        print(f"   Cleaned sheets: {len(sheets)}")
        if not sheets:
            print("   [WARNING] No data sheets found; skipping upload.")
            return

        period_range = determine_period_range_from_sheets(sheets)
        results = upload_multi_sheet_table(table_number, name, sheets, period_range)
        success = all(results.values())
        print(f"   Upload status: {'SUCCESS' if success else 'PARTIAL'}")
        return

    # Attempt to treat as single sheet; fall back to multi if more than one sheet of data exists.
    sheets = fetch_and_clean_all_sheets(url)
    data_sheet_names = list(sheets.keys())

    if not sheets:
        print("   [WARNING] No data sheets found; skipping upload.")
        return

    if len(sheets) == 1:
        sheet_name = data_sheet_names[0]
        print(f"-> Single-sheet table (detected). Sheet: {sheet_name}")
        df = sheets[sheet_name]
        period_range = determine_period_range_from_dataframe(df)
        success = upload_single_table(table_number, name, df, period_range)
        print(f"   Upload status: {'SUCCESS' if success else 'FAILED'}")
    else:
        print(f"-> Multi-sheet table (detected). Sheets: {len(sheets)}")
        period_range = determine_period_range_from_sheets(sheets)
        results = upload_multi_sheet_table(table_number, name, sheets, period_range)
        success = all(results.values())
        print(f"   Upload status: {'SUCCESS' if success else 'PARTIAL'}")


# --- Main entry point ------------------------------------------------------


def normalize_table_selection(selection: Optional[Sequence[str]]) -> Optional[List[int]]:
    """Normalize CLI table selections into a list of configured table numbers."""
    if not selection:
        return None

    resolved: List[int] = []
    for raw in selection:
        token = str(raw).strip()
        if not token:
            continue
        if token.lower() in {"all", "*"}:
            return None
        try:
            table_number = int(token)
        except ValueError:
            print(f"[WARNING] Invalid table identifier '{token}', skipping.")
            continue
        if table_number not in PNADCT_TABLES:
            print(f"[WARNING] Table {table_number} is not configured; skipping.")
            continue
        resolved.append(table_number)
    return resolved or None


def fetch_and_upload_pnadct_tables(selected_tables: Optional[Sequence[int]] = None) -> None:
    """Process all configured PNADCT tables or a selected subset."""
    if selected_tables is None:
        target_tables = PNADCT_TABLES.keys()
    else:
        target_tables = selected_tables

    for table_number in target_tables:
        config = PNADCT_TABLES.get(table_number)
        if not config:
            print(f"\n[WARNING] Table {table_number} is not configured; skipping.")
            continue
        try:
            process_table(table_number, config)
        except Exception as exc:
            print(f"[ERROR] Failed to process table {table_number}: {exc}")
            import traceback

            traceback.print_exc()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and upload IBGE PNAD Contínua (PNADCT) tables to Firebase."
    )
    parser.add_argument(
        "--table",
        dest="tables",
        action="append",
        help="Specific table number to process (repeatable). Omit to run all tables.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selection = normalize_table_selection(args.tables)
    fetch_and_upload_pnadct_tables(selection)


if __name__ == "__main__":
    main()

