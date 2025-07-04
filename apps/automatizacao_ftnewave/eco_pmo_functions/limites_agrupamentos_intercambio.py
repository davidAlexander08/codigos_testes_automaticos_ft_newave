from inewave.newave import Sistema
from inewave.newave import Patamar
from inewave.newave import Agrint
import re
import pandas as pd
from datetime import datetime
from os.path import join
from apps.automatizacao_ftnewave.eco_pmo_functions import peq_usi, carga_adic_tot
import numpy as np
mapa_submercados = {
    1: "SUDESTE",
    2: "SUL",
    4: "NORTE",
    3: "NORDESTE"
}

def compara_agrupamento_intercambio(data_ini, caminho_TesteBase, caminho_testes_eco):
    df_agrupamento_intercambio_pmo = leitura_pmo_bloco_agrupamento_intercambio(data_ini, caminho_TesteBase,"LIMITES DOS AGRUPAMENTOS DE INTERCAMBIO", "CONFIGURACAO DOS REEs")
    agrint = Agrint.read(caminho_TesteBase+"/agrint.dat")
    df_agrupamentos = agrint.agrupamentos
    df_limites_agrupamentos = agrint.limites_agrupamentos
    df_limites_agrupamentos["patamar"] = df_limites_agrupamentos["patamar"].astype(int)
    agrupamentos = df_agrupamentos["agrupamento"].unique()
    patamares = df_limites_agrupamentos["patamar"].unique()
    datas = df_agrupamento_intercambio_pmo["data"].unique()
    lista_df = []
    for agrupamento in agrupamentos:
        df_agrupamento_intercambio_pmo_agp = df_agrupamento_intercambio_pmo.loc[df_agrupamento_intercambio_pmo["agrupamento"] == agrupamento].reset_index(drop=True)
        df_limites_agrupamentos_agp = df_limites_agrupamentos.loc[df_limites_agrupamentos["agrupamento"] == agrupamento].reset_index(drop=True)
        for data in datas:
            df_agrupamento_intercambio_pmo_agp_data = df_agrupamento_intercambio_pmo_agp.loc[df_agrupamento_intercambio_pmo_agp["data"] == data].reset_index(drop=True)
            df_limites_agrupamentos_agp_data = df_limites_agrupamentos_agp.loc[(df_limites_agrupamentos_agp["data_inicio"] <= data) ].reset_index(drop=True)
            df_limites_agrupamentos_agp_data = df_limites_agrupamentos_agp_data[
                df_limites_agrupamentos_agp_data["data_inicio"] == df_limites_agrupamentos_agp_data["data_inicio"].max()
            ].reset_index(drop=True)            
            for pat in patamares:
                df_agrupamento_intercambio_pmo_agp_data_pat = df_agrupamento_intercambio_pmo_agp_data.loc[df_agrupamento_intercambio_pmo_agp_data["patamar"] == int(pat)].reset_index(drop=True)
                df_limites_agrupamentos_agp_data_pat = df_limites_agrupamentos_agp_data.loc[df_limites_agrupamentos_agp_data["patamar"] == pat].reset_index(drop=True)
                df_resultante = pd.DataFrame()
                df_resultante["data"] = df_agrupamento_intercambio_pmo_agp_data_pat["data"]
                df_resultante["agrupamento"] = agrupamento
                df_resultante["patamar"] = pat
                df_resultante["pmo"] = df_agrupamento_intercambio_pmo_agp_data_pat["valor"]
                df_resultante["limite"] = df_limites_agrupamentos_agp_data_pat["valor"]
                df_resultante["erro"] = df_resultante["pmo"] - df_resultante["limite"]
                if(df_resultante["erro"].abs().max() > 1):
                    print("ERRO MAIOR QUE 1: ", agrupamento, pat, "Erro: ", df_resultante["erro"].abs().max())
                    print(df_resultante)
                else:
                    df_resultante["erro"] = np.where(df_resultante["erro"] < 1, 0, df_resultante["erro"])
                lista_df.append(df_resultante)  
    df_resultante = pd.concat(lista_df).reset_index(drop=True)
    df_resultante.to_csv(join(caminho_testes_eco, "sanidade_agrupamento_intercambio.csv"), index=False)
    print("SANIDADE AGRUPAMENTO INTERCAMBIO: ", df_resultante["erro"].sum()) 



def leitura_pmo_bloco_agrupamento_intercambio(data_ini, caminho_TesteBase, bloco_inicial, bloco_final):
    lista_df = []
    with open(caminho_TesteBase+"/pmo.dat", "r", encoding="latin1") as f:
        flag = 0
        leitura_arquivo_dados_intercambio = 0
        for linha in f:
            linha = linha.strip()
            # Detecta início de um novo bloco com patamar
            if bloco_inicial in linha:
                flag = 1
            if bloco_final in linha:
                flag = 0
                break


            if flag == 1:
                if(linha.startswith("GRUPO")):
                    match = re.search(r"GRUPO:\s+(\d+),\s+PATAMAR:\s+(\d+)", linha)
                    if match:
                        grupo = int(match.group(1))
                        patamar = int(match.group(2))
                if linha.startswith("20") or linha.startswith("POS"):
                    valores = linha.split()
                    ano = int(valores[0]) if linha.startswith("20") else 9999
                    meses = [float(v) for v in valores[1:]]
                    for mes, valor in enumerate(meses, start=1):
                        if(datetime(ano, mes, 1) >= data_ini):
                            lista_df.append(
                                pd.DataFrame(
                                    {
                                        "ano": ano,
                                        "mes": mes,
                                        "data": datetime(ano, mes, 1),
                                        "agrupamento": grupo,
                                        "patamar": int(patamar),
                                        "valor": valor
                                    }, index=[0]
                                )
                            )
    df_saida = pd.concat(lista_df).reset_index(drop=True)
    return df_saida