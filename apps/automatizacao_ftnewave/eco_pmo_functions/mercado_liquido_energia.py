from inewave.newave import Sistema
from inewave.newave import Patamar
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

def compara_mercado_energia_liquida(data_ini, caminho_TesteBase, caminho_testes_eco):
    df_mercado_liquido_pmo = leitura_pmo_bloco_merc_liq(data_ini, caminho_TesteBase,"DADOS DE MERCADO LIQUIDO DE ENERGIA", "ASSOCIACAO ENTRE REEs E SUBSISTEMAS")
    sistema = Sistema.read(caminho_TesteBase+"/sistema.dat")

    df_mercado_total_pmo = carga_adic_tot.leitura_pmo_bloco_ano(caminho_TesteBase,"DADOS DE MERCADO TOTAL DE ENERGIA", "DADOS DE GERACAO DE PEQUENAS USINAS")

    df_c_adic_pmo = carga_adic_tot.leitura_pmo_bloco_ano(caminho_TesteBase, "DADOS DE CARGA ADICIONAL DE ENERGIA", "DADOS DE MERCADO TOTAL DE ENERGIA")
    df_mercado_total_pmo["valor"] = df_mercado_total_pmo["valor"] + df_c_adic_pmo["valor"]

    patamar = Patamar.read(caminho_TesteBase+"/patamar.dat")
    lista_df = []
    df_patamar_carga = patamar.carga_patamares
    df_patamar_carga = df_patamar_carga.loc[(df_patamar_carga["data"] >= data_ini)].reset_index(drop=True)
    patamares = df_patamar_carga["patamar"].unique()

    df_peq_usi = peq_usi.leitura_pmo_bloco_peq_usi(data_ini, caminho_TesteBase, "DADOS DE GERACAO DE PEQUENAS USINAS", "DADOS DE MERCADO LIQUIDO DE ENERGIA")
    df_peq_usi = df_peq_usi.loc[(df_peq_usi["fonte"] == "TOTAL")].reset_index(drop = True)

    for sbm in mapa_submercados:
        df_mercado_total_pmo_sbm = df_mercado_total_pmo.loc[(df_mercado_total_pmo["submercado"] == mapa_submercados[sbm])].reset_index(drop = True)
        df_peq_usi_sbm = df_peq_usi.loc[(df_peq_usi["submercado"] == mapa_submercados[sbm])].reset_index(drop = True)
        for pat in patamares:
            df_mercado_liquido_pmo_sbm_pat = df_mercado_liquido_pmo.loc[(df_mercado_liquido_pmo["patamar"] == int(pat)) & (df_mercado_liquido_pmo["submercado"] == mapa_submercados[sbm])].reset_index(drop = True)
            df_pat = df_patamar_carga.loc[(df_patamar_carga["patamar"] == pat) & (df_patamar_carga["codigo_submercado"] == sbm)].reset_index(drop = True)
            df_peq_usi_sbm_pat = df_peq_usi_sbm.loc[(df_peq_usi_sbm["patamar"] == int(pat))].reset_index(drop = True)
            
            # Repete ultimo ano do patamar
            ultimo_ano = df_pat["data"].dt.year.max()
            ultimo_est = df_pat[df_pat["data"].dt.year == ultimo_ano]
            df_pat_pos = pd.concat([df_pat, ultimo_est], ignore_index=True).reset_index(drop = True)

            #Repete ultimo ano do peq usi
            ultimo_est = df_peq_usi_sbm_pat[df_peq_usi_sbm_pat["data"].dt.year == ultimo_ano]
            df_peq_usi_sbm_pat_pos = pd.concat([df_peq_usi_sbm_pat, ultimo_est], ignore_index=True).reset_index(drop = True)

            df_patamarizado = df_mercado_total_pmo_sbm.copy().drop(columns=["valor"])
            df_patamarizado["calculado"] = (df_mercado_total_pmo_sbm["valor"]*df_pat_pos["valor"] - df_peq_usi_sbm_pat_pos["valor"])
            df_patamarizado["pmo"] = df_mercado_liquido_pmo_sbm_pat["valor"]
            df_patamarizado["patamar"] = pat
            df_patamarizado["erro"] = df_patamarizado["calculado"] - df_patamarizado["pmo"]
            df_erro_maior_1 = df_patamarizado.loc[(df_patamarizado["erro"].abs() > 1)].reset_index(drop = True)
            if not df_erro_maior_1.empty:
                print("ERRO: ", df_patamarizado["erro"].sum())
                print("df_erro_maior_1: ", df_erro_maior_1)
            else:
                df_patamarizado["erro"] = np.where(df_patamarizado["erro"] < 1, 0, df_patamarizado["erro"])
            lista_df.append(df_patamarizado)
    df_carga_bruta_patamarizada = pd.concat(lista_df).reset_index(drop = True)
    df_carga_bruta_patamarizada.to_csv(join(caminho_testes_eco, "sanidade_mercado_liquido_energia.csv"), index=False)
    print("ERRO TOTAL: ", df_carga_bruta_patamarizada["erro"].sum())
    

def leitura_pmo_bloco_merc_liq(data_ini, caminho_TesteBase, bloco_inicial, bloco_final):
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
                if(linha.startswith("SUBSISTEMA")):
                    match = re.search(r"SUBSISTEMA:\s+([\w\s]+)", linha)
                    if match:
                        subsistema_nome = match.group(1).strip()
                        bloco = "TOTAL"
                # Ignora linhas com "X---"
                if(linha.startswith("PATAMAR")):
                    match = re.search(r"PATAMAR:\s+([\w\s]+)", linha)
                    if match:
                        patamar = match.group(1).strip()
                        
                if linha.startswith("20") or linha.startswith("POS"):
                    valores = linha.split()
                    ## Separate year and values
                    ano = int(valores[0]) if linha.startswith("20") else 9999
#
                    meses = [float(v) for v in valores[1:]]
                    for mes, valor in enumerate(meses, start=1):
                        if(datetime(ano, mes, 1) >= data_ini):
                            lista_df.append(
                                pd.DataFrame(
                                    {
                                        "ano": ano,
                                        "mes": mes,
                                        "data": datetime(ano, mes, 1),
                                        "submercado": subsistema_nome,
                                        "patamar": int(patamar),
                                        "valor": valor
                                    }, index=[0]
                                )
                            )
    df_peq_usi = pd.concat(lista_df).reset_index(drop=True)


    return df_peq_usi