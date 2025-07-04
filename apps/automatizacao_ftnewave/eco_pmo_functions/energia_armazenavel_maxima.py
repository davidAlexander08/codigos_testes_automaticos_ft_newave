from inewave.newave import Pmo
from inewave.newave import Vazpast
from inewave.newave import Confhd
from inewave.newave import Hidr
from inewave.newave import Ree
from inewave.newave import Parpvaz
from inewave.newave import Vazoes
import re
import pandas as pd
from datetime import datetime
from os.path import join
import numpy as np
mapa_submercados = {
    1: "SUDESTE",
    2: "SUL",
    4: "NORTE",
    3: "NORDESTE"
}

def compara_energia_armazenavel_maxima(data_ini, caminho_TesteBase, caminho_testes_eco):


    pmo = Pmo.read(caminho_TesteBase+"/pmo.dat")
    energia_armazenavel_maxima = pmo.energia_armazenada_maxima
    produtibilidades = pmo.produtibilidades_equivalentes
    print(produtibilidades)
    print(energia_armazenavel_maxima.loc[(energia_armazenavel_maxima["configuracao"] == 1)])
    df_hidr = Hidr.read(caminho_TesteBase+"/hidr.dat").cadastro
    configuracao = 1

    df_confhd = Confhd.read(caminho_TesteBase+"/confhd.dat").usinas
    df_rees = Ree.read(caminho_TesteBase+"/ree.dat").rees
    rees = df_rees["nome"].unique()
    for ree in rees:
        df_ree = df_rees.loc[(df_rees["nome"] == ree)].reset_index(drop=True)
        codigo_ree = df_ree["codigo"].iloc[0]
        df_usinas_ree = df_confhd.loc[(df_confhd["ree"] == codigo_ree)].reset_index(drop=True)
        EMAX_REE = 0
        for index, row in df_usinas_ree.iterrows():
            posto = row.posto
            nome_usina = row.nome_usina
            codigo_usina = row.codigo_usina
            df_hidr_usi = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina)].reset_index(drop=True)
            df_confhd_usi = df_confhd.loc[(df_confhd["codigo_usina"] == codigo_usina)].reset_index(drop = True)
            regularizacao = df_hidr_usi.tipo_regulacao.iloc[0]
            if(regularizacao == "M"):
                vol_max_usi = df_hidr_usi.volume_maximo.iloc[0]
                vol_min_usi = df_hidr_usi.volume_minimo.iloc[0]
                volume_util = vol_max_usi - vol_min_usi
                percentual_vol_inic = df_confhd_usi["volume_inicial_percentual"].iloc[0]
                volume_inicial = volume_util*(percentual_vol_inic/100)
                df_prodt_controlavel = produtibilidades.loc[(produtibilidades["configuracao"] == configuracao) & (produtibilidades["nome_usina"] == nome_usina)  ].reset_index(drop=True)
                prodt_controlabel = df_prodt_controlavel["produtibilidade_acumulada_calculo_earm"].iloc[0]
                contribuicao_usina_Emax_REE = (volume_inicial/2.63)*prodt_controlabel
                print("USI: ", nome_usina, " prodt_controlabel: ", prodt , " vmax: ", vol_max_usi, " vmin: ", vol_min_usi, " vutil: ", round(volume_util,2), " perc: ", percentual_vol_inic, " vini: ", volume_inicial, " contrib: ", round(contribuicao_usina_Emax_REE,2))
                EMAX_REE += contribuicao_usina_Emax_REE
        print("REE: ", ree, " EMAX_REE: ", EMAX_REE)

    exit(1)    
    vazoes_inewave = Vazoes.read(caminho_TesteBase+"/vazoes.dat").vazoes
    print(produtibilidades)




    lista_df_energia_controlavel_rees = []
    for ree in rees:
        df_controlavel_pmo_config_ree = df_controlavel_pmo_config.loc[(df_controlavel_pmo_config["ree"] == ree)].reset_index(drop=True)
        df_ree = df_rees.loc[(df_rees["nome"] == ree)].reset_index(drop=True)
        codigo_ree = df_ree["codigo"].iloc[0]
        lista_df_energia_controlavel = []
        df_usinas_ree = df_confhd.loc[(df_confhd["ree"] == codigo_ree)].reset_index(drop=True)
        for index, row in df_usinas_ree.iterrows():
            posto = row.posto
            nome_usina = row.nome_usina
            codigo_usina = row.codigo_usina
            df_hidr_usi = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina)].reset_index(drop=True)
            regularizacao = df_hidr_usi.tipo_regulacao.iloc[0]
            if(regularizacao == "M"):
                df_prodt_controlavel = produtibilidades.loc[(produtibilidades["configuracao"] == configuracao) & 
                                                            (produtibilidades["nome_usina"] == nome_usina)  ].reset_index(drop=True)
                prodt_controlabel = df_prodt_controlavel["produtibilidade_acumulada_calculo_econ"].iloc[0]
                montantes_reservatorio = buscaUsinasMontanteReservatorio([], codigo_usina, df_confhd, df_hidr)
                df_vaz_usi = vazoes_inewave[posto].copy()

                df_vaz_usi = pd.DataFrame({
                                "valor": df_vaz_usi.values,
                                "data": pd.date_range(start="1931-01-01", periods=len(df_vaz_usi), freq="MS")
                            })
                df_vaz_usi["ree"] = ree
                if(len(montantes_reservatorio) > 0):
                    for usina in montantes_reservatorio:
                        posto_usina = df_confhd.loc[(df_confhd["codigo_usina"] == usina)].reset_index(drop=True)["posto"].iloc[0]
                        df_vaz_usi["valor"] = df_vaz_usi["valor"] - vazoes_inewave[posto_usina]
                df_vaz_usi["valor"] = (df_vaz_usi["valor"]*prodt_controlabel)

                
                lista_df_energia_controlavel.append(df_vaz_usi)
        if(len(lista_df_energia_controlavel) != 0):
            df_controlavel_ree = pd.concat(lista_df_energia_controlavel).reset_index(drop=True)
            df_soma_total = df_controlavel_ree.groupby(["data"], as_index=False)["valor"].sum()
            df_soma_total["ree"] = ree
            df_soma_total["valor"] = df_soma_total["valor"].round(1)
            df_soma_total["pmo"] = df_controlavel_pmo_config_ree["valor"]
            df_soma_total["erro"] = df_soma_total["valor"] - df_soma_total["pmo"]
            if(df_soma_total["erro"].abs().max() > 1):
                print("ERRO MAIOR QUE 1: ", ree, "Erro: ", df_soma_total["erro"].abs().max())
            else:
                df_soma_total["erro"] = np.where(df_soma_total["erro"] < 1, 0, df_soma_total["erro"])
            lista_df_energia_controlavel_rees.append(df_soma_total)
    
    df_resultante = pd.concat(lista_df_energia_controlavel_rees).reset_index(drop=True)
    df_resultante.to_csv(join(caminho_testes_eco, "sanidade_energia_controlavel_ree.csv"), index=False)
    print(df_resultante.loc[(df_resultante["data"].dt.year == 1931) & (df_resultante["data"].dt.month == 1)])
    print("SANIDADE ENERGIA CONTROLAVEL: ", df_resultante["erro"].sum()) 


def leitura_energia_controlavel_bloco_ano(caminho_TesteBase, data_ini, bloco_inicial, bloco_final):
    lista_df = []
    flag_leitura = 0
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
                if(linha.startswith("REE")):
                    match = re.search(r"REE:\s*([A-Z0-9\-]+)\s+ENERGIA CONTROLAVEL\s+\(MWmes\)\s+CONFIGURACAO:\s+(\d+)", linha)
                    if match:
                        ree = match.group(1)         # 'SUDESTE'
                        configuracao = match.group(2) # '1'
                        flag_leitura = 1
                if linha.startswith("19") or linha.startswith("20"):
                    if(flag_leitura == 1 and int(configuracao) == 113):
                        valores = linha.split()
                        print("config: ", configuracao)
                        ## Separate year and values
                        ano = int(valores[0])
                        meses = [float(v) for v in valores[1:]]
                        for mes, valor in enumerate(meses, start=1):
                            lista_df.append(
                                pd.DataFrame(
                                    {
                                        "ano": ano,
                                        "mes": mes,
                                        "data": datetime(ano, mes, 1),
                                        "ree": ree,
                                        "configuracao": int(configuracao),
                                        "valor": valor
                                    }, index=[0]
                                )
                            )
    df_peq_usi = pd.concat(lista_df).reset_index(drop=True)
    return df_peq_usi


def buscaUsinasMontanteReservatorio(lista_montantes_reservatorio, codigo_usi, df_confhd, df_hidr):
    montantes = buscaUsinasMontante(codigo_usi, df_confhd)
    for montante in montantes:
        df_confhd_mon = df_confhd.loc[(df_confhd["codigo_usina"] == montante)].reset_index(drop = True)
        nome_usina = df_confhd_mon["nome_usina"].iloc[0]
        df_hidr_usi = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina)].reset_index(drop = True)
        regularizacao = df_hidr_usi.tipo_regulacao.iloc[0]
        if(regularizacao != "M"):
            montantes_mon = buscaUsinasMontanteReservatorio(lista_montantes_reservatorio, montante, df_confhd, df_hidr)
        else:
            lista_montantes_reservatorio.append(montante)
    return lista_montantes_reservatorio

def buscaUsinasMontante(codigo_usi, df_confhd):
    lista_montantes = []
    lista_usinas = df_confhd["codigo_usina"].unique()
    for usi in lista_usinas:
        df_montante = df_confhd.loc[(df_confhd["codigo_usina"] == usi)].reset_index(drop = True)
        usina_jusante = df_montante["codigo_usina_jusante"].iloc[0]
        if(usina_jusante == codigo_usi):
            usina_montante = df_montante["codigo_usina"].iloc[0]
            lista_montantes.append(usina_montante)
    return lista_montantes
