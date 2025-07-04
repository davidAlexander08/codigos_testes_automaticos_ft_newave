from inewave.newave import Pmo
from inewave.newave import Vazpast
from inewave.newave import Confhd
from inewave.newave import Hidr
from inewave.newave import Ree
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
pd.set_option('display.max_columns', None)


def compara_ena_past(data_ini, caminho_TesteBase, caminho_testes_eco):
    pmo = Pmo.read(caminho_TesteBase+"/pmo.dat")
    df_eafpast_tendencia_hidrologica = pmo.eafpast_tendencia_hidrologica
    df_eafpast_cfuga_medio = pmo.eafpast_cfuga_medio
    produtibilidades = pmo.produtibilidades_equivalentes
    configuracao_1_produtibilidade = produtibilidades.loc[(produtibilidades["configuracao"] == 1)].reset_index(drop=True)
    df_vazpast = Vazpast.read(caminho_TesteBase+"/vazpast.dat").tendencia
    df_confhd = Confhd.read(caminho_TesteBase+"/confhd.dat").usinas
    df_hidr = Hidr.read(caminho_TesteBase+"/hidr.dat").cadastro
    df_rees = Ree.read(caminho_TesteBase+"/ree.dat").rees
    rees = df_rees["nome"].unique()
    lista_ena_past = []
    for ree in rees:
        print("ree: ", ree)
        df_ree = df_rees.loc[(df_rees["nome"] == ree)].reset_index(drop=True)
        codigo_ree = df_ree["codigo"].iloc[0]
        df_usinas_ree = df_confhd.loc[(df_confhd["ree"] == codigo_ree)].reset_index(drop=True)
        lista_df_ree = []
        for index, row in df_usinas_ree.iterrows():
            codigo_usina = row.codigo_usina
            posto_usina = row.posto
            nome_usina = row.nome_usina
            df_vazpast_usi = df_vazpast.loc[(df_vazpast["codigo_usina"] == posto_usina)].reset_index(drop=True)
            df_hidr_usi = df_hidr.loc[(df_hidr["nome_usina"] == nome_usina)].reset_index(drop=True)
            regularizacao = df_hidr_usi.tipo_regulacao.iloc[0]
            existente = row.usina_existente
            configuracao_1_produtibilidade_usi = configuracao_1_produtibilidade.loc[(configuracao_1_produtibilidade["nome_usina"] == nome_usina)].reset_index(drop=True)
            if(existente == "NE"):
                prodt = 0
            else:          
                if(regularizacao == "M"):
                    prodt = configuracao_1_produtibilidade_usi["produtibilidade_acumulada_calculo_econ"].iloc[0]
                else:
                    prodt = configuracao_1_produtibilidade_usi["produtibilidade_equivalente_volmin_volmax"].iloc[0]
    
            montantes = buscaUsinasMontanteReservatorio([], codigo_usina, df_confhd, df_hidr)
            if(len(montantes) > 0):
                for usina in montantes:
                    posto_usina = df_confhd.loc[(df_confhd["codigo_usina"] == usina)].reset_index(drop=True)["posto"].iloc[0]
                    df_vazpast_usi_montante = df_vazpast.loc[(df_vazpast["codigo_usina"] == posto_usina)].reset_index(drop=True)
                    df_vazpast_usi["valor"] = df_vazpast_usi["valor"] - df_vazpast_usi_montante["valor"]
            df_vazpast_usi["valor"] = (df_vazpast_usi["valor"]*prodt)
            lista_df_ree.append(df_vazpast_usi)
            df_usi_ena_past = pd.concat(lista_df_ree).reset_index(drop = True)
        df_ree_ena_past = df_usi_ena_past.groupby(["mes"], as_index=False)["valor"].sum()

        df_eafpast_tendencia_hidrologica_ree = df_eafpast_tendencia_hidrologica.loc[(df_eafpast_tendencia_hidrologica["nome_ree"] == ree)].reset_index(drop = True)
        df_ree_ena_past["valor"] = df_ree_ena_past["valor"].round(2)
        df_ree_ena_past["pmo"] = df_eafpast_tendencia_hidrologica_ree["valor"]
        df_ree_ena_past["erro"] = df_ree_ena_past["valor"]  - df_ree_ena_past["pmo"] 
        if(df_ree_ena_past["erro"].abs().max() > 1):
            print("ERRO MAIOR QUE 1: ", ree, "Erro: ", df_ree_ena_past["erro"].abs().max())
            print(df_ree_ena_past.loc[(df_ree_ena_past["mes"] == 1)])
            
        else:
            df_ree_ena_past["erro"] = np.where(df_ree_ena_past["erro"] < 1, 0, df_ree_ena_past["erro"])
            lista_ena_past.append(df_ree_ena_past)


    df_resultante  = pd.concat(lista_ena_past).reset_index(drop = True)
    df_resultante.to_csv(join(caminho_testes_eco, "sanidade_energia_afluente_passada_ree.csv"), index=False)
    print(df_resultante)
    print("SANIDADE ENERGIA AFLUENTE PASSADA: ", df_resultante["erro"].sum()) 

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
