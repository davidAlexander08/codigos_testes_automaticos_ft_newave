from typing import List
from os.path import join
from datetime import datetime
from calendar import monthrange
import numpy as np
import pandas as pd
from inewave.newave import Dger
from apps.utils.log import Log
import os.path
from apps.avalia_fpha.caso import CasoAvalicao
from apps.avalia_fpha.usina import UsinaAvalicao
from apps.avalia_balanco.configuracao import Configuracao
from inewave.newave import AvlCortesFpha
from inewave.newave import Hidr
from inewave.newave import Confhd
from inewave.newave import Pmo
from inewave.newave import Patamar
from inewave.newave import Sistema
from inewave.nwlistop.viol_fpha import ViolFpha
from inewave.nwlistop import GhmaxFpha
from inewave.newave import FphaEco
from inewave.newave import FphaCortes
from inewave.nwlistop import Varmuh
from inewave.nwlistop import Qafluh
from inewave.nwlistop import Qdesviouh
from inewave.nwlistop import Qturuh
from inewave.nwlistop import Qvertuh
from inewave.nwlistop import Vretiradauh
from inewave.nwlistop import Vevapuh
from inewave.nwlistop import Gttotsin
from inewave.nwlistop import Gttot
from inewave.nwlistop import Ghtotsin
from inewave.nwlistop import Ghtot
from inewave.nwlistop import Merclsin
from inewave.nwlistop import Mercl
from inewave.nwlistop import Excessin
from inewave.nwlistop import Exces
from inewave.nwlistop import Defsin
from inewave.nwlistop import Def
from inewave.nwlistop import Intercambio
from inewave.libs import UsinasHidreletricas
from apps.avalia_fpha.indicadores import IndicadoresAvaliacaoFPHA

class FPHA:

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_fpha = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/FPHA"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_fpha):
            os.makedirs(self.caminho_testes_fpha)
            print(f"Folder '{self.caminho_testes_fpha}' created!")
        else:
            print(f"Folder '{self.caminho_testes_fpha}' already exists!")

        self.patamar = Patamar.read(self.caminho_TesteBase+"/patamar.dat")
        self.numeroPatamares = self.patamar.numero_patamares
        self.duracaoMensalPatamares = self.patamar.duracao_mensal_patamares
        self.cargaPatamares = self.patamar.carga_patamares
        self.intercambioPatamares = self.patamar.intercambio_patamares
        self.usinasNaoSimuladasPatamares = self.patamar.usinas_nao_simuladas
        self.sistema = Sistema.read(self.caminho_TesteBase+"/sistema.dat")
        self.confhd = Confhd.read(self.caminho_TesteBase+"/confhd.dat")
        self.dger = Dger.read(self.caminho_TesteBase+"/dger.dat")
        periodo_proximo_mes = 12 - (self.dger.mes_inicio_estudo -1) + 1
        self.usinas = self.confhd.usinas        
        lista_nome_usinas = self.usinas["nome_usina"].unique()

        mes_ini = self.dger.mes_inicio_estudo
        ano_ini = self.dger.ano_inicio_estudo
        self.series_sf = self.dger.num_series_sinteticas
        self.data_ini = pd.to_datetime(str(ano_ini)+"-"+str(mes_ini)+"-01")

        self.df_hidr = Hidr.read(self.caminho_TesteBase+"/hidr.dat").cadastro
        self.df_hidr.to_csv(self.caminho_FTNEWAVE+"/eco_hidr.csv")

        self.pmo = Pmo.read(self.caminho_TesteBase+"/pmo.dat")
        self.vol_ini = self.pmo.volume_armazenado_inicial

        df_NwvEcoFpha = FphaEco.read(self.caminho_TesteBase+"/fpha_eco.csv").tabela
        df_NwvCortesFpha = FphaCortes.read(self.caminho_TesteBase+"/fpha_cortes.csv").tabela
        lista_GHMAX_Calculado = []
        for usi in lista_nome_usinas:
            df_confhd_usi =    self.usinas.loc[(self.usinas["nome_usina"] == usi)]   
            cod_usi = df_confhd_usi["codigo_usina"].iloc[0]
            texto_arquivo = ""
            if(int(cod_usi) <= 9): texto_arquivo = "00"+str(cod_usi)
            elif(int(cod_usi) <= 99): texto_arquivo = "0"+str(cod_usi)
            elif(int(cod_usi) <= 999): texto_arquivo = str(cod_usi)


            self.df_usi = self.df_hidr.loc[(self.df_hidr["nome_usina"] == usi)].reset_index(drop = True)

            arq_Qturuh  = self.caminho_TesteBase+"/qturuh"+texto_arquivo+".out"
            self.__Qturuh = Qturuh.read(arq_Qturuh).valores
            self.__Qturuh = self.__Qturuh.loc[self.__Qturuh["data"] >= self.data_ini].reset_index(drop = True)
            arq_Qvertuh  = self.caminho_TesteBase+"/qvertuh"+texto_arquivo+".out"
            self.__Qvertuh = Qvertuh.read(arq_Qvertuh).valores
            self.__Qvertuh = self.__Qvertuh.loc[self.__Qvertuh["data"] >= self.data_ini].reset_index(drop = True)

            self.series = list(self.__Qturuh["serie"].unique())
            arq_Varmuh  = self.caminho_TesteBase+"/varmuh"+texto_arquivo+".out"
            self.__Varmuh = Varmuh.read(arq_Varmuh).valores
            if(self.__Varmuh is not None):
                self.__Varmuh = self.__Varmuh.loc[self.__Varmuh["data"] >= self.data_ini].reset_index(drop = True)
            self.__Varmi = self.vol_ini.loc[(self.vol_ini["nome_usina"] == usi)]["valor_hm3"].iloc[0]

            arq_GHMAXFPHA  = self.caminho_TesteBase+"/ghmax_fpha"+texto_arquivo+".out"
            self.__GHMAXFPHA = GhmaxFpha.read(arq_GHMAXFPHA).valores
            self.__GHMAXFPHA = self.__GHMAXFPHA.loc[self.__GHMAXFPHA["data"] >= self.data_ini].reset_index(drop = True)


            datas = list(self.__Qturuh["data"].unique())
            self.enumerated_dict = {s: i for i, s in enumerate(datas, start=1)}
            self.df_eco_fpha_usi = df_NwvEcoFpha.loc[(df_NwvEcoFpha["nome_usina"] == usi)].reset_index(drop = True)
            self.df_cortes_fpha_usi = df_NwvCortesFpha.loc[(df_NwvCortesFpha["nome_usina"] == usi)].reset_index(drop = True)
            self.df_cortes_fpha_usi["periodo"] = self.df_cortes_fpha_usi["periodo"] - (mes_ini - 1)
            self.__gera_ghmax_calculado(usi)

            lista_NW = []
            lista_Erro = []
            for data in self.__ghmax_calculado["data"].unique():
                for serie in self.__ghmax_calculado["serie"].unique():
                    for pat in self.__ghmax_calculado["patamar"].unique():
                        df_nw = self.__GHMAXFPHA.loc[(self.__GHMAXFPHA["data"] == data) & (self.__GHMAXFPHA["serie"] == serie)& (self.__GHMAXFPHA["patamar"] == pat)]["valor"].iloc[0]
                        df_calc = self.__ghmax_calculado.loc[(self.__ghmax_calculado["data"] == data) & (self.__ghmax_calculado["serie"] == serie) & (self.__ghmax_calculado["patamar"] == pat)]["GHMAX"].iloc[0]
                        lista_NW.append(df_nw)
                        lista_Erro.append(df_nw - df_calc)
            self.__ghmax_calculado["NW"] = lista_NW
            self.__ghmax_calculado["Erro"] =  lista_Erro
            self.__ghmax_calculado.to_csv(self.caminho_testes_fpha+"/FT_fpha_"+usi+".csv")
            self.__ghmax_memoria_calculo.to_csv(self.caminho_testes_fpha+"/FT_memo_fpha_"+usi+".csv")
            lista_GHMAX_Calculado.append(self.__ghmax_memoria_calculo)
        self.GHMAXUsinas = pd.concat(lista_GHMAX_Calculado)

    def __gera_ghmax_calculado(self, usi) -> pd.DataFrame:
        df_ghmax_memoria = pd.DataFrame(columns=["usina", "data","serie", "patamar","corte","Volf", "Voli","Vol","Coef Vol", "Qturb", "Coef Qturb", "Qvert", "Coef Qvert", "Fator Correc", "RHS", "GHMAX"   ])
        df_ghmax_resultado = pd.DataFrame(columns=["usina", "data", "serie", "patamar","Volf", "Voli","Vol","Qturb", "Qvert", "GHMAX"   ])
        lista_df = []
        vol_min_usina = self.df_usi["volume_minimo"].iloc[0]
        for data in self.__Qturuh["data"].unique():
            df_coefs_usi_per = self.df_cortes_fpha_usi.loc[(self.df_cortes_fpha_usi["periodo"] == self.enumerated_dict[data])].reset_index( drop = True)
            df_qtur_usi_per = self.__Qturuh.loc[(self.__Qturuh["data"] == data)].reset_index( drop = True)
            df_qver_usi_per = self.__Qvertuh.loc[(self.__Qvertuh["data"] == data)].reset_index( drop = True)
            if(self.__Varmuh is not None):
                df_varmf_usi_per = self.__Varmuh.loc[(self.__Varmuh["data"] == data)  ].reset_index( drop = True)
            else:
                Volf = 0
            for cen in self.__Qturuh["serie"].unique():
                if(self.__Varmuh is not None):
                    Volf =    varmf_data.loc[ (varmf_data["serie"] == serie) ]["valor"].iloc[0]
                    if(data == self.data_ini):
                        Voli =  self.__Varmi
                    else:
                        Voli =  self.__Varmuh.loc[(self.__Varmuh["data"] == data-pd.DateOffset(months=1))  & (self.__Varmuh["serie"] == serie) ]["valor"].iloc[0]
                else:
                    Voli = 0
                    Volf = 0
                if(self.__Varmuh is not None):
                    Vol = ((Volf + Voli)/2 ) 
                else:
                    Vol = 0
                for pat in self.__Qturuh["patamar"].unique():
                    durPat = self.duracaoMensalPatamares.loc[(self.duracaoMensalPatamares["data"] == data) & (self.duracaoMensalPatamares["patamar"] == int(pat)) ]["valor"].iloc[0]
                    Qturb = df_qtur_usi_per.loc[(df_qtur_usi_per["serie"] == cen ) & (df_qtur_usi_per["patamar"] == pat  )]["valor"].iloc[0]
                    Qvert = df_qver_usi_per.loc[(df_qver_usi_per["serie"] == cen ) & (df_qver_usi_per["patamar"] == pat  )]["valor"].iloc[0]
                    listaCandidatosGHMAX = []
                    for index, row in df_coefs_usi_per.iterrows():
                        GHMAX_temp = row["fator_correcao"]*(row["coeficiente_volume_util_MW_hm3"]*Vol + row["coeficiente_vazao_turbinada_MW_m3s"]*Qturb*durPat + row["coeficiente_vazao_vertida_MW_m3s"]*Qvert*durPat + row["rhs_energia"])
                        listaCandidatosGHMAX.append(GHMAX_temp)
                        new_row = pd.DataFrame({"usina": usi,
                                                        "data": data,
                                                        "serie": cen,
                                                        "patamar": pat,
                                                        "durpar": durPat,
                                                        "corte": row["indice_corte"],
                                                        "Volf": Volf,
                                                        "Voli": Voli,
                                                        "Vol": Vol,
                                                        "Coef Vol": row["coeficiente_volume_util_MW_hm3"],
                                                        "Qturb":Qturb,
                                                        "Coef Qturb":row["coeficiente_vazao_turbinada_MW_m3s"],
                                                        "Qvert":Qvert,
                                                        "Coef Qvert":row["coeficiente_vazao_vertida_MW_m3s"],
                                                        "Fator Correc":row["fator_correcao"],
                                                        "RHS":row["rhs_energia"],
                                                        "GHMAX":round(GHMAX_temp,2)},
                                                index = [0])
                        df_ghmax_memoria = pd.concat([df_ghmax_memoria.loc[:],new_row]).reset_index(drop=True)
                    GHMAX = min(listaCandidatosGHMAX)
                    new_row = pd.DataFrame({"usina": usi,
                                                    "data": data,
                                                    "serie": cen,
                                                    "patamar": pat,
                                                    "Volf": Volf,
                                                    "Voli": Voli,
                                                    "Vol": Vol,
                                                    "Qturb":Qturb,
                                                    "Qvert":Qvert,
                                                    "GHMAX":round(GHMAX,2)},
                                            index = [0])
                    df_ghmax_resultado = pd.concat([df_ghmax_resultado.loc[:],new_row]).reset_index(drop=True)
                    self.__ghmax_calculado = df_ghmax_resultado
                    self.__ghmax_memoria_calculo = df_ghmax_memoria
        return 0
