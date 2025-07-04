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
from inewave.newave import VolrefSaz
from inewave.newave import NwvEcoEvap
from inewave.newave import NwvCortesEvap
from inewave.nwlistop import GhmaxFpha
from inewave.nwlistop import Varmuh
from inewave.nwlistop import Vbomb
from inewave.nwlistop import Qafluh
from inewave.nwlistop import Qdesviouh
from inewave.nwlistop import Qturuh
from inewave.nwlistop import Qvertuh
from inewave.nwlistop import Vretiradauh
from inewave.nwlistop import Vevapuh
from inewave.nwlistop import Hmont
from inewave.nwlistop import Hliq
from inewave.libs import UsinasHidreletricas
from apps.avalia_fpha.indicadores import IndicadoresAvaliacaoFPHA
from apps.automatizacao_ftnewave.hjus import FT_Hjus
class FT_Hliq:

    def __init__(self, caminho_deck_ftnewave):

        obj_Hjus = FT_Hjus(caminho_deck_ftnewave)
        self.calcHjus = obj_Hjus.hjusUsinas
        self.calcHmon = obj_Hjus.calcHmon

        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_Hliq = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Altura/Hliq"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_Hliq):
            os.makedirs(self.caminho_testes_Hliq)
            print(f"Folder '{self.caminho_testes_Hliq}' created!")
        else:
            print(f"Folder '{self.caminho_testes_Hliq}' already exists!")

        self.patamar = Patamar.read(self.caminho_TesteBase+"/patamar.dat")
        self.numeroPatamares = self.patamar.numero_patamares
        self.duracaoMensalPatamares = self.patamar.duracao_mensal_patamares
        self.cargaPatamares = self.patamar.carga_patamares
        self.intercambioPatamares = self.patamar.intercambio_patamares
        self.usinasNaoSimuladasPatamares = self.patamar.usinas_nao_simuladas

        self.sistema = Sistema.read(self.caminho_TesteBase+"/sistema.dat")
        self.limitesInterc = self.sistema.limites_intercambio
        self.dger = Dger.read(self.caminho_TesteBase+"/dger.dat")
        mes_ini = self.dger.mes_inicio_estudo
        ano_ini = self.dger.ano_inicio_estudo
        self.series_sf = self.dger.num_series_sinteticas
        self.data_ini = pd.to_datetime(str(ano_ini)+"-"+str(mes_ini)+"-01")

        self.confhd = Confhd.read(self.caminho_TesteBase+"/confhd.dat")
        self.usinas = self.confhd.usinas
        lista_nome_usinas = self.usinas["nome_usina"].unique()
        self.df_hidr = Hidr.read(self.caminho_TesteBase+"/hidr.dat").cadastro
        self.pmo = Pmo.read(self.caminho_TesteBase+"/pmo.dat")
        self.vol_ini = self.pmo.volume_armazenado_inicial
        self.lista_Hliq_Calculado = []
        for nome_usina in lista_nome_usinas:
            usi =    self.usinas.loc[(self.usinas["nome_usina"] == nome_usina)]   
            cod_usi = usi["codigo_usina"].iloc[0]
            texto_arquivo = ""
            if(int(cod_usi) <= 9): texto_arquivo = "00"+str(cod_usi)
            elif(int(cod_usi) <= 99): texto_arquivo = "0"+str(cod_usi)
            elif(int(cod_usi) <= 999): texto_arquivo = str(cod_usi)

            self.df_usi = self.df_hidr.loc[(self.df_hidr["nome_usina"] == nome_usina)].reset_index(drop = True)
            arq  = self.caminho_TesteBase+"/hliq"+texto_arquivo+".out"
            self.hliq_nw = Hliq.read(arq).valores
            self.hliq_nw = self.hliq_nw.loc[self.hliq_nw["data"] >= self.data_ini].reset_index(drop = True)

            self.__gera_hliq_calculado(nome_usina)

            lista_NW = []
            lista_Erro = []
            for data in self.__hliq_calculado["data"].unique():
                for serie in self.__hliq_calculado["serie"].unique():
                    for pat in self.__hliq_calculado["patamar"].unique():
                        df_nw = self.hliq_nw.loc[(self.hliq_nw["data"] == data) & (self.hliq_nw["serie"] == serie) & (self.hliq_nw["patamar"] == pat)]["valor"].iloc[0]
                        df_calc = self.__hliq_calculado.loc[(self.__hliq_calculado["data"] == data) & (self.__hliq_calculado["serie"] == serie) & (self.__hliq_calculado["patamar"] == pat)]["Hliq_calc"].iloc[0]
                        lista_NW.append(df_nw)
                        lista_Erro.append(df_nw - df_calc)
            self.__hliq_calculado["NW"] = lista_NW
            self.__hliq_calculado["Erro"] =  lista_Erro
            self.lista_Hliq_Calculado.append(self.__hliq_calculado)
            self.__hliq_calculado.to_csv(self.caminho_testes_Hliq+"/"+"FT_HLIQ_"+nome_usina+".csv")
            self.__hliq_memoria_calculo.to_csv(self.caminho_testes_Hliq+"/"+"FT_MEM_HLIQ_"+nome_usina+".csv")
        self.hliqUsinas = pd.concat(self.lista_Hliq_Calculado)


    def __gera_hliq_calculado(self, nome_usina) -> pd.DataFrame:
        listaDF = []
        df_hliq_calculado = pd.DataFrame(columns=["usina", "data","patamar","serie", "Hliq_calc"])
        df_hliq_memoria = pd.DataFrame(columns=["usina", "data","patamar","serie", "Hmon_calc", "Hjus_calc", "perdas", "Hliq_calc"])
        perdas = self.df_usi["perdas"].iloc[0]
        for data in self.hliq_nw["data"].unique():
            hjus_usi_per = self.calcHjus.loc[(self.calcHjus["usina"] == nome_usina) & (self.calcHjus["data"] == data)]
            hmon_usi_per = self.calcHmon.loc[(self.calcHmon["usina"] == nome_usina) & (self.calcHmon["data"] == data)]
            for cen in self.hliq_nw["serie"].unique():
                hmon = hmon_usi_per.loc[(hmon_usi_per["serie"] == cen)]["Hmon_calc"].iloc[0]
                for pat in self.hliq_nw["patamar"].unique():
                    hjus = hjus_usi_per.loc[(hjus_usi_per["serie"] == cen) & (hjus_usi_per["patamar"] == pat) ]["Hjus_calc"].iloc[0]
                    hliq = hmon - hjus - perdas
                    hliq = round(hliq,2)
                    new_row = pd.DataFrame({"usina": nome_usina,
                                    "data": data,
                                    "patamar": pat,
                                    "serie": cen,
                                    "Hliq_calc":hliq},
                            index = [0])
                    new_row_memoria = pd.DataFrame({"usina": nome_usina,
                                    "data": data,
                                "patamar": pat,
                                    "serie": cen,
                                "Hmon_calc":hmon,
                                "Hjus_calc":hjus,
                                "perdas":perdas,
                                    "Hliq_calc":hliq},
                            index = [0])
                    df_hliq_calculado = pd.concat([df_hliq_calculado.loc[:],new_row]).reset_index(drop=True)
                    df_hliq_memoria = pd.concat([df_hliq_memoria.loc[:],new_row_memoria]).reset_index(drop=True)
        self.__hliq_calculado = df_hliq_calculado
        self.__hliq_memoria_calculo = df_hliq_memoria
        return 0
