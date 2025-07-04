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
from inewave.newave import Modif
from inewave.libs import UsinasHidreletricas
from apps.avalia_fpha.indicadores import IndicadoresAvaliacaoFPHA
class Hmon():

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_hmon = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Altura/Hmon"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_hmon):
            os.makedirs(self.caminho_testes_hmon)
            print(f"Folder '{self.caminho_testes_hmon}' created!")
        else:
            print(f"Folder '{self.caminho_testes_hmon}' already exists!")

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
        self.lista_Hmon_Calculado = []
        for nome_usina in lista_nome_usinas:
            print("CALCULANDO HMON USINA: ", nome_usina)
            usi =    self.usinas.loc[(self.usinas["nome_usina"] == nome_usina)]   
            cod_usi = usi["codigo_usina"].iloc[0]
            texto_arquivo = ""
            if(int(cod_usi) <= 9): texto_arquivo = "00"+str(cod_usi)
            elif(int(cod_usi) <= 99): texto_arquivo = "0"+str(cod_usi)
            elif(int(cod_usi) <= 999): texto_arquivo = str(cod_usi)

            self.df_usi = self.df_hidr.loc[(self.df_hidr["nome_usina"] == nome_usina)].reset_index(drop = True)
            self.vol_cota_A0 = self.df_usi["a0_volume_cota"].iloc[0]
            self.vol_cota_A1 = self.df_usi["a1_volume_cota"].iloc[0]
            self.vol_cota_A2 = self.df_usi["a2_volume_cota"].iloc[0]
            self.vol_cota_A3 = self.df_usi["a3_volume_cota"].iloc[0]
            self.vol_cota_A4 = self.df_usi["a4_volume_cota"].iloc[0]
            self.volume_minimo = self.df_usi["volume_minimo"].iloc[0]

            arq_Qturuh  = self.caminho_TesteBase+"/qturuh"+texto_arquivo+".out"
            self.__Qturuh = Qturuh.read(arq_Qturuh).valores
            self.__Qturuh = self.__Qturuh.loc[self.__Qturuh["data"] >= self.data_ini].reset_index(drop = True)
            self.series = list(self.__Qturuh["serie"].unique())
            arq_Varmuh  = self.caminho_TesteBase+"/varmuh"+texto_arquivo+".out"
            self.__Varmuh = Varmuh.read(arq_Varmuh).valores
            if(self.__Varmuh is not None):
                self.__Varmuh = self.__Varmuh.loc[self.__Varmuh["data"] >= self.data_ini].reset_index(drop = True)
            self.__Varmi = self.vol_ini.loc[(self.vol_ini["nome_usina"] == nome_usina)]["valor_hm3"].iloc[0]

            self.__calcula_df_altura_montante(nome_usina)

            arq_Hmon  = self.caminho_TesteBase+"/hmont"+texto_arquivo+".out"
            self.hmon_nw = Hmont.read(arq_Hmon).valores
            self.hmon_nw = self.hmon_nw.loc[self.hmon_nw["data"] >= self.data_ini].reset_index(drop = True)
            lista_NW = []
            lista_Erro = []
            for data in self.__hmon_calculado["data"].unique():
                for serie in self.__hmon_calculado["serie"].unique():
                    df_nw = self.hmon_nw.loc[(self.hmon_nw["data"] == data) & (self.hmon_nw["serie"] == serie)]["valor"].iloc[0]
                    df_calc = self.__hmon_calculado.loc[(self.__hmon_calculado["data"] == data) & (self.__hmon_calculado["serie"] == serie)]["Hmon_calc"].iloc[0]
                    lista_NW.append(df_nw)
                    lista_Erro.append(df_nw - df_calc)
            

            self.__hmon_calculado["NW"] = lista_NW
            self.__hmon_calculado["Erro"] =  lista_Erro
            self.lista_Hmon_Calculado.append(self.__hmon_calculado)
            self.__hmon_calculado.to_csv(self.caminho_testes_hmon+"/"+"FT_HMON_"+nome_usina+".csv")
        self.hmonUsinas = pd.concat(self.lista_Hmon_Calculado)


    def __calcula_df_altura_montante(self, nome_usina):
        df_hmon_resultado = pd.DataFrame(columns=["usina", "data","serie", "Hmon_calc"])
        df_hmon_memoria = pd.DataFrame(columns=["usina", "data", "serie", "vf", "vi", "v_med", "vmin", "A0", "A1", "A2", "A3", "A4", "Hmon_calc" ])
        df_temp = pd.DataFrame()
        for data in self.__Qturuh["data"].unique():  
            if(self.__Varmuh is not None):
                varmf_data = self.__Varmuh.loc[(self.__Varmuh["data"] == data)  ]
            else:
                varmf_ser = 0
            for cen in self.series:
                if(self.__Varmuh is not None):
                    varmf_ser =    varmf_data.loc[ (varmf_data["serie"] == cen) ]["valor"].iloc[0]
                    if(data == self.data_ini):
                        varmi_ser =  self.__Varmi
                    else:
                        varmi_ser =  self.__Varmuh.loc[(self.__Varmuh["data"] == data-pd.DateOffset(months=1))  & (self.__Varmuh["serie"] == cen) ]["valor"].iloc[0]
                else:
                    varmi_ser = 0
                v_med = ((varmf_ser+varmi_ser)/2) + self.volume_minimo
                h_mon = (v_med**0)*self.vol_cota_A0 + (v_med**1)*self.vol_cota_A1 + (v_med**2)*self.vol_cota_A2 + (v_med**3)*self.vol_cota_A3 + (v_med**4)*self.vol_cota_A4
                
                h_mon = round(h_mon,2)
                new_row = pd.DataFrame({
                                "usina": nome_usina,
                                "data": data,
                                "serie": cen,
                                "Hmon_calc":h_mon},
                        index = [0])
                new_row_memoria = pd.DataFrame({
                                "usina": nome_usina,
                                "data": data,
                                "serie": cen,
                                "vf": varmf_ser,
                                "vi": varmi_ser,
                                "v_med": v_med,
                                "vmin": self.volume_minimo,
                                "A0" : self.vol_cota_A0, 
                                "A1" : self.vol_cota_A1, 
                                "A2" : self.vol_cota_A2, 
                                "A3" : self.vol_cota_A3, 
                                "A4" : self.vol_cota_A4, 
                                "Hmon_calc":h_mon},
                        index = [0])
                df_hmon_resultado = pd.concat([df_hmon_resultado.loc[:],new_row]).reset_index(drop=True)
                df_hmon_memoria = pd.concat([df_hmon_memoria.loc[:],new_row_memoria]).reset_index(drop=True)
#
        self.__hmon_calculado = df_hmon_resultado
        self.__hmon_memoria_calculo = df_hmon_memoria
        return 0