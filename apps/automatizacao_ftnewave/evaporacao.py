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

class Evaporacao:

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_evaporacao = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Evaporacao"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_evaporacao):
            os.makedirs(self.caminho_testes_evaporacao)
            print(f"Folder '{self.caminho_testes_evaporacao}' created!")
        else:
            print(f"Folder '{self.caminho_testes_evaporacao}' already exists!")

        self.patamar = Patamar.read(self.caminho_TesteBase+"/patamar.dat")
        self.numeroPatamares = Patamar.numero_patamares
        self.duracaoMensalPatamares = Patamar.duracao_mensal_patamares
        self.cargaPatamares = Patamar.carga_patamares
        self.intercambioPatamares = Patamar.intercambio_patamares
        self.usinasNaoSimuladasPatamares = Patamar.usinas_nao_simuladas
        self.patamar = Patamar.read(self.caminho_TesteBase+"/patamar.dat")
        self.sistema = Sistema.read(self.caminho_TesteBase+"/sistema.dat")
        self.confhd = Confhd.read(self.caminho_TesteBase+"/confhd.dat")
        self.dger = Dger.read(self.caminho_TesteBase+"/dger.dat")
        periodo_proximo_mes = 12 - (self.dger.mes_inicio_estudo -1) + 1
        print(periodo_proximo_mes)
        self.usinas = self.confhd.usinas
        self.mapaMesNumero = {
            1:"JAN",
            2:"FEV",
            3:"MAR",
            4:"ABR",
            5:"MAI",
            6:"JUN",
            7:"JUL",
            8:"AGO",
            9:"SET",
            10:"OUT",
            11:"NOV",
            12:"DEZ"
        }
        
        lista_nome_usinas = self.usinas["nome_usina"].unique()

        self.df_hidr = Hidr.read(self.caminho_TesteBase+"/hidr.dat").cadastro
        self.df_hidr.to_csv(self.caminho_FTNEWAVE+"/eco_hidr.csv")
        df_VolrefSaz = VolrefSaz.read(self.caminho_TesteBase+"/volref_saz.dat").volumes
        df_NwvEcoEvap = NwvEcoEvap.read(self.caminho_TesteBase+"/evap_eco.csv").tabela
        df_NwvCortesEvap = NwvCortesEvap.read(self.caminho_TesteBase+"/evap_cortes.csv").tabela
        numero_elementos_pol_cota_area = 5 
        numero_elementos_pol_cota_area = 12
        for usi in lista_nome_usinas:
            self.df_usi = self.df_hidr.loc[(self.df_hidr["nome_usina"] == usi)].reset_index(drop = True)

            df_eco_evap = df_NwvEcoEvap.loc[(df_NwvEcoEvap["nome_usina"] == usi)].reset_index(drop = True)
            df_cortes_evap = df_NwvCortesEvap.loc[(df_NwvCortesEvap["nome_usina"] == usi)].reset_index(drop = True)

            self.mapa_coef_evap_usi = {}
            self.mapa_coef_evap_usi["JAN"] = self.df_usi["evaporacao_JAN"].iloc[0]
            self.mapa_coef_evap_usi["FEV"] = self.df_usi["evaporacao_FEV"].iloc[0]
            self.mapa_coef_evap_usi["MAR"] = self.df_usi["evaporacao_MAR"].iloc[0]
            self.mapa_coef_evap_usi["ABR"] = self.df_usi["evaporacao_ABR"].iloc[0]
            self.mapa_coef_evap_usi["MAI"] = self.df_usi["evaporacao_MAI"].iloc[0]
            self.mapa_coef_evap_usi["JUN"] = self.df_usi["evaporacao_JUN"].iloc[0]
            self.mapa_coef_evap_usi["JUL"] = self.df_usi["evaporacao_JUL"].iloc[0]
            self.mapa_coef_evap_usi["AGO"] = self.df_usi["evaporacao_AGO"].iloc[0]
            self.mapa_coef_evap_usi["SET"] = self.df_usi["evaporacao_SET"].iloc[0]
            self.mapa_coef_evap_usi["OUT"] = self.df_usi["evaporacao_OUT"].iloc[0]
            self.mapa_coef_evap_usi["NOV"] = self.df_usi["evaporacao_NOV"].iloc[0]
            self.mapa_coef_evap_usi["DEZ"] = self.df_usi["evaporacao_DEZ"].iloc[0]

            self.mapa_vol_cota = {}
            self.vol_cota_A0 = self.df_usi["a0_volume_cota"].iloc[0]
            self.vol_cota_A1 = self.df_usi["a1_volume_cota"].iloc[0]
            self.vol_cota_A2 = self.df_usi["a2_volume_cota"].iloc[0]
            self.vol_cota_A3 = self.df_usi["a3_volume_cota"].iloc[0]
            self.vol_cota_A4 = self.df_usi["a4_volume_cota"].iloc[0]
            self.A0_Cotare = self.df_usi["a0_cota_area"].iloc[0]
            self.A1_Cotare = self.df_usi["a1_cota_area"].iloc[0]
            self.A2_Cotare = self.df_usi["a2_cota_area"].iloc[0]
            self.A3_Cotare = self.df_usi["a3_cota_area"].iloc[0]
            self.A4_Cotare = self.df_usi["a4_cota_area"].iloc[0]

            self.volume_minimo = self.df_usi["volume_minimo"].iloc[0]
            volref_saz_usi = df_VolrefSaz.loc[(df_VolrefSaz["nome_usina"]== usi)].reset_index(drop = True)
            volref_saz_usi["valor"] = volref_saz_usi["valor"] + self.volume_minimo
            self.df_memo_evap_ref = pd.DataFrame()
            self.df_evap_ref = pd.DataFrame()   
            self.df_evap_ref = volref_saz_usi
            self.df_evap_ref = self.df_evap_ref.drop(columns = ["valor"])
            self.df_evap_ref["vol_ref"] = volref_saz_usi["valor"]
            self.__calcula_df_evap_ref(usi, volref_saz_usi)

            self.df_calc_derivadas = pd.DataFrame()
            self.df_calc_derivadas["mes"] = self.df_evap_ref["mes"]
            self.df_memo_derivadas = self.df_calc_derivadas.copy()
            self.__calcula_derivada_polinomios()

            self.df_evap_ref.to_csv(self.caminho_testes_evaporacao+"/df_evap_ref"+usi+".csv")
            self.df_calc_derivadas.to_csv(self.caminho_testes_evaporacao+"/df_calc_derivadas"+usi+".csv")

    def __calcula_df_evap_ref(self, usi, volref_saz_usi) -> pd.DataFrame:

        self.df_memo_evap_ref = volref_saz_usi.copy()
        self.df_memo_evap_ref["vol_cota_A0"] = self.vol_cota_A0
        self.df_memo_evap_ref["vol_cota_A1"] = self.vol_cota_A1
        self.df_memo_evap_ref["vol_cota_A2"] = self.vol_cota_A2
        self.df_memo_evap_ref["vol_cota_A3"] = self.vol_cota_A3
        self.df_memo_evap_ref["vol_cota_A4"] = self.vol_cota_A4

        for index, row in volref_saz_usi.iterrows():
            hmon_ref  = self.vol_cota_A0*(row["valor"]**0) + self.vol_cota_A1*(row["valor"]**1) + self.vol_cota_A2*(row["valor"]**2) + self.vol_cota_A3*(row["valor"]**3) + self.vol_cota_A4*(row["valor"]**4)
            self.df_evap_ref.at[index, "hmon_ref"] = hmon_ref
            self.df_memo_evap_ref.at[index, "hmon_ref"] = hmon_ref

        self.df_memo_evap_ref["cota_area_A0"] = self.A0_Cotare
        self.df_memo_evap_ref["cota_area_A1"] = self.A1_Cotare
        self.df_memo_evap_ref["cota_area_A2"] = self.A2_Cotare
        self.df_memo_evap_ref["cota_area_A3"] = self.A3_Cotare
        self.df_memo_evap_ref["cota_area_A4"] = self.A4_Cotare

        for index, row in self.df_evap_ref.iterrows():
            area = self.A0_Cotare*(row["hmon_ref"]**0) + self.A1_Cotare*(row["hmon_ref"]**1) + self.A2_Cotare*(row["hmon_ref"]**2) + self.A3_Cotare*(row["hmon_ref"]**3) + self.A4_Cotare*(row["hmon_ref"]**4)
            self.df_evap_ref.at[index, "area_ref"] = area
            self.df_memo_evap_ref.at[index, "area_ref"] = area

        for index, row in self.df_evap_ref.iterrows():
            evap_referencia = (row["area_ref"]*(self.mapa_coef_evap_usi[self.mapaMesNumero[row["mes"]]]/(10**6)))*(10**3)
            self.df_evap_ref.at[index, "coef_evap"] = self.mapa_coef_evap_usi[self.mapaMesNumero[row["mes"]]]
            self.df_evap_ref.at[index, "evap_ref"] = evap_referencia
            self.df_memo_evap_ref.at[index, "coef_evap"] = self.mapa_coef_evap_usi[self.mapaMesNumero[row["mes"]]]
            self.df_memo_evap_ref.at[index, "evap_ref"] = evap_referencia
        self.df_evap_ref = self.df_evap_ref.reset_index(drop = True)
        self.df_memo_evap_ref = self.df_memo_evap_ref.reset_index(drop = True)
    
    def __calcula_derivada_polinomios(self):
        self.df_memo_derivadas["derivada_A1_cotvol"] = self.vol_cota_A1*1
        self.df_memo_derivadas["derivada_A2_cotvol"] = self.vol_cota_A2*2
        self.df_memo_derivadas["derivada_A3_cotvol"] = self.vol_cota_A3*3
        self.df_memo_derivadas["derivada_A4_cotvol"] = self.vol_cota_A4*4
        self.df_memo_derivadas["derivada_A1_cotarea"] = self.A1_Cotare*1
        self.df_memo_derivadas["derivada_A2_cotarea"] = self.A2_Cotare*2
        self.df_memo_derivadas["derivada_A3_cotarea"] = self.A3_Cotare*3
        self.df_memo_derivadas["derivada_A4_cotarea"] = self.A4_Cotare*4
        for index, row in self.df_evap_ref.iterrows():
            derivada_vol = self.vol_cota_A1*(row["vol_ref"]**0) + self.vol_cota_A2*2*(row["vol_ref"]**1) + self.vol_cota_A3*3*(row["vol_ref"]**2) + self.vol_cota_A4*4*(row["vol_ref"]**3) 
            derivada_hmon = self.A1_Cotare*(row["hmon_ref"]**0) + self.A2_Cotare*2*(row["hmon_ref"]**1) + self.A3_Cotare*3*(row["hmon_ref"]**2) + self.A4_Cotare*4*(row["hmon_ref"]**3) 
            self.df_calc_derivadas.at[index, "derivada_vol"] = derivada_vol
            self.df_calc_derivadas.at[index, "derivada_hmon"] = derivada_hmon
            reta_coef_vol = (derivada_hmon*derivada_vol*row["coef_evap"])/(1000) 
            self.df_calc_derivadas.at[index, "reta coef vol"] = reta_coef_vol/2    # DIVIDO POR 2 PORQUE O ECO DO NEWAVE ESTA ERRADO, DEPOIS QUANDO NEWAVE CORRIGIR O ECO, TIRAR A DIVISAO POR 2
            self.df_calc_derivadas.at[index, "rhs"] = (row["evap_ref"] - reta_coef_vol*(row["vol_ref"]-self.volume_minimo))

            self.df_memo_derivadas.at[index,"vol_ref"] = row["vol_ref"]
            self.df_memo_derivadas.at[index,"hmon_ref"] = row["hmon_ref"]
            self.df_memo_derivadas.at[index, "derivada_vol"] = derivada_vol
            self.df_memo_derivadas.at[index, "derivada_hmon"] = derivada_hmon
            self.df_memo_derivadas.at[index, "reta coef vol"] = reta_coef_vol/2    # DIVIDO POR 2 PORQUE O ECO DO NEWAVE ESTA ERRADO, DEPOIS QUANDO NEWAVE CORRIGIR O ECO, TIRAR A DIVISAO POR 2
            self.df_memo_derivadas.at[index, "rhs"] = (row["evap_ref"] - reta_coef_vol*(row["vol_ref"]-self.volume_minimo))

        self.df_calc_derivadas = self.df_calc_derivadas.reset_index(drop = True)
        self.df_memo_derivadas = self.df_memo_derivadas.reset_index(drop = True)