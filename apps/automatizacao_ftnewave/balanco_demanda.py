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

class Balanco_Demanda:

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_balanco_demanda = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Balanco_Demanda"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_balanco_demanda):
            os.makedirs(self.caminho_testes_balanco_demanda)
            print(f"Folder '{self.caminho_testes_balanco_demanda}' created!")
        else:
            print(f"Folder '{self.caminho_testes_balanco_demanda}' already exists!")

        self.patamar = Patamar.read(self.caminho_TesteBase+"/patamar.dat")
        self.numeroPatamares = Patamar.numero_patamares
        self.duracaoMensalPatamares = Patamar.duracao_mensal_patamares
        self.cargaPatamares = Patamar.carga_patamares
        self.intercambioPatamares = Patamar.intercambio_patamares
        self.usinasNaoSimuladasPatamares = Patamar.usinas_nao_simuladas


        self.patamar = Patamar.read(self.caminho_TesteBase+"/patamar.dat")

        self.sistema = Sistema.read(self.caminho_TesteBase+"/sistema.dat")
        #NUMERO SUBMERCADOS
        self.limitesInterc = self.sistema.limites_intercambio
        self.codigoSubmercados = list(set(list(self.limitesInterc["submercado_de"].unique()) + list(self.limitesInterc["submercado_para"].unique())))
        for codigoSubmercado in self.codigoSubmercados:
            print("REALIZANDO BALANCO DE DEMANDA DO SUBMERCADO: ", codigoSubmercado)
            texto_arquivo = ""
            if(int(codigoSubmercado) <= 9): texto_arquivo = "00"+str(codigoSubmercado)
            elif(int(codigoSubmercado) <= 99): texto_arquivo = "0"+str(codigoSubmercado)
            elif(int(codigoSubmercado) <= 999): texto_arquivo = str(codigoSubmercado)
            if(codigoSubmercado != 11):
                self.le_variaveis_balanco_demanda_SBM(codigoSubmercado, texto_arquivo)
                df_balanco_demanda_SBM = self.__gera_balanco_demanda_SBM(codigoSubmercado)
                df_balanco_demanda_SBM.to_csv(self.caminho_testes_balanco_demanda+"/"+"FT_Balanco_Demanda_SBM_"+texto_arquivo+".csv")

        print("REALIZANDO BALANCO DE DEMANDA DO SIN")
        self.le_variaveis_balanco_demanda_SIN()
        df_balanco_demanda_SIN = self.__gera_balanco_demanda_SIN()
        df_balanco_demanda_SIN.to_csv(self.caminho_testes_balanco_demanda+"/"+"FT_Balanco_Demanda_SIN.csv")

    def load_and_filter(self, file, reader, filter_col="patamar", filter_val="TOTAL"):
        if os.path.isfile(file):
            df = reader.read(file).valores
            return df.loc[df[filter_col] != filter_val].reset_index(drop=True) if filter_col in df.columns else df
        return None

    def le_variaveis_balanco_demanda_SBM(self, codigoSubmercado,texto_arquivo):

        arq_gt  = self.caminho_TesteBase+"/gttot"+texto_arquivo+".out"
        arq_gh = self.caminho_TesteBase+"/ghtotm"+texto_arquivo+".out"
        arq_mercl = self.caminho_TesteBase+"/mercl"+texto_arquivo+".out"
        arq_def = self.caminho_TesteBase+"/def"+texto_arquivo+"p001.out"
        arq_exc = self.caminho_TesteBase+"/exces"+texto_arquivo+".out"
        self.__gt_SBM = self.load_and_filter(arq_gt, Gttot)
        self.__gh_SBM = self.load_and_filter(arq_gh, Ghtot)
        self.__mercL_SBM = self.load_and_filter(arq_mercl, Mercl)
        self.__def_SBM = self.load_and_filter(arq_def, Def)
        self.__exc_SBM = self.load_and_filter(arq_exc, Exces)

        self.__mapa_df_intercambios = {}
        for codigoSegundoSubmercado in self.codigoSubmercados:
            texto_segundo_interc = ""
            if(int(codigoSegundoSubmercado) <= 9): texto_segundo_interc = "00"+str(codigoSegundoSubmercado)
            elif(int(codigoSegundoSubmercado) <= 99): texto_segundo_interc = "0"+str(codigoSegundoSubmercado)
            elif(int(codigoSegundoSubmercado) <= 999): texto_segundo_interc = str(codigoSegundoSubmercado)
            if os.path.isfile(self.caminho_TesteBase+"/int"+texto_arquivo+texto_segundo_interc+".out"):
                df = Intercambio.read(self.caminho_TesteBase+"/int"+texto_arquivo+texto_segundo_interc+".out").valores
                df = df.loc[(df["patamar"] != "TOTAL")].reset_index(drop = True)
                self.__mapa_df_intercambios[(texto_arquivo+"_"+texto_segundo_interc)] = df
            elif os.path.isfile(self.caminho_TesteBase+"/int"+texto_segundo_interc+texto_arquivo+".out"):
                df = Intercambio.read(self.caminho_TesteBase+"/int"+texto_segundo_interc+texto_arquivo+".out").valores
                df = df.loc[(df["patamar"] != "TOTAL")].reset_index(drop = True)
                df["valor"] = -df["valor"]
                self.__mapa_df_intercambios[(texto_arquivo+"_"+texto_segundo_interc)] = df


    def __gera_balanco_demanda_SBM(self, codigo_sbm):
        lista_df = ["data", "cenario", "submercado", "MERCL", "(-)GT", "(-)GH", "(-)DEF", "EXC"]
        for par in self.__mapa_df_intercambios.keys():
            lista_df.append(par)
        lista_df.append("TOTAL_INTERC")
        lista_df.append("SOMA")
        df_comparacao = pd.DataFrame(columns=lista_df)
        lista_data_frame = []
        
        for data in self.__gt_SBM["data"].unique():
            
            mercl = self.__mercL_SBM.loc[(self.__mercL_SBM["data"] == data)  ]["valor"].iloc[0]
            gter_data = self.__gt_SBM.loc[(self.__gt_SBM["data"] == data) ]
            ghid_data = self.__gh_SBM.loc[(self.__gh_SBM["data"] == data)   ]
            deficit_data = self.__def_SBM.loc[(self.__def_SBM["data"] == data) ]
            excesso_data = self.__exc_SBM.loc[(self.__exc_SBM["data"] == data) ]
            for serie in self.__gt_SBM["serie"].unique():
                gter_ser =       gter_data.loc[  (gter_data["serie"] == serie) ]
                ghid_ser =       ghid_data.loc[  (ghid_data["serie"] == serie)]
                deficit_ser =    deficit_data.loc[ (deficit_data["serie"] == serie) ]
                excesso_ser =    excesso_data.loc[ (excesso_data["serie"] == serie) ]
                gter_pat =  0 
                ghid_pat =  0 
                deficit_pat = 0
                excesso_pat = 0
                row_map = {}
                for pat in self.__gt_SBM["patamar"].unique():
                    gter_pat +=       gter_ser.loc[  (gter_ser["patamar"] == pat)]["valor"].iloc[0]
                    ghid_pat +=       ghid_ser.loc[  (ghid_ser["patamar"] == pat)]["valor"].iloc[0]
                    deficit_pat +=   deficit_ser.loc[ (deficit_ser["patamar"] == pat)]["valor"].iloc[0]
                    excesso_pat +=   excesso_ser.loc[ (excesso_ser["patamar"] == pat)]["valor"].iloc[0]
                total_interc = 0
                for par in self.__mapa_df_intercambios.keys():
                    df = self.__mapa_df_intercambios[par]
                    df = df.loc[(df["data"] == data) & (df["serie"] == serie)]
                    row_map[par] = df["valor"].sum()
                    total_interc += row_map[par]
                soma = -gter_pat - ghid_pat - deficit_pat +mercl + excesso_pat + total_interc
                row_map["data"] = data
                row_map["cenario"] = serie
                row_map["submercado"] = codigo_sbm
                row_map["MERCL"] =  mercl
                row_map["(-)GT"] = -gter_pat
                row_map["(-)GH"] = -ghid_pat
                row_map["(-)DEF"] =  -deficit_pat
                row_map["EXC"] = excesso_pat
                row_map["TOTAL_INTERC"] = total_interc
                row_map["SOMA"] =soma
                df_comparacao = pd.concat([df_comparacao, pd.DataFrame([row_map])], ignore_index=True)

        return df_comparacao


    

    def le_variaveis_balanco_demanda_SIN(self):
        arq_gt = self.caminho_TesteBase+"/gttotsin.out"
        arq_gh = self.caminho_TesteBase+"/ghtotsin.out"
        arq_mercl = self.caminho_TesteBase+"/merclsin.out"
        arq_def = self.caminho_TesteBase+"/defsinp001.out"
        arq_exc = self.caminho_TesteBase+"/excessin.out"
        self.__gt_SIN = self.load_and_filter(arq_gt,Gttotsin )
        self.__gh_SIN = self.load_and_filter(arq_gh,Ghtotsin )
        self.__mercL_SIN = self.load_and_filter(arq_mercl,Merclsin )
        self.__def_SIN = self.load_and_filter(arq_def,Defsin )
        self.__exc_SIN = self.load_and_filter(arq_exc,Excessin )




    def __gera_balanco_demanda_SIN(self):
        df_comparacao = pd.DataFrame(columns=["data", "cenario", "patamar", "MERCL", "(-)GT", "(-)GH", "(-)DEF", "EXC" ])
        for data in self.__gt_SIN["data"].unique():
            mercl = self.__mercL_SIN.loc[(self.__mercL_SIN["data"] == data)  ]["valor"].iloc[0]
            gter_data = self.__gt_SIN.loc[(self.__gt_SIN["data"] == data) ]
            ghid_data = self.__gh_SIN.loc[(self.__gh_SIN["data"] == data)   ]
            deficit_data = self.__def_SIN.loc[(self.__def_SIN["data"] == data) ]
            excesso_data = self.__exc_SIN.loc[(self.__exc_SIN["data"] == data) ]
            for serie in self.__gt_SIN["serie"].unique():
                gter_ser =       gter_data.loc[  (gter_data["serie"] == serie) ]
                ghid_ser =       ghid_data.loc[  (ghid_data["serie"] == serie)]
                deficit_ser =    deficit_data.loc[ (deficit_data["serie"] == serie) ]
                excesso_ser =    excesso_data.loc[ (excesso_data["serie"] == serie) ]
                gter_pat =  0 
                ghid_pat =  0 
                deficit_pat = 0
                excesso_pat = 0
                for pat in self.__gt_SIN["patamar"].unique():
                    gter_pat +=       gter_ser.loc[  (gter_ser["patamar"] == pat)]["valor"].iloc[0]
                    ghid_pat +=       ghid_ser.loc[  (ghid_ser["patamar"] == pat)]["valor"].iloc[0]
                    deficit_pat +=   deficit_ser.loc[ (deficit_ser["patamar"] == pat)]["valor"].iloc[0]
                    excesso_pat +=   excesso_ser.loc[ (excesso_ser["patamar"] == pat)]["valor"].iloc[0]
                soma = gter_pat + ghid_pat + deficit_pat -mercl - excesso_pat
                new_row = pd.DataFrame({"data": data,
                                        "cenario": serie, 
                                        "MERCL": mercl,
                                        "(-)GT": -gter_pat,
                                        "(-)GH": -ghid_pat,
                                        "(-)DEF": -deficit_pat,
                                        "EXC": excesso_pat,
                                        "SOMA": soma
                                    },
                                    index = [0])
        
                df_comparacao = pd.concat([df_comparacao.loc[:],new_row]).reset_index(drop=True)
        return df_comparacao











        #self.__gt_SIN = Gttotsin.read(self.caminho_TesteBase+"/gttotsin.out").valores
        #self.__gt_SIN = self.__gt_SIN.loc[(self.__gt_SIN["patamar"] != "TOTAL")].reset_index(drop = True)
        #self.__gh_SIN = Ghtotsin.read(self.caminho_TesteBase+"/ghtotsin.out").valores
        #self.__gh_SIN = self.__gh_SIN.loc[(self.__gh_SIN["patamar"] != "TOTAL")].reset_index(drop = True)
        #self.__mercL_SIN = Merclsin.read(self.caminho_TesteBase+"/merclsin.out").valores
        #self.__def_SIN = Defsin.read(self.caminho_TesteBase+"/defsinp001.out").valores
        #self.__def_SIN = self.__def_SIN.loc[(self.__def_SIN["patamar"] != "TOTAL")].reset_index(drop = True)
        #self.__exc_SIN = Excessin.read(self.caminho_TesteBase+"/excessin.out").valores
        #self.__exc_SIN = self.__exc_SIN.loc[(self.__exc_SIN["patamar"] != "TOTAL")].reset_index(drop = True)