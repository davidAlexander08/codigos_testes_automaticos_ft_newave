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
from inewave.libs import UsinasHidreletricas
from apps.avalia_fpha.indicadores import IndicadoresAvaliacaoFPHA

class Balanco_Hidrico:

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_balanco_hidrico = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Balanco_Hidrico"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_balanco_hidrico):
            os.makedirs(self.caminho_testes_balanco_hidrico)
            print(f"Folder '{self.caminho_testes_balanco_hidrico}' created!")
        else:
            print(f"Folder '{self.caminho_testes_balanco_hidrico}' already exists!")

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
        self.data_ini = pd.to_datetime(str(ano_ini)+"-"+str(mes_ini)+"-01")

        self.pmo = Pmo.read(self.caminho_TesteBase+"/pmo.dat")
        self.vol_ini = self.pmo.volume_armazenado_inicial
        print(self.vol_ini)
        self.confhd = Confhd.read(self.caminho_TesteBase+"/confhd.dat")
        self.usinas = self.confhd.usinas
        print(self.usinas)
        lista_nome_usinas = self.usinas["nome_usina"].unique()
        print(lista_nome_usinas)
        for nome_usina in lista_nome_usinas:
            print("REALIZANDO BALANCO DA USINA: ", nome_usina)
            usi =    self.usinas.loc[(self.usinas["nome_usina"] == nome_usina)]   
            cod_usi = usi["codigo_usina"].iloc[0]
            texto_arquivo = ""
            if(int(cod_usi) <= 9): texto_arquivo = "00"+str(cod_usi)
            elif(int(cod_usi) <= 99): texto_arquivo = "0"+str(cod_usi)
            elif(int(cod_usi) <= 999): texto_arquivo = str(cod_usi)

            self.__Varmi = self.vol_ini.loc[(self.vol_ini["nome_usina"] == nome_usina)]["valor_hm3"].iloc[0]

            arq_Varmuh  = self.caminho_TesteBase+"/varmuh"+texto_arquivo+".out"
            self.__Varmuh = Varmuh.read(arq_Varmuh).valores
            if(self.__Varmuh is not None):
                self.__Varmuh = self.__Varmuh.loc[self.__Varmuh["data"] >= self.data_ini]
            arq_Qturuh  = self.caminho_TesteBase+"/qturuh"+texto_arquivo+".out"
            self.__Qturuh = Qturuh.read(arq_Qturuh).valores
            self.__Qturuh = self.__Qturuh.loc[self.__Qturuh["data"] >= self.data_ini]
            arq_Qvertuh  = self.caminho_TesteBase+"/qvertuh"+texto_arquivo+".out"
            self.__Qvertuh = Qvertuh.read(arq_Qvertuh).valores
            self.__Qvertuh = self.__Qvertuh.loc[self.__Qvertuh["data"] >= self.data_ini]
            arq_Qafluh  = self.caminho_TesteBase+"/qafluh"+texto_arquivo+".out"
            self.__Qafluh = Varmuh.read(arq_Qafluh).valores
            self.__Qafluh = self.__Qafluh.loc[self.__Qafluh["data"] >= self.data_ini]
            arq_Qdesviouh  = self.caminho_TesteBase+"/qdesviouh"+texto_arquivo+".out"
            self.__Qdesviouh = Qdesviouh.read(arq_Qdesviouh).valores
            self.__Qdesviouh = self.__Qdesviouh.loc[self.__Qdesviouh["data"] >= self.data_ini]
            arq_Vretiradauh  = self.caminho_TesteBase+"/vretiradauh"+texto_arquivo+".out"
            self.__Vretiradauh = Vretiradauh.read(arq_Vretiradauh).valores
            if(self.__Vretiradauh is not None):
                self.__Vretiradauh = self.__Vretiradauh.loc[self.__Vretiradauh["data"] >= self.data_ini]
            arq_Vevapuh  = self.caminho_TesteBase+"/vevapuh"+texto_arquivo+".out"
            self.__Vevapuh = Vevapuh.read(arq_Vevapuh).valores
            self.__Vevapuh = self.__Vevapuh.fillna(0)
            self.__Vevapuh = self.__Vevapuh.loc[self.__Vevapuh["data"] >= self.data_ini]

            #print("__Varmuh ", self.__Varmuh)
            #print("__Qturuh ", self.__Qturuh)
            #print("__Qvertuh ", self.__Qvertuh)
            #print("__Qafluh ", self.__Qafluh)
            #print("__Qdesviouh ", self.__Qdesviouh)
            #print("__Vretiradauh ", self.__Vretiradauh)
            #print("__Vevapuh ", self.__Vevapuh)
            df_balanco_usina = self.__gera_balanco_hidraulico_usina(nome_usina)
            df_balanco_usina.to_csv(self.caminho_testes_balanco_hidrico+"/"+"FT_Balanco_Usina_"+nome_usina+".csv")
            
    def __gera_balanco_hidraulico_usina(self, nome_usina):
        lista_df = ["usina", "data", "cenario", "varmi",  "vafl","(-)vtur","(-)vver","(-)vret", "(-)vdes", "(-)vevap", "(-)varmf", "SOMA"]
        df_comparacao = pd.DataFrame(columns=lista_df)
        lista_data_frame = []
        for data in self.__Qturuh["data"].unique():       
            qtur_data = self.__Qturuh.loc[(self.__Qturuh["data"] == data) ]
            qver_data = self.__Qvertuh.loc[(self.__Qvertuh["data"] == data)   ]
            qafl_data = self.__Qafluh.loc[(self.__Qafluh["data"] == data) ]
            qdesvio_data = self.__Qdesviouh.loc[(self.__Qdesviouh["data"] == data) ]
            vevap_data = self.__Vevapuh.loc[(self.__Vevapuh["data"] == data) ]
            if(self.__Vretiradauh is not None):
                vretirada_data = self.__Vretiradauh.loc[(self.__Vretiradauh["data"] == data) ]
            else:
                vretirada_ser = 0
            if(self.__Varmuh is not None):
                varmf_data = self.__Varmuh.loc[(self.__Varmuh["data"] == data)  ]
            else:
                varmf_ser = 0
            for serie in self.__Qturuh["serie"].unique():
                qtur_ser =       qtur_data.loc[  (qtur_data["serie"] == serie)]
                qver_ser =    qver_data.loc[ (qver_data["serie"] == serie) ]
                vafl_ser =    qafl_data.loc[ (qafl_data["serie"] == serie) ]["valor"].iloc[0]*2.63
                qdesvio_ser =    qdesvio_data.loc[ (qdesvio_data["serie"] == serie) ]
                if(self.__Vretiradauh is not None):
                    vretirada_ser =    vretirada_data.loc[ (vretirada_data["serie"] == serie) ]["valor"].iloc[0]
                vevap_ser =    vevap_data.loc[ (vevap_data["serie"] == serie) ]["valor"].iloc[0]
                if(self.__Varmuh is not None):
                    varmf_ser =    varmf_data.loc[ (varmf_data["serie"] == serie) ]["valor"].iloc[0]
                    if(data == self.data_ini):
                        varmi_ser =  self.__Varmi
                    else:
                        varmi_ser =  self.__Varmuh.loc[(self.__Varmuh["data"] == data-pd.DateOffset(months=1))  & (self.__Varmuh["serie"] == serie) ]["valor"].iloc[0]
                else:
                    varmi_ser = 0

                qtur_pat =  0 
                qver_pat =  0 
                qdesvio_pat = 0
                for pat in self.__Qturuh["patamar"].unique():
                    dur_pat = self.duracaoMensalPatamares.loc[(self.duracaoMensalPatamares["data"] == data) & (self.duracaoMensalPatamares["patamar"] == int(pat))]["valor"].iloc[0]
                    qtur_pat +=       qtur_ser.loc[  (qtur_ser["patamar"] == pat)]["valor"].iloc[0]*dur_pat
                    qver_pat +=       qver_ser.loc[  (qver_ser["patamar"] == pat)]["valor"].iloc[0]*dur_pat
                    qdesvio_pat +=   qdesvio_ser.loc[ (qdesvio_ser["patamar"] == pat)]["valor"].iloc[0]*dur_pat

                vtur_pat = qtur_pat*2.63
                vver_pat = qver_pat*2.63
                vdesvio_pat = qdesvio_pat*2.63
                
                row_map = {}
                soma = -vtur_pat - vver_pat -vdesvio_pat - vretirada_ser -vevap_ser - varmf_ser + vafl_ser + varmi_ser
                row_map["data"] = data
                row_map["cenario"] = serie
                row_map["usina"] = nome_usina
                row_map["varmi"] =  varmi_ser
                row_map["vafl"] = vafl_ser
                row_map["(-)vtur"] = -vtur_pat
                row_map["(-)vver"] =  -vver_pat
                row_map["(-)vret"] = -vretirada_ser
                row_map["(-)vdes"] = -vdesvio_pat
                row_map["(-)vevap"] = -vevap_ser
                row_map["(-)varmf"] = -varmf_ser
                row_map["SOMA"] = soma
                df_comparacao = pd.concat([df_comparacao, pd.DataFrame([row_map])], ignore_index=True)
#
        return df_comparacao
#