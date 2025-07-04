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
from inewave.nwlistop import Hjus
from inewave.libs import UsinasHidreletricas
from apps.automatizacao_ftnewave.hmon import Hmon
class FT_Hjus():

    def __init__(self, caminho_deck_ftnewave):

        obj_Hmon = Hmon(caminho_deck_ftnewave)
        self.calcHmon = obj_Hmon.hmonUsinas


        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_hjus = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Altura/Hjus"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_hjus):
            os.makedirs(self.caminho_testes_hjus)
            print(f"Folder '{self.caminho_testes_hjus}' created!")
        else:
            print(f"Folder '{self.caminho_testes_hjus}' already exists!")

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
        self.df_hidr = self.df_hidr.reset_index(drop = False)
        self.pmo = Pmo.read(self.caminho_TesteBase+"/pmo.dat")
        self.vol_ini = self.pmo.volume_armazenado_inicial
        self.lista_Hjus_Calculado = []
        for nome_usina in lista_nome_usinas:
            usi =    self.usinas.loc[(self.usinas["nome_usina"] == nome_usina)]   
            cod_usi = usi["codigo_usina"].iloc[0]
            texto_arquivo = ""
            if(int(cod_usi) <= 9): texto_arquivo = "00"+str(cod_usi)
            elif(int(cod_usi) <= 99): texto_arquivo = "0"+str(cod_usi)
            elif(int(cod_usi) <= 999): texto_arquivo = str(cod_usi)

            self.df_usi = self.df_hidr.loc[(self.df_hidr["nome_usina"] == nome_usina)].reset_index(drop = True)
            self.codigo_usi = self.df_usi["codigo_usina"].iloc[0]
            df_confhd_usi = self.usinas.loc[(self.usinas["nome_usina"] == nome_usina)].reset_index(drop = True)
            codigo_usi_jusante = df_confhd_usi["codigo_usina_jusante"].iloc[0]
            if(codigo_usi_jusante != 0):
                nome_usina_jusante = self.usinas.loc[(self.usinas["codigo_usina"] == codigo_usi_jusante)]["nome_usina"].iloc[0]
                
            else:
                nome_usina_jusante = "0"
            caminho  = "/".join( (self.caminho_TesteBase,"polinjus.csv") )
            
            df_polinjus = UsinasHidreletricas.read(caminho)
            self.df_eco_curvajusante_polinomio = ( df_polinjus.hidreletrica_curvajusante_polinomio(df=True) )
            self.df_eco_curvajusante = df_polinjus.hidreletrica_curvajusante(df=True)
            self.df_eco_curvajusante_polinomio_segmento  = df_polinjus.hidreletrica_curvajusante_polinomio_segmento(df=True)
            self.df_curvajusante_afogamentoexplicito_usina = ( df_polinjus.hidreletrica_curvajusante_afogamentoexplicito_usina( df=True) )


            arq_Hjus  = self.caminho_TesteBase+"/hjus"+texto_arquivo+".out"
            self.hjus_nw = Hjus.read(arq_Hjus).valores
            self.hjus_nw = self.hjus_nw.loc[self.hjus_nw["data"] >= self.data_ini].reset_index(drop = True)

            arq_Qturuh  = self.caminho_TesteBase+"/qturuh"+texto_arquivo+".out"
            self.__Qturuh = Qturuh.read(arq_Qturuh).valores
            self.__Qturuh = self.__Qturuh.loc[self.__Qturuh["data"] >= self.data_ini].reset_index(drop = True)
            arq_Qvertuh  = self.caminho_TesteBase+"/qvertuh"+texto_arquivo+".out"
            self.__Qvertuh = Qvertuh.read(arq_Qvertuh).valores
            self.__Qvertuh = self.__Qvertuh.loc[self.__Qvertuh["data"] >= self.data_ini].reset_index(drop = True)

            self.__Qdefuh = self.__Qturuh
            vertfuga = int(self.df_usi["influencia_vertimento_canal_fuga"].iloc[0])
            if vertfuga == 1:
                self.__Qdefuh["valor"] = self.__Qdefuh["valor"] + self.__Qvertuh["valor"]
            self.__calcula_df_altura_jusante(nome_usina, nome_usina_jusante)
            

            lista_NW = []
            lista_Erro = []
            for data in self.__hjus_calculado["data"].unique():
                for serie in self.__hjus_calculado["serie"].unique():
                    for pat in self.__hjus_calculado["patamar"].unique():
                        df_nw = self.hjus_nw.loc[(self.hjus_nw["data"] == data) & (self.hjus_nw["serie"] == serie)& (self.hjus_nw["patamar"] == pat)]["valor"].iloc[0]
                        df_calc = self.__hjus_calculado.loc[(self.__hjus_calculado["data"] == data) & (self.__hjus_calculado["serie"] == serie) & (self.__hjus_calculado["patamar"] == pat)]["Hjus_calc"].iloc[0]
                        lista_NW.append(df_nw)
                        lista_Erro.append(df_nw - df_calc)
            

            self.__hjus_calculado["NW"] = lista_NW
            self.__hjus_calculado["Erro"] =  lista_Erro
            self.__hjus_calculado.to_csv(self.caminho_testes_hjus+"/"+"FT_HJUS_"+nome_usina+".csv")
            self.__hjus_memoria_calculo.to_csv(self.caminho_testes_hjus+"/"+"FT_MEMO_HJUS_"+nome_usina+".csv")
            self.lista_Hjus_Calculado.append(self.__hjus_calculado)
        self.hjusUsinas = pd.concat(self.lista_Hjus_Calculado)


    def __calcula_df_altura_jusante(self, nome_usina, nome_usina_jusante) -> pd.DataFrame:
        df_hjus_calculado = pd.DataFrame(columns=["usina", "usina_jusante", "data", "patamar","DurPat","serie", "Hjus_calc"])
        df_hjus_memoria = pd.DataFrame(columns=["usina", "usina_jusante", "data","patamar" , "DurPat", "serie", "hmon_jus",
                                                "indice_familia1", "indice_familia2","href_familia1", "href_familia2",
                                                "qtur", "indice_polinomio_1","indice_polinomio_2",
                                                "limite_inferior1", "limite_superior1", "limite_inferior2", "limite_superior2",
                                                "A0_1", "A1_1", "A2_1", "A3_1", "A4_1",  "A0_2", "A1_2", "A2_2", "A3_2", "A4_2",
                                                "hjus_1", "hjus_2", "Hjus_calc" ])
        df_temp = pd.DataFrame()
        if(nome_usina_jusante != "0"):
            df_hmon_usi_jus = self.calcHmon.loc[(self.calcHmon["usina"] == nome_usina_jusante)]
        
        df_eco_curva_jusante_usi = self.df_eco_curvajusante.loc[(self.df_eco_curvajusante["codigo_usina"] == self.codigo_usi)]
        df_eco_curvajusante_polinomio_usi = self.df_eco_curvajusante_polinomio.loc[(self.df_eco_curvajusante_polinomio["codigo_usina"] == self.codigo_usi)]
        df_eco_curvajusante_polinomio_segmento_usi = self.df_eco_curvajusante_polinomio_segmento.loc[(self.df_eco_curvajusante_polinomio_segmento["codigo_usina"] == self.codigo_usi)]
        
        datas = self.__Qturuh["data"].unique()
        cenarios = self.__Qturuh["serie"].unique()
        patamares = self.__Qturuh["patamar"].unique()                
        for data in datas:
            for cen in cenarios:
                if(nome_usina_jusante != "0"):
                    hmon = df_hmon_usi_jus.loc[(df_hmon_usi_jus["data"] == data ) & (df_hmon_usi_jus["serie"] == cen)]["Hmon_calc"].iloc[0]
                    hmon = round(hmon,4)
                else:
                    hmon = 0
                df_teste = pd.DataFrame()
                df_teste = df_eco_curva_jusante_usi.copy()
                df_teste["nivel_montante_referencia"] = (df_eco_curva_jusante_usi["nivel_montante_referencia"] - hmon).round(4)
                lista_teste = df_teste["nivel_montante_referencia"].tolist()
                indice_familias = []
                salva_HRefJus = []
                elemento_anterior = 0
                if(lista_teste[-1] < 0.00):
                    indice_familias.append(max(df_eco_curva_jusante_usi["indice_familia"].tolist()))
                    

                elif(lista_teste[0] > 0.00):
                    indice_familias.append(min(df_eco_curva_jusante_usi["indice_familia"].tolist()))
                else:
                    elemento_anterior = 0
                    flag_fim = 0
                    for elemento in lista_teste:
                        if((elemento > 0.00) & (elemento_anterior < 0.00) & (flag_fim == 0)):
                            indice_familias.append(df_teste.loc[(df_teste["nivel_montante_referencia"] == elemento_anterior)]["indice_familia"].iloc[0])
                            indice_familias.append(df_teste.loc[(df_teste["nivel_montante_referencia"] == elemento)]["indice_familia"].iloc[0])
                            flag_fim = 1
                        if((elemento == 0.00) & (flag_fim == 0)):
                            indice_familias.append(df_teste.loc[(df_teste["nivel_montante_referencia"] == elemento)]["indice_familia"].iloc[0])
                            flag_fim = 1
                        elemento_anterior = elemento
                
                if(len(indice_familias) == 2):
                    print(df_eco_curva_jusante_usi)
                    for indice in indice_familias:
                        df_href = df_eco_curva_jusante_usi.loc[(df_eco_curva_jusante_usi["indice_familia"] == indice)].reset_index(drop = True)
                        print("df_href: ", df_href)
                        salva_HRefJus.append(df_href["nivel_montante_referencia"].iloc[0])
                    
                for pat in patamares:
                    durPat = self.duracaoMensalPatamares.loc[(self.duracaoMensalPatamares["data"] == data) & (self.duracaoMensalPatamares["patamar"] == int(pat)) ]["valor"].iloc[0]
                    df_vazao_jusante_usi_est_pat_cen = self.__Qdefuh.loc[(self.__Qdefuh["data"] == data) & (self.__Qdefuh["patamar"] == pat) & (self.__Qdefuh["serie"] == cen)]["valor"].iloc[0]
                    df_vazao_jusante_usi_est_pat_cen = round(df_vazao_jusante_usi_est_pat_cen,4)
                    lista_hjus = []
                    save_old_hjus = []
                    salva_indice_polinomio = []
                    salva_limite_inferior = []
                    salva_limite_superior = []
                    salva_A0 = []
                    salva_A1 = []
                    salva_A2 = []
                    salva_A3 = []
                    salva_A4 = []

                    for familia in indice_familias:
                        n_polinomios = df_eco_curvajusante_polinomio_usi.loc[(df_eco_curvajusante_polinomio_usi["indice_familia"] == familia)]["numero_polinomios"].iloc[0]
                        df_familia = df_eco_curvajusante_polinomio_segmento_usi.loc[(df_eco_curvajusante_polinomio_segmento_usi["indice_familia"] == familia)]
                        lista_limite_inferior_vazao_jusante = df_familia["limite_inferior_vazao_jusante"].tolist()
                        lista_limite_superior_vazao_jusante = df_familia["limite_superior_vazao_jusante"].tolist()

                        indice_polinomio = 0
                        for contador in range(0,len(lista_limite_inferior_vazao_jusante)):
                            if((df_vazao_jusante_usi_est_pat_cen >= lista_limite_inferior_vazao_jusante[contador]) & (df_vazao_jusante_usi_est_pat_cen <= lista_limite_superior_vazao_jusante[contador])):
                                salva_limite_inferior.append(lista_limite_inferior_vazao_jusante[contador])
                                salva_limite_superior.append( lista_limite_superior_vazao_jusante[contador])
                                indice_polinomio = df_familia["indice_polinomio"].tolist()[contador]
                                salva_indice_polinomio.append(indice_polinomio)
                        if((indice_polinomio == 0) and (df_vazao_jusante_usi_est_pat_cen <= lista_limite_inferior_vazao_jusante[0])):
                            indice_polinomio = 1
                            salva_indice_polinomio.append(indice_polinomio)
                            salva_limite_inferior.append(lista_limite_inferior_vazao_jusante[0])
                            salva_limite_superior.append( lista_limite_superior_vazao_jusante[0])
                        if((indice_polinomio == 0) and (df_vazao_jusante_usi_est_pat_cen >= lista_limite_superior_vazao_jusante[-1])):
                            indice_polinomio = max(df_familia["indice_polinomio"].tolist())
                            salva_indice_polinomio.append(indice_polinomio)
                            salva_limite_inferior.append(lista_limite_inferior_vazao_jusante[0])
                            salva_limite_superior.append( lista_limite_superior_vazao_jusante[0])
                        polinomios = df_familia.loc[(df_familia["indice_polinomio"] == indice_polinomio)]
                        A0 = polinomios["coeficiente_a0"].iloc[0]
                        A1 = polinomios["coeficiente_a1"].iloc[0]
                        A2 = polinomios["coeficiente_a2"].iloc[0]
                        A3 = polinomios["coeficiente_a3"].iloc[0]
                        A4 = polinomios["coeficiente_a4"].iloc[0]
                        salva_A0.append(A0)
                        salva_A1.append(A1)
                        salva_A2.append(A2)
                        salva_A3.append(A3)
                        salva_A4.append(A4)
                        hjus = A0*(df_vazao_jusante_usi_est_pat_cen**0) + A1*(df_vazao_jusante_usi_est_pat_cen**1) + A2*(df_vazao_jusante_usi_est_pat_cen**2) + A3*(df_vazao_jusante_usi_est_pat_cen**3) + A4*(df_vazao_jusante_usi_est_pat_cen**4)
                        lista_hjus.append(round(hjus,2))
                        save_old_hjus.append(round(hjus,2))
                    memoria_fatores = []
                    if(len(lista_hjus) == 2):
                        hjus = 0
                        ## É REALIZADA UMA LINEARIZACAO EM RELACAO A HJUS E HREFJUS CONSIDERANDO HMON DA USINA A JUSANTE
                        # Y = y0 + (y1 - y0) *((x - x0)/(x1- x0))
                        hjus = lista_hjus[0] + (lista_hjus[1] - lista_hjus[0])*((hmon - salva_HRefJus[0])/(salva_HRefJus[1] - salva_HRefJus[0]))
                        lista_hjus = []
                        lista_hjus.append(round(hjus,2))
                        
                    hjus = lista_hjus[0]
                    new_row = pd.DataFrame({"usina": nome_usina,
                                    "usina_jusante": nome_usina_jusante,
                                    "data": data,
                                "patamar": pat,
                                "DurPat":durPat,
                                    "serie": cen,
                                    "Hjus_calc":hjus},
                            index = [0])
                    indice_familia2 = 0 if len(indice_familias) == 1 else indice_familias[1]
                    hjus_2 = 0 if len(save_old_hjus) == 1 else save_old_hjus[1] 
                    indice_polinomio_2 = 0 if len(salva_indice_polinomio) == 1 else salva_indice_polinomio[1]
                    limite_inferior_2 =0 if len(salva_limite_inferior) == 1 else salva_limite_inferior[1]
                    limite_superior_2 =0 if len(salva_limite_superior) == 1 else salva_limite_superior[1]
                    salva_HRefJus_base = 0 if len(salva_HRefJus) == 0 else salva_HRefJus[0]
                    salva_HRefJus2 = 0 if (len(salva_HRefJus) <= 1) else salva_HRefJus[1]
                    A0_2 = 0 if len(salva_A0) == 1 else salva_A0[1]
                    A1_2 = 0 if len(salva_A1) == 1 else salva_A1[1]
                    A2_2 = 0 if len(salva_A2) == 1 else salva_A2[1]
                    A3_2 = 0 if len(salva_A3) == 1 else salva_A3[1]
                    A4_2 = 0 if len(salva_A4) == 1 else salva_A4[1]

                    new_row_memoria = pd.DataFrame({"usina": nome_usina,
                                            "usina_jusante":nome_usina_jusante,
                                            "data": data,
                                            "patamar": pat,
                                            "DurPat":durPat,
                                            "serie": cen,
                                            "hmon_jus":hmon,
                                            "indice_familia1": indice_familias[0],
                                            "indice_familia2": indice_familia2,
                                            "href_familia1":salva_HRefJus_base, 
                                            "href_familia2":salva_HRefJus2,
                                            "qtur":df_vazao_jusante_usi_est_pat_cen,
                                            "indice_polinomio_1":salva_indice_polinomio[0],
                                            "indice_polinomio_2":indice_polinomio_2,
                                            "limite_inferior1":salva_limite_inferior[0], 
                                            "limite_superior1":salva_limite_superior[0], 
                                            "limite_inferior2":limite_inferior_2, 
                                            "limite_superior2":limite_superior_2,
                                            "A0_1":salva_A0[0], 
                                            "A1_1":salva_A1[0],
                                            "A2_1":salva_A2[0],
                                            "A3_1":salva_A3[0],
                                            "A4_1":salva_A4[0],
                                            "A0_2":A0_2, 
                                            "A1_2":A1_2, 
                                            "A2_2":A2_2, 
                                            "A3_2":A3_2, 
                                            "A4_2":A4_2,                                                            
                                            "hjus_1":save_old_hjus[0], 
                                            "hjus_2":hjus_2, 
                                            "Hjus_calc": hjus
                                            },
                            index = [0])

                    df_hjus_calculado = pd.concat([df_hjus_calculado.loc[:],new_row]).reset_index(drop=True)
                    df_hjus_memoria = pd.concat([df_hjus_memoria.loc[:],new_row_memoria]).reset_index(drop=True)
        self.__hjus_calculado = df_hjus_calculado
        self.__hjus_memoria_calculo = df_hjus_memoria
        return 0