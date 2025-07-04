import sys
import os
# Caminho absoluto para a pasta que contém o inewave caso esteja utilizando uma inewave no PC.
#base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..", "inewave"))
#print(base_dir)
#sys.path.insert(0, base_dir)


from typing import List
from os.path import join
from datetime import datetime
from calendar import monthrange
import numpy as np
import pandas as pd
import re
from inewave.newave import Dger
from apps.utils.log import Log
import os.path
from apps.avalia_fpha.caso import CasoAvalicao
from apps.avalia_fpha.usina import UsinaAvalicao
from apps.avalia_balanco.configuracao import Configuracao
from inewave.newave import Pmo
from inewave.newave import Hidr

from inewave.libs import UsinasHidreletricas
from apps.avalia_fpha.indicadores import IndicadoresAvaliacaoFPHA
from apps.automatizacao_ftnewave.eco_pmo_functions import carga_adic_tot, capacidade_intercambio, peq_usi, mercado_liquido_energia, limites_agrupamentos_intercambio, ena_past, energia_controlavel, energia_armazenavel_maxima



class Eco_pmo:

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_eco = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Eco"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_testes_eco):
            os.makedirs(self.caminho_testes_eco)
            print(f"Folder '{self.caminho_testes_eco}' created!")
        else:
            print(f"Folder '{self.caminho_testes_eco}' already exists!")

        self.dger = Dger.read(self.caminho_TesteBase+"/dger.dat")
        mes_ini = self.dger.mes_inicio_estudo
        ano_ini = self.dger.ano_inicio_estudo
        self.data_ini = pd.to_datetime(str(ano_ini)+"-"+str(mes_ini)+"-01")
        hidr = Hidr.read(self.caminho_TesteBase+"/hidr.dat")
        df_cadastro = hidr.cadastro
        df_cadastro.to_csv(join(self.caminho_testes_eco, "hidr.csv"), index=False)


        energia_armazenavel_maxima.compara_energia_armazenavel_maxima(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        energia_controlavel.compara_energia_controlavel(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        ena_past.compara_ena_past(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        mercado_liquido_energia.compara_mercado_energia_liquida(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        capacidade_intercambio.compara_capacidade_intercambio_entre_subsistemas(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        carga_adic_tot.compara_eco_c_adic(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        carga_adic_tot.compara_mercado_energia_total(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        peq_usi.compara_geracao_usinas_nao_simuladas(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)
        limites_agrupamentos_intercambio.compara_agrupamento_intercambio(self.data_ini, self.caminho_TesteBase, self.caminho_testes_eco)



    
