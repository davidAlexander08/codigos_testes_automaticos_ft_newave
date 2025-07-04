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
class DeckBase():
    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.eco_deck = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/EcoDeck"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"
        if not os.path.exists(self.eco_deck):
            os.makedirs(self.eco_deck)
            print(f"Folder '{self.eco_deck}' created!")
        else:
            print(f"Folder '{self.eco_deck}' already exists!")

        self.patamar = Patamar.read(self.caminho_TesteBase+"/patamar.dat")
        self.numeroPatamares = patamar.numero_patamares
        self.duracaoMensalPatamares = patamar.duracao_mensal_patamares
        self.cargaPatamares = patamar.carga_patamares
        self.intercambioPatamares = patamar.intercambio_patamares
        self.usinasNaoSimuladasPatamares = patamar.usinas_nao_simuladas

        self.sistema = Sistema.read(self.caminho_TesteBase+"/sistema.dat")
        self.limitesInterc = self.sistema.limites_intercambio

        self.dger = Dger.read(self.caminho_TesteBase+"/dger.dat")
        mes_ini = self.dger.mes_inicio_estudo
        ano_ini = self.dger.ano_inicio_estudo
        self.series_sf = self.dger.num_series_sinteticas
        self.data_ini = pd.to_datetime(str(ano_ini)+"-"+str(mes_ini)+"-01")


        self.confhd = Confhd.read(self.caminho_TesteBase+"/confhd.dat")
        self.usinas = self.confhd.usinas
        self.lista_nome_usinas = self.usinas["nome_usina"].unique()
        
        self.df_hidr = Hidr.read(self.caminho_TesteBase+"/hidr.dat").cadastro
        self.df_hidr = self.df_hidr.reset_index(drop = False)
        
        self.pmo = Pmo.read(self.caminho_TesteBase+"/pmo.dat")
        self.vol_ini = self.pmo.volume_armazenado_inicial
        