from typing import Dict
import os
import pandas as pd
import shutil
from inewave.newave.dger import Dger
from inewave.nwlistop.viol_eletricasin import ViolEletricasin
from inewave.nwlistop.viol_eletrica import ViolEletrica
from inewave.nwlistop.ghiduh import Ghiduh
from inewave.nwlistop.intercambio import Intercambio
import subprocess
import time
import zipfile
import re
class TermoEquacao:
    def __init__(self, sinal, fator,corpo, tipo):
        self.sinal = sinal
        self.fator = fator
        self.corpo = corpo
        self.tipo = tipo
        print("ObjetoCriado: SINAL: ", sinal, " FATOR: ", fator, " CORPO: ", corpo, " TIPO: ", tipo)

class RestricoesEletricasEspeciais:

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_testes_restricoes = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/Restricoes_Eletricas_Especiais"
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"
        self.caminho_ecos = self.caminho_testes_restricoes+"/eco"
        if not os.path.exists(self.caminho_ecos):
            os.makedirs(self.caminho_ecos)
            print(f"Folder '{self.caminho_ecos}' created!")
        else:
            print(f"Folder '{self.caminho_ecos}' already exists!")

        self.caminho_resultado_teste1 = self.caminho_testes_restricoes+"/Resultados_TESTE1"
        if not os.path.exists(self.caminho_resultado_teste1):
            os.makedirs(self.caminho_resultado_teste1)
            print(f"Folder '{self.caminho_resultado_teste1}' created!")
        else:
            print(f"Folder '{self.caminho_resultado_teste1}' already exists!")



        self.verifica_existencia_de_arquivos()
        self.le_restricoes_eletricas_em_data_frame()

        ## REALIZANDO TESTE 1 PARA TESTE AUTOMATICO DAS RESTRICOES ELETRICAS, O TESTE VERIFICARÁ O ECO DAS RESTRICOES NO PMO.DAT
        self.descompacta_arquivos_teste1()
        self.le_arquivos_violacao_teste1()
        print(self.mapa_restricoes_dataFrameViolacao)

        print("SUBTESTE 1 - VERIFICANDO ECO FLAG RESTRICOES ELETRICAS ESPECIAIS NO PMO.DAT")
        with open(self.caminho_TesteBase+"/pmo.dat", "r") as file:
            content = file.read()
            if "CONSIDERA RESTRICAO ELETRICA ESPECIAL                    SIM" in content:
                print("Subteste 1 - OK - FLAG RESTRICOES ELETRICAS ENCONTADO NO PMO.DAT.")
            else:
                print("Subteste 1 - ERRO - FLAG RESTRICOES ELETRICAS NÃO ENCONTADO NO PMO.DAT.") 

        print("SUBTESTE 2 - VERIFICA ATENDIMENTO DAS RESTRICOES ELETRICAS PARA TODOS CENARIOS, PERIODOS E PATAMARES DO CASO")


        self.identifica_termos_das_equcaoes()
        self.decompacta_oper_gh_interc()
        self.teste1()




        ## RETORNAR UM RELATORIO DE O QUE FOI TESTADO NO TESTE1 E SE O TESTE ESTA OU NAO OK

    def teste1(self):
        codigo_restricoes = self.df_RE["RE"].unique()
        for codigo in codigo_restricoes:
            lista = []
            contador = 0
            df_soma = pd.DataFrame()
            for termo in self.mapa_termos_eq[codigo]:
                df_atendimento = pd.DataFrame()
                if(termo.tipo == "GH"):
                    print("LENDO ARQUIVO DE GERACAO HIDRELETRICA DA USINA "+termo.corpo)
                    texto_codigo = ""
                    if(int(termo.corpo) < 10):
                        texto_codigo = "00"+termo.corpo
                    elif(int(termo.corpo)  < 100):
                        texto_codigo = "0"+termo.corpo
                    elif(int(termo.corpo)  < 1000):
                        texto_codigo = termo.corpo
                    df_atendimento = Ghiduh.read(self.caminho_TesteBase+"/ghiduh"+texto_codigo+".out").valores
                    df_atendimento["valor"] = df_atendimento["valor"]*float(termo.fator)
                    if(termo.sinal == "-"):
                        df_atendimento["valor"] = -df_atendimento["valor"]
                    if(contador == 0):
                        df_soma["soma"] = df_atendimento["valor"]
                    else:
                        df_soma["soma"] += df_atendimento["valor"]
                    df_atendimento = df_atendimento.rename(columns={'valor': termo.tipo+"_"+termo.corpo})
                    lista.append(df_atendimento)

                if(termo.tipo == "INTERC"):
                    print("LENDO ARQUIVO DE INTERCAMBIO"+termo.corpo)
                    codigos_interc = termo.corpo.split(",")
                    cod = [0,0]
                    for indice in range(0,len(codigos_interc)):
                        texto_codigo = ""
                        if(int(codigos_interc[indice]) < 10):
                            cod[indice] = "00"+codigos_interc[indice]
                        elif(int(codigos_interc[indice])  < 100):
                            cod[indice] = "0"+codigos_interc[indice]
                    
                    arquivo_interc_1 = self.caminho_TesteBase+"/int"+cod[0]+cod[1]+".out"
                    arquivo_interc_2 = self.caminho_TesteBase+"/int"+cod[1]+cod[0]+".out"
                    if os.path.isfile(arquivo_interc_1):
                        df_atendimento = Intercambio.read(arquivo_interc_1).valores
                    elif os.path.isfile(arquivo_interc_2):
                        df_atendimento = Intercambio.read(arquivo_interc_2).valores
                        df_atendimento["valor"] = -df_atendimento["valor"]

                    df_atendimento = df_atendimento.loc[df_atendimento["patamar"] != "TOTAL"].reset_index(drop = True)
                    df_atendimento["valor"] = df_atendimento["valor"]*float(termo.fator)
                    if(termo.sinal == "-"):
                        df_atendimento["valor"] = -df_atendimento["valor"]
                    if(contador == 0):
                        df_soma["soma"] = df_atendimento["valor"]
                    else:
                        df_soma["soma"] += df_atendimento["valor"]
                    df_atendimento = df_atendimento.rename(columns={'valor': termo.tipo+"_"+termo.corpo})
                    lista.append(df_atendimento)
                    printdf_atendimento)
                contador += 1
            df_viol = self.mapa_restricoes_dataFrameViolacao[codigo]
            df_viol = df_viol.loc[df_viol["patamar"] != "TOTAL"].reset_index(drop = True)
            df_viol = df_viol.rename(columns={'valor': "viol_RE_"+str(codigo)})
            lista.append(df_viol)
            lista.append(df_soma)
            df_resultante = pd.concat(lista, axis = 1)
            df_resultante = df_resultante.loc[:, ~df_resultante.columns.duplicated()]
            df_resultante["Lim_INF"] = None
            df_resultante["Lim_SUP"] = None
            df_soma["soma"] = df_soma["soma"]*0

            patamares = df_resultante["patamar"].unique()
            df_RE_temp = self.df_RE.loc[self.df_RE["RE"] == codigo]
            df_RE_HORIZ_PER_temp = self.df_RE_HORIZ_PER.loc[self.df_RE_HORIZ_PER["RE"] == codigo]
            df_RE_LIM_FORM_PER_PAT_temp = self.df_RE_LIM_FORM_PER_PAT.loc[self.df_RE_LIM_FORM_PER_PAT["RE"] == codigo]
            
            for data in df_resultante["data"].unique():
                if(data >= df_RE_HORIZ_PER_temp["data_inicio"].iloc[0] and data <= df_RE_HORIZ_PER_temp["data_fim"].iloc[0]):
                    datas_inicio_alteracoes = df_RE_LIM_FORM_PER_PAT_temp["data_inicio"].unique()
                    data_fim_alteracoes = df_RE_LIM_FORM_PER_PAT_temp["data_fim"].unique()
                    for i in range(0,len(data_fim_alteracoes)):
                        if(data >= datas_inicio_alteracoes[i] and data <= data_fim_alteracoes[i]):
                            df_valores = df_RE_LIM_FORM_PER_PAT_temp.loc[(df_RE_LIM_FORM_PER_PAT_temp["data_inicio"] == datas_inicio_alteracoes[i]) & (df_RE_LIM_FORM_PER_PAT_temp["data_fim"] == data_fim_alteracoes[i])]
                            print("data: ", data, " valores: ", df_valores)
                            for pat in patamares:
                                df_resultante.loc[(df_resultante["data"] == data) & (df_resultante["patamar"] == pat), "Lim_INF"] = df_valores.loc[df_valores["pat"] == int(pat), "lim_inf"].iloc[0]
                                df_resultante.loc[(df_resultante["data"] == data) & (df_resultante["patamar"] == pat), "Lim_SUP"] = df_valores.loc[df_valores["pat"] == int(pat), "lim_sup"].iloc[0]
            
            df_resultante = df_resultante.loc[pd.notna(df_resultante["Lim_INF"])].reset_index(drop = True)
            lista_OK = []
            for index,row in df_resultante.iterrows():
                print(row)
                if(row.soma >= float(row.Lim_INF) and row.soma <= float(row.Lim_SUP)):
                    lista_OK.append("OK")
                else:
                    lista_OK.append("ERRO")
            df_resultante["FTNEWAVE"] = lista_OK
            df_resultante.to_csv(self.caminho_resultado_teste1+"/balanco_RE_"+str(codigo)+".csv")
            

    def decompacta_oper_gh_interc(self):
        print("Verificando ZIPs e Descompactando Geracao de USINAS E INTERCAMBIO")
        out_zip = self.caminho_TesteBase+"/operacao_TesteBase.zip"
        if os.path.isfile(out_zip):
            print("ARQUIVO operacao_TESTE1.zip EXISTE, DESCOMPACTANDO VIOLACOES RESTRICOES")
            with zipfile.ZipFile(out_zip, 'r') as zip_ref:
                # Iterate over the files in the zip archive
                for file in zip_ref.namelist():
                    # Check if the file name matches the pattern (e.g., starts with 'viol_ele')
                    if file.startswith('ghiduh'):
                        # Extract the specific file to the output directory
                        zip_ref.extract(file, self.caminho_TesteBase)
                        print(f"Extracted {file} to {self.caminho_TesteBase}")
                    if file.startswith('int'):
                        # Extract the specific file to the output directory
                        zip_ref.extract(file, self.caminho_TesteBase)
                        print(f"Extracted {file} to {self.caminho_TesteBase}")


    def identifica_termos_das_equcaoes(self):
        codigo_restricoes = self.df_RE["RE"].unique()
        self.mapa_termos_eq = {}
        for codigo in codigo_restricoes:
            df_restr = self.df_RE.loc[self.df_RE["RE"] == codigo]
            df_restr_horiz = self.df_RE_HORIZ_PER.loc[self.df_RE_HORIZ_PER["RE"] == codigo]
            df_restr_pat_lim = self.df_RE_LIM_FORM_PER_PAT.loc[self.df_RE_LIM_FORM_PER_PAT["RE"] == codigo]
            print("RESTR: ", codigo)
            print(df_restr)
            print(df_restr_horiz)
            print(df_restr_pat_lim)
            expressao = df_restr["Expressao"].iloc[0].replace(" ", "")
            print(expressao)
            expressao = expressao.split(")")
            print(expressao)
            lista_termos_equacao = []
            for termo in expressao:
                if("ger_usih" in termo):
                    separa = termo.split("ger_usih")
                    print(separa)
                    corpo = separa[1].split("(")[1]
                    sinal = ""
                    fator = "1.0"
                    termo_sinal = separa[0]
                    if(separa[0] == "" or separa[0] == "+"):
                        sinal = "+"
                    elif(separa[0] == "-"):
                        sinal = "-"
                    elif(len(separa[0].split("+"))>1 ):
                        novo_termo = separa[0].split("+")
                        sinal = "+"
                        fator = novo_termo[1]
                    elif(len(separa[0].split("-"))>1 ):
                        novo_termo = separa[0].split("-")
                        sinal = "-"
                        fator = novo_termo[1]
                    else:
                        try:
                            value = float(separa[0])  # Attempt to convert to a float
                            fator = separa[0]
                            sinal = "+"
                        except ValueError:
                            # If it fails, separa[0] is not a number
                            print("ERRO NO SINAL DA EQUACAO", expressao)
                            exit(1)
                    
                    objeto = TermoEquacao(sinal, fator, corpo, "GH")
                    lista_termos_equacao.append(objeto)

                if("ener_interc" in termo):
                    separa = termo.split("ener_interc")
                    print(separa)
                    corpo = separa[1].split("(")[1]
                    sinal = ""
                    fator = "1.0"
                    termo_sinal = separa[0]
                    if(separa[0] == "" or separa[0] == "+"):
                        sinal = "+"
                    elif(separa[0] == "-"):
                        sinal = "-"
                    elif(len(separa[0].split("+"))>1 ):
                        novo_termo = separa[0].split("+")
                        sinal = "+"
                        fator = novo_termo[1]
                    elif(len(separa[0].split("-"))>1 ):
                        novo_termo = separa[0].split("-")
                        sinal = "-"
                        fator = novo_termo[1]
                    else:
                        try:
                            value = float(separa[0])  # Attempt to convert to a float
                            fator = separa[0]
                            sinal = "+"
                        except ValueError:
                            # If it fails, separa[0] is not a number
                            print("ERRO NO SINAL DA EQUACAO", expressao)
                            exit(1)
                    
                    objeto = TermoEquacao(sinal, fator, corpo, "INTERC")
                    lista_termos_equacao.append(objeto)
            self.mapa_termos_eq[codigo] = lista_termos_equacao


    def le_arquivos_violacao_teste1(self):



        print("LENDO ARQUIVOS DE VIOLACAO ELETRICA")
        caminho_viol_elet_sin = self.caminho_TesteBase+"/viol_eletricasin.out"
        if os.path.isfile(caminho_viol_elet_sin):
            dados_vol_elet_sin = ViolEletricasin.read(caminho_viol_elet_sin)
            self.df_viol_sin = dados_vol_elet_sin.valores
        print(self.caminho_antes_da_pasta_teste)

        self.df_viol_sin.to_csv(self.caminho_ecos+"/df_viol_sin.csv")

        self.mapa_restricoes_dataFrameViolacao = {}
        codigo_restricoes = self.df_RE["RE"].unique()
        for codigo in codigo_restricoes:
            texto_codigo = ""
            if(codigo < 10):
                texto_codigo = "00"+str(codigo)
            elif(codigo < 100):
                texto_codigo = "0"+str(codigo)
            elif(codigo < 1000):
                texto_codigo = str(codigo)
            dados_viol = ViolEletrica.read(self.caminho_TesteBase+"/viol_eletrica"+texto_codigo+".out")
            dados_viol.valores.to_csv(self.caminho_ecos+"/viol_eletrica"+texto_codigo+".csv")
            self.mapa_restricoes_dataFrameViolacao[codigo] = dados_viol.valores
        

    def descompacta_arquivos_teste1(self):
        print("Verificando ZIPs e Descompactando Restrições Elétricas")
        out_zip = self.caminho_TesteBase+"/operacao_TesteBase.zip"
        if os.path.isfile(out_zip):
            print("ARQUIVO operacao_TESTE1.zip EXISTE, DESCOMPACTANDO VIOLACOES RESTRICOES")
            with zipfile.ZipFile(out_zip, 'r') as zip_ref:
                # Iterate over the files in the zip archive
                for file in zip_ref.namelist():
                    # Check if the file name matches the pattern (e.g., starts with 'viol_ele')
                    if file.startswith('viol_eletrica'):
                        # Extract the specific file to the output directory
                        zip_ref.extract(file, self.caminho_TesteBase)
                        print(f"Extracted {file} to {self.caminho_TesteBase}")

    def le_restricoes_eletricas_em_data_frame(self):
        print("LENDO RESTRICOES ELETRICAS")
        lista_df_RE = []
        lista_df_RE_HORIZ_PER = []
        lista_df_RE_LIM_FORM_PER_PAT = []
        with open(self.arquivo_restricoes_eletricas, "r") as file:
            for line in file:
                if("&" not in line and line.strip()):
                    linha = line.strip().split(";")
                    #print(linha)
                    if(linha[0].strip() == "RE"):
                        df_RE_temp = pd.DataFrame({"RE":[int(linha[1].strip())], "Expressao":[linha[2]]})
                        lista_df_RE.append(df_RE_temp)
                    elif(linha[0].strip() == "RE-HORIZ-PER"):
                        df_RE_HORIZ_PER_temp = pd.DataFrame({"RE":[int(linha[1].strip())], "data_inicio":[linha[2].strip()], "data_fim":[linha[3].strip()]})
                        df_RE_HORIZ_PER_temp['data_inicio'] = pd.to_datetime(df_RE_HORIZ_PER_temp['data_inicio'] + '/01')  # Add day for conversion
                        df_RE_HORIZ_PER_temp['data_fim'] = pd.to_datetime(df_RE_HORIZ_PER_temp['data_fim'] + '/01')
                        lista_df_RE_HORIZ_PER.append(df_RE_HORIZ_PER_temp)
                    elif(linha[0].strip() == "RE-LIM-FORM-PER-PAT"):
                        df_RE_LIM_FORM_PER_PAT_temp = pd.DataFrame({"RE":[int(linha[1].strip())], "data_inicio":[linha[2].strip()], "data_fim":[linha[3].strip()],
                        "pat":[int(linha[4].strip())], "lim_inf":[linha[5].strip()], "lim_sup":[linha[6].strip()]})
                        df_RE_LIM_FORM_PER_PAT_temp['data_inicio'] = pd.to_datetime(df_RE_LIM_FORM_PER_PAT_temp['data_inicio'] + '/01')  # Add day for conversion
                        df_RE_LIM_FORM_PER_PAT_temp['data_fim'] = pd.to_datetime(df_RE_LIM_FORM_PER_PAT_temp['data_fim'] + '/01')

                        lista_df_RE_LIM_FORM_PER_PAT.append(df_RE_LIM_FORM_PER_PAT_temp)
        self.df_RE = pd.concat(lista_df_RE)
        self.df_RE_HORIZ_PER = pd.concat(lista_df_RE_HORIZ_PER)
        self.df_RE_LIM_FORM_PER_PAT = pd.concat(lista_df_RE_LIM_FORM_PER_PAT)
        self.df_RE.to_csv(self.caminho_ecos+"/df_RE.csv")
        self.df_RE_HORIZ_PER.to_csv(self.caminho_ecos+"/df_RE_HORIZ_PER.csv")
        self.df_RE_LIM_FORM_PER_PAT.to_csv(self.caminho_ecos+"/df_RE_LIM_FORM_PER_PAT.csv")
        print("RESTRICOES ELETRICAS LIDAS")


    def verifica_existencia_de_arquivos(self):        
        print("VERIFICANDO ARQUIVOS PARA REALIZACAO DOS TESTES")
        self.indices = self.caminho + "/indices.csv"
        if os.path.isfile(self.indices):
            print("INDICES.CSV OK")
        else: 
            print("INDICES.CSV não existe, não é possível realizar o teste")
            exit(1)
        flag = 0
        with open(self.indices, "r") as file:
            for line in file:
                if("RESTRICAO-ELETRICA-ESPECIAL" in line):
                    print("FUNCIONALIDADE RESTRICAO-ELETRICA-ESPECIAL OK. Prosseguindo")
                    flag = 1
                    linha = line.strip()
                    self.arquivo_restricoes_eletricas = self.caminho + "/"+linha.split(';')[2].strip()
                    if os.path.isfile(self.indices):
                        print(self.arquivo_restricoes_eletricas + " ARQUIVO COM RESTRICOES ELETRICAS EXISTE")
                    else: 
                        print(self.arquivo_restricoes_eletricas + " não existe, não é possível realizar o teste")
                        exit(1)
            if(flag == 0):
                print("FUNCIONALIDADE FUNCIONALIDADE RESTRICAO-ELETRICA-ESPECIAL NÃO ENCONTRADA EM indices.csv")

        self.dger = self.caminho + "/dger.dat"
        if os.path.isfile(self.dger):
            print("dger.dat OK")
            dados_dger = Dger.read(self.dger)
            flag_restricoes_eletricas = dados_dger.restricoes_eletricas
            if(int(flag_restricoes_eletricas) == 1):
                print("Flag restrições elétricas OK")
            else:
                print("Flag restrições elétricas não é igual a 1, não é possível realizar o teste")
                exit(1)
        else: 
            print("dger.dat não existe, não é possível realizar o teste")
            exit(1)   
        


        print(self.caminho)