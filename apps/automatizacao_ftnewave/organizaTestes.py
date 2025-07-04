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

class OrganizaTestes:

    def __init__(self, caminho_deck_ftnewave):
        self.caminho = caminho_deck_ftnewave
        self.caminho_antes_da_pasta_teste = "/".join(caminho_deck_ftnewave.split("/")[:-1])
        self.caminho_FTNEWAVE = self.caminho_antes_da_pasta_teste+"/FTNEWAVE"
        self.caminho_TesteBase = self.caminho_antes_da_pasta_teste+"/FTNEWAVE/TesteBase"

        if not os.path.exists(self.caminho_TesteBase):
            shutil.copytree(self.caminho, self.caminho_TesteBase)
        else:
            #shutil.rmtree(self.caminho_teste_1)
            #shutil.copytree(self.caminho, self.caminho_teste_1)
            print("DIRETORIO TESTEBASE JÁ EXISTE, UTILIZANDO O DIRETORIO EXISTENTE")
        print(self.caminho_TesteBase+"/sintese/ESTATISTICAS_OPERACAO_SIN.parquet")
        if os.path.isfile(self.caminho_TesteBase+"/sintese/ESTATISTICAS_OPERACAO_SIN.parquet"):
            
            print("ARQUIVO ESTATISTICAS_OPERACAO_SIN.parquet EXISTE, CASO JÁ RODADO")
            self.descompactaArquivos()
        else:
            print("ARQUIVO ESTATISTICAS_OPERACAO_SIN.parquet NÃO ENCOTRADO, RODANDO CASO")
            try:
                os.chdir(self.caminho_TesteBase)
                exec_newave_command = "exec_newave 30.0.4 10"
                process = subprocess.Popen(exec_newave_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                print("Running exec_newave...")
                #process = subprocess.Popen("/home/pem/versoes/NEWAVE/v30.0.1/newave30.0.1_L", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                #cli_output = subprocess.check_output(cli_command, shell=True).decode("utf-8")
                process.wait() 
                # Check if the process has completed by reading the txt file
                print("Checking if the model has finished...")
                while not os.path.exists(self.caminho_TesteBase+"/sintese/ESTATISTICAS_OPERACAO_SIN.parquet"):
                    print(f"Esperando ESTATISTICAS_OPERACAO_SIN.parquet aparecer na sintese...")
                    time.sleep(30)  # Wait 30 seconds before checking again
            # Optionally check the contents of the file to confirm it finished successfully
                with open(self.caminho_TesteBase+"/pmo.dat", "r") as file:
                    content = file.read()
                    if "NENHUM ERRO FOI DETECTADO NO CALCULO DA SIMULACAO FINAL" in content:
                        print("Model finished successfully.")
                        self.descompactaArquivos()
                        time.sleep(30)
                    else:
                        print("Model did not finish as expected.")
            
            except Exception as e:
                print(f"An error occurred: {e}")


    def descompactaArquivos(self):
        print("Verificando ZIPs e Descompactando")
        out_zip = self.caminho_TesteBase+"/operacao_TesteBase.zip"
        if os.path.isfile(out_zip):
            print("ARQUIVO operacao_TesteBase.zip EXISTE, DESCOMPACTANDO ARQUIVOS")
            try:
                with zipfile.ZipFile(out_zip, 'r') as zip_ref:
                    zip_ref.extractall(self.caminho_TesteBase)
                   ## Lista de prefixos para filtragem
                   #prefixos = ['ghiduh', 'int']
                   #arquivos_extraidos = []

                   ## Itera pelos arquivos no ZIP
                   #for file in zip_ref.namelist():
                   #    if any(file.startswith(prefixo) for prefixo in prefixos):
                   #        zip_ref.extract(file, self.caminho_TesteBase)
                   #        arquivos_extraidos.append(file)
                   #        print(f"Extraído: {file}")
                    #return arquivos_extraidos
                    print("Extração concluída.")
            except zipfile.BadZipFile:
                print("Erro: O arquivo ZIP está corrompido.")
            except Exception as e:
                print(f"Erro inesperado: {e}")
        else:
            print(f"Arquivo {out_zip} não encontrado.")

        out_zip = self.caminho_TesteBase+"/relatorios_TesteBase.zip"
        if os.path.isfile(out_zip):
            print("ARQUIVO relatorios_TesteBase.zip EXISTE, DESCOMPACTANDO ARQUIVOS")
            try:
                with zipfile.ZipFile(out_zip, 'r') as zip_ref:
                    zip_ref.extractall(self.caminho_TesteBase)
                   ## Lista de prefixos para filtragem
                   #prefixos = ['ghiduh', 'int']
                   #arquivos_extraidos = []

                   ## Itera pelos arquivos no ZIP
                   #for file in zip_ref.namelist():
                   #    if any(file.startswith(prefixo) for prefixo in prefixos):
                   #        zip_ref.extract(file, self.caminho_TesteBase)
                   #        arquivos_extraidos.append(file)
                   #        print(f"Extraído: {file}")
                    #return arquivos_extraidos
                    print("Extração concluída.")
            except zipfile.BadZipFile:
                print("Erro: O arquivo ZIP está corrompido.")
            except Exception as e:
                print(f"Erro inesperado: {e}")
        else:
            print(f"Arquivo {out_zip} não encontrado.")