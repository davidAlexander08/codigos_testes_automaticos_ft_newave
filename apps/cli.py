import click
import os
import json
import warnings
from apps.utils.log import Log
@click.group()
def cli():
    """
    Aplicação para reproduzir testes
    de planejamento energético feitos no âmbito
    da FT-NEWAVE pelo ONS.
    """
    pass

@click.command("ftnewave")
@click.argument(
    "arquivo_txt",
)
def avalia_ftnewave(arquivo_txt):
    """
    Realiza Rodadas da FTNEWAVE
    - Pega um deck teste com o caminho apontado no txt
    - Realiza o conjunto de testes descritos no txt
    """
    
    warnings.filterwarnings('ignore')
    from apps.automatizacao_ftnewave.eco_pmo import Eco_pmo    
    from apps.automatizacao_ftnewave.organizaTestes import OrganizaTestes
    from apps.automatizacao_ftnewave.deckBase import DeckBase
    print("--------- Realizando Testes da FT-NEWAVE---------------")
    flag_prossegue = 0
    with open(arquivo_txt, "r") as file:
        for line in file:
            if("caminho" in line):
                caminho = line.split('"')[1]
                if os.path.isdir(caminho):
                    print("Pasta: ", caminho, " Existe, Prosseguindo")
                else:
                    print("ERRO: Pasta: ", caminho, " Não Existe")
                    exit(1)
                OrganizaTestes(caminho)
                flag_prossegue = 1
            if(line.startswith("&")):
                continue  
            
            if("ECO" in line and flag_prossegue == 1):
                print("INICIALIZANDO TESTES DE ECO DE PMO.DAT")
                print(line.strip())
                Eco_pmo(caminho)
            
            if("Restricao_Eletrica_Especial" in line and flag_prossegue == 1):
                print("INICIALIZANDO TESTES DE RESTRICOES ELETRICAS ESPECIAIS")
                print(line.strip())
                RestricoesEletricasEspeciais(caminho)
            if("Balanco_Demanda" in line and flag_prossegue == 1):
                print("INICIALIZANDO TESTES DE BALANÇO DE DEMANDA")
                print(line.strip())
                Balanco_Demanda(caminho)

            if("Balanco_Hidrico_Usina" in line and flag_prossegue == 1):
                print("INICIALIZANDO TESTES DE BALANÇO HIDRICO DAS USINAS")
                print(line.strip())
                Balanco_Hidrico(caminho)

            if("Altura" in line and flag_prossegue == 1):
                print("INICIALIZANDO TESTES DE BALANÇO HIDRICO DAS USINAS")
                print(line.strip())
                FT_Hjus(caminho)
                FT_Hliq(caminho)

            if("Evaporacao" in line and flag_prossegue == 1):
                print("INICIALIZANDO TESTES DE EVAPORACAO")
                print(line.strip())
                Evaporacao(caminho)

            if("FPHA" in line and flag_prossegue == 1):
                print("INICIALIZANDO TESTES DE FPHA")
                print(line.strip())
                FPHA(caminho)

            else:
                print(line.strip())

cli.add_command(avalia_ftnewave)
