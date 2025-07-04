from inewave.newave import Cadic
from inewave.newave import Sistema
import re
import pandas as pd
from datetime import datetime
from os.path import join

mapa_submercados = {
    1: "SUDESTE",
    2: "SUL",
    4: "NORTE",
    3: "NORDESTE"
}
def compara_mercado_energia_total(data_ini, caminho_TesteBase, caminho_testes_eco):
    df_carga_sistema = Sistema.read(caminho_TesteBase+"/sistema.dat").mercado_energia
    df_carga_sistema = df_carga_sistema.loc[(df_carga_sistema["data"] >= data_ini)].reset_index(drop=True)
    df_carga_sistema["submercado"] = df_carga_sistema["codigo_submercado"].map(mapa_submercados)
    df_mercado_total_pmo = leitura_pmo_bloco_ano(caminho_TesteBase,"DADOS DE MERCADO TOTAL DE ENERGIA", "DADOS DE GERACAO DE PEQUENAS USINAS")
    

    compara_mercado = pd.DataFrame()
    for sbm in df_carga_sistema["submercado"].unique():
        df_carga_sistema_sbm = df_carga_sistema.loc[df_carga_sistema["submercado"] == sbm].reset_index(drop=True)
        df_mercado_total_pmo_sbm = df_mercado_total_pmo.loc[df_mercado_total_pmo["submercado"] == sbm].reset_index(drop=True)
        compara_mercado["sbm"] = df_mercado_total_pmo_sbm["submercado"]
        compara_mercado["data"] = df_mercado_total_pmo_sbm["data"]
        compara_mercado["sitema"] = df_carga_sistema_sbm["valor"]
        compara_mercado["pmo"] = df_mercado_total_pmo_sbm["valor"]
        compara_mercado["erro"] = df_carga_sistema_sbm["valor"]   - df_mercado_total_pmo_sbm["valor"]
    compara_mercado.to_csv(join(caminho_testes_eco, "sanidade_sistema_mercado_energia_total.csv"), index=False)
    print("SANIDADE MERCADO ENERGIA TOTAL - ERRO: ", compara_mercado["erro"].sum())



def compara_eco_c_adic(data_ini, caminho_TesteBase, caminho_testes_eco):
    df_c_adic = Cadic.read(caminho_TesteBase+"/c_adic.dat").cargas
    df_c_adic = df_c_adic.loc[(df_c_adic["data"] >= data_ini)].reset_index(drop=True)
    df_c_adic_sbm = df_c_adic.groupby(["nome_submercado", "data"])["valor"].sum().reset_index()

    df_c_adic_pmo = leitura_pmo_bloco_ano(caminho_TesteBase, "DADOS DE CARGA ADICIONAL DE ENERGIA", "DADOS DE MERCADO TOTAL DE ENERGIA")

    compara_c_adic = pd.DataFrame()
    for sbm in df_c_adic_sbm["nome_submercado"].unique():
        df_c_adic_sbm_sbm = df_c_adic_sbm.loc[df_c_adic_sbm["nome_submercado"] == sbm].reset_index(drop=True)
        df_c_adic_pmo_sbm = df_c_adic_pmo.loc[df_c_adic_pmo["submercado"] == sbm].reset_index(drop=True)
        compara_c_adic["sbm"] = df_c_adic_pmo_sbm["submercado"]
        compara_c_adic["pmo"] = df_c_adic_pmo_sbm["submercado"]
        compara_c_adic["data"] = df_c_adic_sbm_sbm["data"]
        compara_c_adic["c_adic"] = df_c_adic_sbm_sbm["valor"]
        compara_c_adic["pmo"] = df_c_adic_pmo_sbm["valor"]
        compara_c_adic["erro"] = df_c_adic_sbm_sbm["valor"]   - df_c_adic_pmo_sbm["valor"]
    compara_c_adic.to_csv(join(caminho_testes_eco, "sanidade_c_adic.csv"), index=False)
    print("SANIDADE C_ADIC - ERRO: ", compara_c_adic["erro"].sum())

def leitura_pmo_bloco_ano(caminho_TesteBase, inicio, fim):
    lista_df = []
    lista_anos = []
    with open(caminho_TesteBase+"/pmo.dat", "r", encoding="latin1") as f:
        flag = 0
        leitura_arquivo_dados_intercambio = 0
        for linha in f:
            linha = linha.strip()
            # Detecta início de um novo bloco com patamar
            if inicio in linha:
                flag = 1
            if fim in linha:
                flag = 0
                break


            if flag == 1:
                if(linha.startswith("SUBSISTEMA")):
                    match = re.search(r"SUBSISTEMA:\s+([\w\s]+)", linha)
                    if match:
                        subsistema_nome = match.group(1).strip()
                # Ignora linhas com "X---"
                if linha.startswith("20") or linha.startswith("POS"):
                    #print(linha)
                    valores = linha.split()
                    # Separate year and values
                    ano = int(valores[0]) if linha.startswith("20") else 9999

                    meses = [float(v) for v in valores[1:]]
                    for mes, valor in enumerate(meses, start=1):
                        if valor != 0:
                            lista_df.append(
                                pd.DataFrame(
                                    {
                                        "ano": ano,
                                        "mes": mes,
                                        "data": datetime(ano, mes, 1),
                                        "submercado": subsistema_nome,
                                        "valor": valor
                                    }, index=[0]
                                )
                            )
    df_c_adic_pmo = pd.concat(lista_df).reset_index(drop=True)


    return df_c_adic_pmo