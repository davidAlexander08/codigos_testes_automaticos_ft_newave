from inewave.newave import Sistema
from inewave.newave import Patamar
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
def compara_geracao_usinas_nao_simuladas(data_ini, caminho_TesteBase, caminho_testes_eco):
    df_peq_usi_sistema = Sistema.read(caminho_TesteBase+"/sistema.dat").geracao_usinas_nao_simuladas
    df_peq_usi_sistema = df_peq_usi_sistema.loc[(df_peq_usi_sistema["data"] >= data_ini)].reset_index(drop=True)
    df_peq_usi_sistema["submercado"] = df_peq_usi_sistema["codigo_submercado"].map(mapa_submercados)

    #print(df_peq_usi_sistema)
    patamar = Patamar.read(caminho_TesteBase+"/patamar.dat")
    
    df_patamares_peq_usi = patamar.usinas_nao_simuladas
    df_patamares_peq_usi = df_patamares_peq_usi.loc[(df_patamares_peq_usi["data"] >= data_ini)].reset_index(drop=True)
    df_patamares_peq_usi["submercado"] = df_patamares_peq_usi["codigo_submercado"].map(mapa_submercados)
    #print(df_patamares_peq_usi)
    
    df_peq_usi = leitura_pmo_bloco_peq_usi(data_ini, caminho_TesteBase, "DADOS DE GERACAO DE PEQUENAS USINAS", "DADOS DE MERCADO LIQUIDO DE ENERGIA")
    #print(df_peq_usi)
    
    patamares = df_patamares_peq_usi["patamar"].unique()
    fontes = df_peq_usi_sistema["fonte"].unique()
    submercados = df_peq_usi_sistema["submercado"].unique()
    #print(patamares)
    #print(fontes)
    lista_df = []
    
    for sbm in submercados:


        #print("SUBMERCADO: ", sbm)
        df_peq_usi_sistema_sbm = df_peq_usi_sistema.loc[df_peq_usi_sistema["submercado"] == sbm].reset_index(drop=True)
        df_peq_usi_sbm = df_peq_usi.loc[df_peq_usi["submercado"] == sbm].reset_index(drop=True)
        df_patamar_sbm = df_patamares_peq_usi.loc[df_patamares_peq_usi["submercado"] == sbm].reset_index(drop=True)


        for fonte in fontes:
            df_peq_usi_sistema_fonte = df_peq_usi_sistema_sbm.loc[df_peq_usi_sistema_sbm["fonte"] == fonte].reset_index(drop=True)
            df_peq_usi_fonte = df_peq_usi_sbm.loc[df_peq_usi_sbm["fonte"] == fonte].reset_index(drop=True)
            df_patamar_fonte = df_patamar_sbm.loc[df_patamar_sbm["indice_bloco"] == df_peq_usi_sistema_fonte["indice_bloco"].iloc[0]].reset_index(drop=True)


            for pat in patamares:
                #print("PATAMAR: ", pat)
                indice_bloco = df_peq_usi_sistema_fonte["indice_bloco"].iloc[0]
                df_patamar_peq_usi_fonte_pat = df_patamar_fonte.loc[df_patamar_fonte["patamar"] == pat].reset_index(drop=True)
                df_peq_usi_sistema_fonte_pat = df_peq_usi_sistema_fonte.copy()
                df_peq_usi_sistema_fonte_pat["valor"] = (df_peq_usi_sistema_fonte["valor"]*df_patamar_peq_usi_fonte_pat["valor"]).round(0)
                df_peq_usi_sistema_fonte_pat["valor_real"] = (df_peq_usi_sistema_fonte["valor"]*df_patamar_peq_usi_fonte_pat["valor"])
                df_peq_usi_fonte_pat = df_peq_usi_fonte.loc[df_peq_usi_fonte["patamar"] == pat].reset_index(drop=True)
                #print(df_peq_usi_sistema_fonte_pat)
                #print(df_peq_usi_fonte_pat)
                #print(df_patamar_peq_usi_fonte_pat)
                df_sanidade_peq_usi = pd.DataFrame()
                df_sanidade_peq_usi["data"] = df_peq_usi_sistema_fonte_pat["data"]
                df_sanidade_peq_usi["fonte"] = fonte
                df_sanidade_peq_usi["submercado"] = sbm
                df_sanidade_peq_usi["patamar"] = pat
                df_sanidade_peq_usi["sistema_orig"] = df_peq_usi_sistema_fonte["valor"]
                df_sanidade_peq_usi["pat"] = df_patamar_peq_usi_fonte_pat["valor"]
                df_sanidade_peq_usi["sistema_calc"] = df_peq_usi_sistema_fonte_pat["valor_real"]
                df_sanidade_peq_usi["sistema"] = df_peq_usi_sistema_fonte_pat["valor"]
                df_sanidade_peq_usi["pmo"] = df_peq_usi_fonte_pat["valor"]
                df_sanidade_peq_usi["erro"] = df_peq_usi_fonte_pat["valor"] - df_peq_usi_sistema_fonte_pat["valor"]
                lista_df.append(df_sanidade_peq_usi)
                erro_total = df_sanidade_peq_usi["erro"].sum()
                #print("SANIDADE PEQ_USI - ERRO: ", erro_total.sum())
                if erro_total != 0:
                    print(f"Erro encontrado para a fonte {fonte} no patamar {pat}. Erro total: {erro_total}")
                    print(df_sanidade_peq_usi)
                    df_sanidade_peq_usi.to_csv("TESTE.csv", index=False)
                    #print("df_peq_usi_sistema_fonte_pat: ", df_peq_usi_sistema_fonte_pat)
                    #print("df_peq_usi_fonte_pat: ", df_peq_usi_fonte_pat)
                    #print("df_patamar_peq_usi_fonte_pat: ", df_patamar_peq_usi_fonte_pat)
                    exit(1)
        
    df_resultado_sanidade = pd.concat(lista_df).reset_index(drop=True)
    #print(df_resultado_sanidade)
    df_resultado_sanidade.to_csv(join(caminho_testes_eco, "sanidade_peq_usi.csv"), index=False)
    print("SANIDADE PEQ_USI - ERRO: ", df_resultado_sanidade["erro"].sum())

    lista_df_total_submercados = []
    for sbm in submercados:
        df_sanidade_total = pd.DataFrame()
        df_resultado_sanidade_sbm = df_resultado_sanidade.loc[df_resultado_sanidade["submercado"] == sbm].reset_index(drop=True)
        df_peq_usi_sbm = df_peq_usi.loc[(df_peq_usi["submercado"] == sbm) & (df_peq_usi["fonte"] == "TOTAL")].reset_index(drop=True)
        for pat in patamares:
            df_resultado_sanidade_sbm_pat = df_resultado_sanidade_sbm.loc[df_resultado_sanidade_sbm["patamar"] == pat].reset_index(drop=True)
            df_resultado_sanidade_sbm_pat = df_resultado_sanidade_sbm_pat.groupby("data").sum(numeric_only=True).reset_index()

            df_peq_usi_sbm_pat = df_peq_usi_sbm.loc[df_peq_usi_sbm["patamar"] == pat].reset_index(drop=True)


            df_sanidade_total["data"] = df_resultado_sanidade_sbm_pat["data"]
            df_sanidade_total["submercado"] = sbm
            df_sanidade_total["patamar"] = pat
            df_sanidade_total["sistema"] = df_resultado_sanidade_sbm_pat["sistema_calc"].round(0)
            df_sanidade_total["pmo"] = df_peq_usi_sbm_pat["valor"]
            df_sanidade_total["erro"] = df_sanidade_total["sistema"] - df_sanidade_total["pmo"]
            if(df_sanidade_total["erro"].sum() != 0):
                print(f"Erro encontrado para o submercado {sbm} no patamar {pat}. Erro total: {df_sanidade_total['erro'].sum()}")
                #print(df_sanidade_total)
                exit(1)
            lista_df_total_submercados.append(df_sanidade_total)
            #print(df_sanidade_total)

    df_resultado_sanidade_total = pd.concat(lista_df).reset_index(drop=True)
    #print(df_resultado_sanidade_total)
    df_resultado_sanidade_total.to_csv(join(caminho_testes_eco, "sanidade_peq_usi_total.csv"), index=False)
    print("SANIDADE PEQ_USI_TOTAL - ERRO: ", df_resultado_sanidade_total["erro"].sum())


def leitura_pmo_bloco_peq_usi(data_ini, caminho_TesteBase, bloco_inicial, bloco_final):
    lista_df = []
    with open(caminho_TesteBase+"/pmo.dat", "r", encoding="latin1") as f:
        flag = 0
        leitura_arquivo_dados_intercambio = 0
        for linha in f:
            linha = linha.strip()
            # Detecta início de um novo bloco com patamar
            if bloco_inicial in linha:
                flag = 1
            if bloco_final in linha:
                flag = 0
                break


            if flag == 1:
                if(linha.startswith("SUBSISTEMA")):
                    match = re.search(r"SUBSISTEMA:\s+([\w\s]+)", linha)
                    if match:
                        subsistema_nome = match.group(1).strip()
                        #print(f"Submercado: {subsistema_nome}")
                        bloco = "TOTAL"
                # Ignora linhas com "X---"
                if(linha.startswith("PATAMAR")):
                    match = re.search(r"PATAMAR:\s+([\w\s]+)", linha)
                    if match:
                        patamar = match.group(1).strip()
                        #print(f"Patamar: {patamar}")
                if(linha.startswith("BLOCO")):
                    match = re.search(r"BLOCO:\s+([\w\s]+)", linha)
                    if match:
                        bloco = match.group(1).strip()
                        #print(f"Bloco: {bloco}")
                        
                if linha.startswith("20"):
                    #print(linha)
                    valores = linha.split()
                    ## Separate year and values
                    ano = int(valores[0]) if linha.startswith("20") else 9999
#
                    meses = [float(v) for v in valores[1:]]
                    #print("ano: ", ano, " | meses: ", meses)
                    for mes, valor in enumerate(meses, start=1):
                        #print("mes: ", mes, " | valor: ", valor)
                        if(datetime(ano, mes, 1) >= data_ini):
                            lista_df.append(
                                pd.DataFrame(
                                    {
                                        "ano": ano,
                                        "mes": mes,
                                        "data": datetime(ano, mes, 1),
                                        "submercado": subsistema_nome,
                                        "patamar": int(patamar),
                                        "fonte": bloco,
                                        "valor": valor
                                    }, index=[0]
                                )
                            )
    df_peq_usi = pd.concat(lista_df).reset_index(drop=True)


    return df_peq_usi