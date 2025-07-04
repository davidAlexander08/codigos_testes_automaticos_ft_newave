
from inewave.newave import Patamar
from inewave.newave import Sistema
import re
import pandas as pd
from datetime import datetime
from os.path import join

def compara_capacidade_intercambio_entre_subsistemas(data_ini, caminho_TesteBase, caminho_testes_eco):
    df_capacidade_intercambio = leitura_capacidade_intercambio_entre_subsistemas(caminho_TesteBase)
    df_capacidade_intercambio = df_capacidade_intercambio.rename(columns={"sbm_de": "submercado_de", "sbm_para":"submercado_para"})
    df_capacidade_intercambio = df_capacidade_intercambio.sort_values(by=["submercado_de", "submercado_para"], ascending=[True, True])
    df_capacidade_intercambio["submercado_de"] = df_capacidade_intercambio["submercado_de"].replace(5, 11)
    df_capacidade_intercambio["submercado_para"] = df_capacidade_intercambio["submercado_para"].replace(5, 11)
    df_capacidade_intercambio = df_capacidade_intercambio.loc[(df_capacidade_intercambio["data"] >= data_ini)].reset_index(drop=True)
    pairs = list(df_capacidade_intercambio[["submercado_de", "submercado_para"]].drop_duplicates().itertuples(index=False, name=None))

    patamar = Patamar.read(caminho_TesteBase+"/patamar.dat")
    sistema = Sistema.read(caminho_TesteBase+"/sistema.dat")
    #NUMERO SUBMERCADOS 
    df_patamares_intercambio = patamar.intercambio_patamares
    df_patamares_intercambio_est = df_patamares_intercambio.loc[(df_patamares_intercambio["data"] >= data_ini)]
    df_patamares_intercambio_est = df_patamares_intercambio_est.sort_values(by=["submercado_de", "submercado_para"], ascending=[True, True])
    patamares = df_patamares_intercambio_est["patamar"].unique()
    pairs = list(df_patamares_intercambio_est[["submercado_de", "submercado_para"]].drop_duplicates().itertuples(index=False, name=None))



    df_limites_interc = sistema.limites_intercambio.loc[(sistema.limites_intercambio["data"] >= data_ini)].reset_index(drop=True)
    df_limites_interc = df_limites_interc.sort_values(by=["submercado_de", "submercado_para"], ascending=[True, True])
    df_limites_interc["par"] = df_limites_interc.apply(
        lambda row: (row["submercado_de"], row["submercado_para"]) if row["sentido"] == 0 
                    else (row["submercado_para"], row["submercado_de"]),
        axis=1
    )
    # Obtém pares únicos ordenados
    pares_unicos = sorted(df_limites_interc["par"].drop_duplicates().tolist())

    submercados = list(set(list(df_patamares_intercambio_est["submercado_de"].unique()) + list(df_patamares_intercambio_est["submercado_para"].unique())))

    lista_df = []
    for pat in patamares:
        df_patamares_intercambio_est_pat = df_patamares_intercambio_est.loc[df_patamares_intercambio_est["patamar"] == pat].reset_index(drop=True)
        df_capacidade_intercambio_pat = df_capacidade_intercambio.loc[df_capacidade_intercambio["patamar"] == pat].reset_index(drop=True)
        for par in pares_unicos:
            submercado_de, submercado_para = par
            df_limite_par = df_limites_interc.loc[(df_limites_interc["par"] == (submercado_de, submercado_para))].reset_index(drop=True)
            df_patamar_par = df_patamares_intercambio_est_pat.loc[(df_patamares_intercambio_est_pat["submercado_de"] == submercado_de) & (df_patamares_intercambio_est_pat["submercado_para"] == submercado_para)].reset_index(drop=True)
            
            df_capacidade_par = df_capacidade_intercambio_pat.loc[(df_capacidade_intercambio_pat["submercado_de"] == submercado_de) & (df_capacidade_intercambio_pat["submercado_para"] == submercado_para)].reset_index(drop=True)
            tamanho = len(df_limite_par["valor"].tolist())
            if len(df_patamar_par["valor"].tolist()) != tamanho or len(df_capacidade_par["valor"].tolist()) != tamanho:
                print(f"Erro: Dados faltando para o par ({submercado_de}, {submercado_para}) no patamar {pat}.")
                print("Limite: ", df_limite_par)
                print("Patamar: ", df_patamar_par)
                print("Capacidade: ", df_capacidade_par)
                exit(1)

            df_sanidade_limite_intercambio_patamar = df_limite_par.copy()
            df_sanidade_limite_intercambio_patamar = df_sanidade_limite_intercambio_patamar.drop(columns=["valor"])

            df_sanidade_limite_intercambio_patamar["calculado"] = (df_limite_par["valor"] * df_patamar_par["valor"]).round(0)
            df_sanidade_limite_intercambio_patamar["esperado"] = df_capacidade_par["valor"]
            df_sanidade_limite_intercambio_patamar["erro"] =df_sanidade_limite_intercambio_patamar["calculado"] - df_sanidade_limite_intercambio_patamar["esperado"]
            lista_df.append(df_sanidade_limite_intercambio_patamar)


    df_resultado_teste_capacidade_intercambio = pd.concat(lista_df).reset_index(drop=True)
    df_resultado_teste_capacidade_intercambio.to_csv(join(caminho_testes_eco, "capacidade_intercambio.csv"), index=False)
    print("CAPACIDADES DE INTERCAMBIO ENTRE OS SUBSISTEMAS - ERRO: ", df_resultado_teste_capacidade_intercambio["erro"].sum())


def leitura_capacidade_intercambio_entre_subsistemas(caminho_TesteBase):
    lista_df = []
    with open(caminho_TesteBase+"/pmo.dat", "r", encoding="latin1") as f:
        patamar = None
        sistema1 = sistema2 = None
        flag = 0
        leitura_arquivo_dados_intercambio = 0
        for linha in f:
            linha = linha.strip()
            # Detecta início de um novo bloco com patamar
            if "CAPACIDADES DE INTERCAMBIO ENTRE OS SUBSISTEMAS" in linha:
                flag = 1

            if flag == 1:
                if(linha.startswith("SISTEMA")):
                    match = re.search(r"SISTEMA\s+(\d+):\s+(\w+)\s+SISTEMA\s+(\d+):\s+(\w+)\s+PATAMAR:\s+(\d+)", linha)

                    if match:
                        sistema1_num = int(match.group(1))
                        sistema1_name = match.group(2)
                        sistema2_num = int(match.group(3))
                        sistema2_name = match.group(4)
                        patamar = int(match.group(5))
                if "-> " in linha:
                    partes = re.split(r'\s+|(?<=\d)\s*->\s*(?=\d)', linha)
                    if len(partes) == 15:
                        ano = int(partes[0])
                        sbm_de = int(partes[1])
                        sbm_para = int(partes[2])
                        for i in range(3, 15):
                            valor = float(partes[i])
                            lista_df.append(
                                pd.DataFrame(
                                    {
                                        "ano": ano,
                                        "mes": i-2,
                                        "data": datetime(ano, i-2, 1),
                                        "sbm_de": sbm_de,
                                        "sbm_para": sbm_para,
                                        "patamar": patamar,
                                        "sistema1": sistema1_name,
                                        "sistema2": sistema2_name,
                                        "valor": valor
                                    }, index=[0]
                                )
                            )
                        
                    elif len(partes) == 14:
                        sbm_de = int(partes[0])
                        sbm_para = int(partes[1])
                        for i in range(2, 14):
                            valor = float(partes[i])
                            lista_df.append(
                                pd.DataFrame(
                                    {
                                        "ano": ano,
                                        "mes": i-1,
                                        "data": datetime(ano, i-1, 1),
                                        "sbm_de": sbm_de,
                                        "sbm_para": sbm_para,
                                        "patamar": patamar,
                                        "sistema1": sistema1_name,
                                        "sistema2": sistema2_name,
                                        "valor": valor
                                    }, index=[0]
                                )
                            )


            if "DADOS DE CARGA ADICIONAL DE ENERGIA" in linha:
                flag = 0
                break
    df_capacidade_intercambio = pd.concat(lista_df).reset_index(drop=True)
    return df_capacidade_intercambio