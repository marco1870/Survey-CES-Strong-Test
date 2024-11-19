import pandas as pd
import os
from app.utils import hashing_sha256
# from typing import Dataframe

def handle_list() -> dict:
    
    """
    Summary:
        Handle the list in file folder in order to be uploaded into bigquery
    Arg:
        None
    Return:
        dict
    """

    # files = [file for file in os.listdir("file")]
    # print(files)

    # pd.set_option('display.max_colwidth', None)

    df_lista_1 = pd.read_csv("file/APP_ListaPrenotatore1.csv", dtype=str, index_col=False)
    df_lista_2 = pd.read_csv("file/APP_ListaPrenotatore2.csv", dtype=str, index_col=False)
    df_lista_3 = pd.read_csv("file/APP_ListaPrenotatore3.csv", dtype=str, index_col=False)
    df_lista_4 = pd.read_csv("file/APP_ListaPrenotatore4.csv", dtype=str, index_col=False)
    df_lista_5 = pd.read_csv("file/APP_ListaPrenotatore5.csv", dtype=str, index_col=False)
    df_lista_6 = pd.read_csv("file/APP_ListaPrenotatore6.csv", dtype=str, index_col=False)
    df_lista_7 = pd.read_csv("file/APP_ListaPrenotatore7.csv", dtype=str, index_col=False)
    df_lista_8 = pd.read_csv("file/APP_ListaPrenotatore8.csv", dtype=str, index_col=False)
    df_lista_9 = pd.read_csv("file/APP_ListaPrenotatore9.csv", dtype=str, index_col=False)
    df_lista_9_1 = pd.read_csv("file/APP_ListaPrenotatore9_1.csv", dtype=str, index_col=False)
    df_lista_10 = pd.read_csv("file/APP_ListaPrenotatore10.csv", dtype=str, index_col=False)
    df_lista_11 = pd.read_csv("file/APP_ListaPrenotatore11.csv", dtype=str, index_col=False)
    df_lista_CTRL = pd.read_csv("file/APP_ListaPrenotatore_CTRL_APP.csv", dtype=str, index_col=False)
    df_lista_Other = pd.read_csv("file/APP_ListaPrenotatore_Other_APP.csv", dtype=str, index_col=False, usecols=["msisdn"])
    df_lista_Strong_Test = pd.read_csv("file/APP_ListaPrenotatore_Strong_Test.csv", dtype=str, index_col=False)
    df_lista_Strong_Test_New = pd.read_csv("file/APP_ListaPrenotatore_Strong_Test_New.csv", dtype=str, index_col=False)

    df_lista_1["sha256"]=df_lista_1["msisdn"].apply(hashing_sha256)
    df_lista_2["sha256"]=df_lista_2["msisdn"].apply(hashing_sha256)
    df_lista_3["sha256"]=df_lista_3["msisdn"].apply(hashing_sha256)
    df_lista_4["sha256"]=df_lista_4["msisdn"].apply(hashing_sha256)
    df_lista_5["sha256"]=df_lista_5["msisdn"].apply(hashing_sha256)
    df_lista_6["sha256"]=df_lista_6["msisdn"].apply(hashing_sha256)
    df_lista_7["sha256"]=df_lista_7["msisdn"].apply(hashing_sha256)
    df_lista_8["sha256"]=df_lista_8["msisdn"].apply(hashing_sha256)
    df_lista_9["sha256"]=df_lista_9["msisdn"].apply(hashing_sha256)
    df_lista_9_1["sha256"]=df_lista_9_1["msisdn"].apply(hashing_sha256)
    df_lista_10["sha256"]=df_lista_10["msisdn"].apply(hashing_sha256)
    df_lista_11["sha256"]=df_lista_11["msisdn"].apply(hashing_sha256)
    df_lista_CTRL["sha256"]=df_lista_CTRL["msisdn"].apply(hashing_sha256)
    df_lista_Other["sha256"]=df_lista_Other["msisdn"].apply(hashing_sha256)
    df_lista_Strong_Test["sha256"]=df_lista_Strong_Test["msisdn"].apply(hashing_sha256)
    df_lista_Strong_Test_New["sha256"]=df_lista_Strong_Test_New["msisdn"].apply(hashing_sha256)

    df_lista_1 = df_lista_1.drop(columns=["msisdn"])
    df_lista_2 = df_lista_2.drop(columns=["msisdn"])
    df_lista_3 = df_lista_3.drop(columns=["msisdn"])
    df_lista_4 = df_lista_4.drop(columns=["msisdn"])
    df_lista_5 = df_lista_5.drop(columns=["msisdn"])
    df_lista_6 = df_lista_6.drop(columns=["msisdn"])
    df_lista_7 = df_lista_7.drop(columns=["msisdn"])
    df_lista_8 = df_lista_8.drop(columns=["msisdn"])
    df_lista_9 = df_lista_9.drop(columns=["msisdn"])
    df_lista_9_1 = df_lista_9_1.drop(columns=["msisdn"])
    df_lista_10 = df_lista_10.drop(columns=["msisdn"])
    df_lista_11 = df_lista_11.drop(columns=["msisdn"])
    df_lista_CTRL = df_lista_CTRL.drop(columns=["msisdn"])
    df_lista_Other = df_lista_Other.drop(columns=["msisdn"])
    df_lista_Strong_Test = df_lista_Strong_Test.drop(columns=["msisdn"])
    df_lista_Strong_Test_New = df_lista_Strong_Test_New.drop(columns=["msisdn"])

    df_CTRL_Other = pd.merge(df_lista_CTRL, df_lista_Other, how="inner", on="sha256")
    df_CTRL_Strong = pd.merge(df_lista_CTRL, df_lista_Strong_Test, how="inner", on="sha256")
    df_Strong_Other = pd.merge(df_lista_Strong_Test, df_lista_Other, how="inner", on="sha256")
    df_StrongNew_Other = pd.merge(df_lista_Strong_Test_New, df_lista_Other, how="inner", on="sha256")
    df_StrongNew_Strong = pd.merge(df_lista_Strong_Test_New, df_lista_Strong_Test, how="inner", on="sha256")
    df_StrongNew_CTRL = pd.merge(df_lista_Strong_Test_New, df_lista_CTRL, how="inner", on="sha256")
    print("Inner join between group list. (Shoub be empty)",df_CTRL_Other, df_CTRL_Strong, df_Strong_Other, df_StrongNew_Other, df_StrongNew_Strong, df_StrongNew_CTRL)

    df_lista_1.insert(1, "run", "run_1")
    df_lista_2.insert(1, "run", "run_2")
    df_lista_3.insert(1, "run", "run_3")
    df_lista_4.insert(1, "run", "run_4")
    df_lista_5.insert(1, "run", "run_5")
    df_lista_6.insert(1, "run", "run_6")
    df_lista_7.insert(1, "run", "run_7")
    df_lista_8.insert(1, "run", "run_8")
    df_lista_9.insert(1, "run", "run_9")
    df_lista_9_1.insert(1, "run", "run_9")
    df_lista_10.insert(1, "run", "run_10")
    df_lista_11.insert(1, "run", "run_11")
    df_lista_CTRL.insert(1, "group", "CTRL APP")
    df_lista_Other.insert(1, "group", "OTHER APP")
    df_lista_Strong_Test.insert(1, "group", "STRONG TEST")
    df_lista_Strong_Test_New.insert(1, "group", "STRONG TEST NEW")

    df_all_run = pd.concat([df_lista_1,
                        df_lista_2,
                        df_lista_3,
                        df_lista_4,
                        df_lista_5,
                        df_lista_6,
                        df_lista_7,
                        df_lista_8,
                        df_lista_9,
                        df_lista_9_1,
                        df_lista_10,
                        df_lista_11])

    print("Tutte le run pre deduplica:",df_all_run.info())
    df_all_run = df_all_run.drop_duplicates(subset="sha256", keep="last")
    print("Tutte le run post deduplica:",df_all_run.info())

    df_lista_CTRL = df_lista_CTRL.drop_duplicates(subset="sha256", keep="first")
    df_lista_Other = df_lista_Other.drop_duplicates(subset="sha256", keep="first")
    df_lista_Strong_Test = df_lista_Strong_Test.drop_duplicates(subset="sha256", keep="first")
    df_lista_Strong_Test_New = df_lista_Strong_Test_New.drop_duplicates(subset="sha256", keep="first")

    df_all_group = pd.concat([df_lista_CTRL, df_lista_Other, df_lista_Strong_Test, df_lista_Strong_Test_New])

    df_all_final = df_all_run.merge(df_all_group, how="left", on="sha256")
    dict_all_final = df_all_final.to_dict(orient='dict')

    return dict_all_final