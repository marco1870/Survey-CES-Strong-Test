import os
import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import Literal
from datetime import datetime
from app.utils import remove_timezone

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =r"fine-elf-279710-32a5687ecc7b.json"

def ask_table_exist (project_id: str, dateset_name: str, table_name: str) -> bool:

    """
    Summary:
        Asking to bigquery if a table already exist or not
    Arg:
        project_id (str)
        dataset_name (str)
        table_name (str)
    Return:
        respose (bool)
    """

    client = bigquery.Client()

    table_id = project_id+"."+dateset_name+"."+table_name

    try:
        client.get_table(table_id)  # Make an API request.
        response = True
        print("Table {} already exists.".format(table_id))
    except NotFound:
        response = False
        print("Table {} is not found.".format(table_id))
    
    return response

def insert_rows_into_table (project_id: str, dateset_name: str, table_name: str, rows_to_insert: list[dict]) -> None:

    """
    Summary:
        Inser row into a BigQuery table
    Arg:
        project_id: str
        dataset_name: str
        table_name: str
        row_to_insert: list[dict]
    Return:
        None
    """

    client = bigquery.Client()

    table_id = project_id+"."+dateset_name+"."+table_name

    errors = client.insert_rows_json(table_id, rows_to_insert)
    print(errors)

    return None

def upload_table_on_bigquery(project_id: str, dateset_name: str, table_name: str, rows_to_insert: list[dict], if_exists: Literal["fail", "replace", "append"]) -> str:
    """
    Summary:
        The funcion takes as input a list of dictionaries (rows) in order to create a 
        dataframe pandas to be uploaded into bigquery
    Arg:
        project_id: str
        dataset_name: str
        table_name: str
        row_to_insert: list[dict]
        if_exists: Literal["fail", "replace", "append"]
            parameter useful for to_gbq function  
    Return:
        upload_status: str
            usuful in case of error
    """

    table_id = project_id+"."+dateset_name+"."+table_name

    df = pd.DataFrame.from_dict(rows_to_insert)

    upload_status = None

    try:
        pd_gbq.to_gbq(dataframe=df, destination_table=table_id, if_exists=if_exists)
        upload_status = "Table uploaded"
        print(f"Table {table_id} uploaded.")
    except Exception as e:
        upload_status = f"An error occurred: {e}"
        print(upload_status)

    return upload_status

def read_and_upload_table_on_bigquery(project_id: str, dateset_name: str, table_name: str, rows_to_insert: list[dict], if_exists: Literal["fail", "replace", "append"]) -> None:
    """
    Summary:
        The funcion takes as input a list of dictionaries (rows) in order to create a 
        dataframe pandas to be uploaded into bigquery in a new table, before upload the the table the function read 
        an other table and perform a concat function with pandas in order to handle new column
    Arg:
        project_id: str
        dataset_name: str
        table_name: str
        row_to_insert: list[dict]
        if_exists: Literal["fail", "replace", "append"]
            parameter useful for to_gbq function  
    Return:
        None
    """

    table_id = project_id+"."+dateset_name+"."+table_name

    df_update = pd.DataFrame.from_dict(rows_to_insert)

    df_bigquery = pd_gbq.read_gbq(query_or_table=table_id)
    df_bigquery["TIMESTAMP_HD"] = df_bigquery["TIMESTAMP_HD"].apply(remove_timezone)

    df_final_concat = pd.concat([df_bigquery,df_update])
    df_final = df_final_concat.drop_duplicates(subset="FEEDBACKID_HD", keep="first")

    upload_status = None

    try:
        pd_gbq.to_gbq(dataframe = df_final, destination_table=table_id, if_exists=if_exists)
        upload_status = "Read the old table and uploaded a new one"
        print(upload_status)
    except Exception as e:
        upload_status = f"An error occurred: {e}"
        print(upload_status)

    return None

def retrieve_last_dttm_download(project_id: str, dateset_name: str, table_name: str) -> str:
    """
    Summary:
        The funcion retrieve the last datetime of download feedback in order to use the date as input for next download
    Arg:
        project_id: str
        dataset_name: str
        table_name: str
    Return:
        next_download_dttm: str
    """
    
    client = bigquery.Client()
    
    table_id = project_id+"."+dateset_name+"."+table_name

    query = f"""
            SELECT max(DTTM_DOWNLOAD_FEEDBACK) FROM `{table_id}`
            """
    rows = client.query_and_wait(query)
    for row in rows:
        last_download_dttm = row[0]
    
    return last_download_dttm

def delete_table(project_id: str, dateset_name: str, table_name: str) -> None:
    """
    Summary:
        The funcion left join two table
    Arg:
        project_id: str
        dataset_name: str
        table_name: str
    Return:
        None
    """
    
    client = bigquery.Client()
    
    table_id = project_id+"."+dateset_name+"."+table_name

    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print(f"Table {table_id} deleted.")
    
    return None

def join_feedback_group_tables(project_id: str, dateset_name: str, table_name_left: str, table_name_right: str, table_name_flagged: str) -> None:
    """
    Summary:
        The funcion left join two table
    Arg:
        project_id: str
        dataset_name: str
        table_name_left: str
        table_name_right: str
        table_name_flagged: str
    Return:
        None
    """
    
    client = bigquery.Client()
    
    table_id_left = project_id+"."+dateset_name+"."+table_name_left
    table_id_right = project_id+"."+dateset_name+"."+table_name_right
    table_id_new = project_id+"."+dateset_name+"."+table_name_flagged

    query = f"""
            CREATE TABLE `{table_id_new}` AS
            SELECT a.*, 
                   CASE WHEN a.msisdn_sha256 = b.sha256 THEN b.run ELSE "" END AS run,
                   CASE WHEN a.msisdn_sha256 = b.sha256 THEN b.group ELSE "OTHER CB" END AS cluster 
            FROM `{table_id_left}` AS a
            LEFT JOIN `{table_id_right}` AS b
            ON a.msisdn_sha256 = b.sha256 
            """
    rows = client.query_and_wait(query)
    print(f"Table {table_id_new} created.")
    
    return None