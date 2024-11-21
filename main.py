import time
from app.config import settings
from app.bigquery import ask_table_exist, upload_table_on_bigquery, read_and_upload_table_on_bigquery, retrieve_last_dttm_download, join_feedback_group_tables, delete_table
from app.medallia_api import get_medallia_feedbakcs_rule_id
from app.utils import today_now, add_1_second
from app.handle_list import handle_list

def main():

    time_start = time.time()

    project_id = settings.project_id
    dataset_name = settings.dataset_name
    table_name_feedbacks = settings.table_name_feedbacks
    table_name_group = settings.table_name_group
    table_name_feedbacks_flagged = settings.table_name_feedbacks_flagged
    username_medallia = settings.username_medallia
    password_medallia = settings.password_medallia
    rule_id_medallia = settings.rule_id_medallia
    
    today_now_str = today_now()

    response = ask_table_exist(project_id, dataset_name, table_name_feedbacks)
    
    #maintenance flow
    if response:
        last_dttm_download = retrieve_last_dttm_download(project_id, dataset_name, table_name_feedbacks)
        dt_start = add_1_second(last_dttm_download)
        feedbacks = get_medallia_feedbakcs_rule_id(username=username_medallia, password=password_medallia, dt_start=dt_start, dt_end=today_now_str, rule_id=rule_id_medallia)
        result = upload_table_on_bigquery(project_id, dataset_name, table_name_feedbacks, feedbacks, "append")
        if "Schema does not match" in result or "An error occurred: Could not convert DataFrame to Parquet" in result:
            read_and_upload_table_on_bigquery(project_id, dataset_name, table_name_feedbacks, feedbacks, "replace")

    #start flow
    else:
        feedbacks = get_medallia_feedbakcs_rule_id(username=username_medallia, password=password_medallia, dt_start="2024-06-30 00:00:01", dt_end=today_now_str, rule_id=rule_id_medallia)
        result = upload_table_on_bigquery(project_id, dataset_name, table_name_feedbacks, feedbacks, "replace")

    response_handle_list_function = ask_table_exist(project_id, dataset_name, table_name_group)
    if response_handle_list_function == False:
        group_flags = handle_list()
        result = upload_table_on_bigquery(project_id, dataset_name, table_name_group, group_flags, "replace")
    
    response_delete_table_function = ask_table_exist(project_id, dataset_name, table_name_feedbacks_flagged)
    if response_delete_table_function:
        delete_table(project_id, dataset_name, table_name_feedbacks_flagged)
    join_feedback_group_tables(project_id, dataset_name, table_name_feedbacks, table_name_group, table_name_feedbacks_flagged)

    time_end = time.time()
    elapsed_time = round((time_end - time_start)/60, 2)
    print(f"Program ended. Elapsed time: {elapsed_time} minutes")

if __name__ == "__main__":
    main()