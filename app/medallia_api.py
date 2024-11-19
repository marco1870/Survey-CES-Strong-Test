# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 00:34:24 2023

@author: ZZMPEAF
"""
import http.client
import json
from app.utils import print_progress, get_authentication_basic, dt_to_ms, convert_unix_millis_to_local, take_isoweek, take_month, take_year, take_day, take_dt, take_start_dt_week
from typing import Optional

def get_medallia_feedbakcs_rule_id(username: str, password: str, dt_start: str, dt_end: str, rule_id: int ) -> list:
    
    '''
    Summary:
        The aim of get_medallia_api function is to retrieve all the feedbacks and save them in a list

    Arg:
        username (str) : username of Medallia
        password (str) : password of Medallia
        dt_start (str) : starting date from when you want to start to retrieve the feedbacks (format es. 2023-03-01 15:00:01)
        dt_end (str): defining the end data of retrieving feedbacks (format es. 2023-03-01 15:00:01)
        rule_id: the ruleid set on Medallia

    Return:
        feedbacks (list)
    '''

    dt_start_ms = dt_to_ms(dt_start)
    dt_end_ms = dt_to_ms(dt_end)
    credentials = get_authentication_basic(username,password)
    
    feedbacks = []
    url = "nebula-eu.kampyle.com"
    has_next_page = True
    n_page  = 0
        
    while has_next_page:
        endpoint = f"/kma/api/feedback?ruleId={rule_id}&startDate={dt_start_ms}&endDate={dt_end_ms}&page={n_page}"
        conn = http.client.HTTPSConnection(url)
        headers = {"Authorization": f"Basic {credentials}"}
        conn.request("GET", endpoint, headers=headers)
        response = conn.getresponse()
        data = response.read().decode("utf-8")
        json_data = json.loads(data)
        # formatted_json = json.dumps(json_data, indent=2)
        # print(formatted_json)
        
        page_number = json_data["pageNumber"]
        total_pages = json_data["totalPages"]
        n_feedbacks = json_data["totalElements"]
        
        if n_feedbacks == 0:
            return None
        elif page_number == 0:
            print(f"Number of feedbacks: {n_feedbacks}")
        elif page_number == total_pages:
            print(f"Number of feedbacks retrieved: {n_feedbacks}")
        
        print_progress(page_number, total_pages)
        
        has_next_page = json_data["hasNextPage"]
        n_page += 1

        parsed_feedbacks = parse_feedbacks(json_data.get('feedbacks'), dt_end=dt_end)
        feedbacks.extend(parsed_feedbacks)
    
    print(f"Number of feedbacks parsed: {len(feedbacks)}")
   
    return feedbacks

def parse_feedbacks(feedbacks: list, dt_end: str) -> list:
    
    '''
    Summary:
        The aim of parse_feedbacks function is to organize data retrieved from the medallia API.
        The variable are all hardcoded (in this case the ending is: HD) except DynamicData and CustomParameter
    Arg:
        feedbacks (list) : the list of feedbacks
        dt_end (str) : is the value for the column "DTTM_DOWNLOAD_FEEDBACK" useful for the maintenance flow
    Return:

    '''

    parsed_feedbacks = []

    for feedback in feedbacks:
        result = {
            'TIMESTAMP_HD': convert_unix_millis_to_local(feedback.get('creationDate'), result_type="integer"),
            'CREATIONDATE_HD': convert_unix_millis_to_local(feedback.get('creationDate'),result_type="string"),
            'DATE_HD': take_dt(convert_unix_millis_to_local(feedback.get('creationDate'), result_type="string")),
            'WEEK_DT_HD': take_start_dt_week(convert_unix_millis_to_local(feedback.get('creationDate'), result_type="string")),
            'ISOWEEK_HD': take_isoweek(convert_unix_millis_to_local(feedback.get('creationDate'), result_type="string")),
            'DD_HD': take_day(convert_unix_millis_to_local(feedback.get('creationDate'), result_type="string")),
            'MM_HD': take_month(convert_unix_millis_to_local(feedback.get('creationDate'), result_type="string"), type="number"),
            'MMM_HD': take_month(convert_unix_millis_to_local(feedback.get('creationDate'), result_type="string"), type="letter"),
            'YYYY_HD': take_year(convert_unix_millis_to_local(feedback.get('creationDate'), result_type="string")),
            'FEEDBACKID_HD': feedback.get('id'),
            'FORMNAME_HD': feedback.get('form').get('name'),
            'FORMID_HD': feedback.get('form').get('id'),
            'URL_HD': feedback.get('url'),
            'IP_HD': feedback.get('ip'),
            
            'ID_USER_AGENT_HD': feedback.get('userAgentData').get('id'),
            'DEVICE_VENDOR_USER_AGENT_HD': feedback.get('userAgentData').get('deviceVendor'),
            'DEVICE_MODEL_USER_AGENT_HD': feedback.get('userAgentData').get('deviceModel'),
            'DEVICE_MKT_NAME_USER_AGENT_HD': feedback.get('userAgentData').get('deviceMarketingName'),
            'PRIMARY_HARDWARE_TYPE_USER_AGENT_HD': feedback.get('userAgentData').get('primaryHardwareType'),
            'SCREEN_RESOLUTION_USER_AGENT_HD': feedback.get('userAgentData').get('screenResolution'),
            'OS_NAME_USER_AGENT_HD': feedback.get('userAgentData').get('osName'),
            'OS_VERSION_USER_AGENT_HD': feedback.get('userAgentData').get('osVersion'),
            'LANGUAGE_USER_AGENT_HD': feedback.get('userAgentData').get('language'),
            'LANGUAGE_LOCALE_USER_AGENT_HD': feedback.get('userAgentData').get('languageLocale'),
            'BROWSER_NAME_USER_AGENT_HD': feedback.get('userAgentData').get('browserName'),
            'BROWSER_VERSION_USER_AGENT_HD': feedback.get('userAgentData').get('browserVersion'),
            'USABLE_DISPLAY_RESOLUTION_USER_AGENT_HD': feedback.get('userAgentData').get('usableDisplayResolution'),
            
            'COUNTRY_HD': feedback.get('location').get('country'),
            'REGION_HD': feedback.get('location').get('region'),
            'CITY_HD': feedback.get('location').get('city'),
            'COUNTRY_CODE_HD': feedback.get('location').get('countryCode'),
            
        }

        for component in feedback.get('dynamicData').get('pages')[0].get('components'):
            result[component.get('unique_name').upper().replace(" ","_")] = component.get('value')
            

        for param in feedback.get('dynamicData').get('customParams'):
            result[param.get('unique_name').upper().replace(" ","_")] = param.get('value')

        result['DTTM_DOWNLOAD_FEEDBACK'] = dt_end

        parsed_feedbacks.append(result)

    return (parsed_feedbacks)