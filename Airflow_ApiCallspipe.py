import requests
import io
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG, Variable
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.macros.plugins import send_alert
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import boto3
from datetime import datetime,date,time
import datetime as dt
import pandas as pd
import shutil
import os
import csv
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator


dag_owner = 'franko'
dag_name = 'test_api_hubspot'
test = True

aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID') 
aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')
snowflake_hook = SnowflakeHook(snowflake_conn_id = "snowflake_conn", database = 'RAWDATA')
vars = Variable.get('HUBSPOT_LOAD_OBJ_CONFIG', deserialize_json = True)
input_files_path = vars['contacts_created_dir']  # dir for vid contacts
output_file_name_created = "contacts_created.csv"
output_file_name_updated = "contacts_updated.csv"
hubspot_api_key = Variable.get('HUBSPOT_API_KEY')
#currently_date =  datetime.combine(date.today()- dt.timedelta(days=1), time()) ##yesterday day 
currently_date = Variable.get('HUBSPOT_CONTACT_LASTEXEC')
currently_date = datetime.strptime(currently_date, '%Y-%m-%d %H:%M:%S')
accum_req = 0
merge_query = [""" use raw_data.hubspot;""",
    """
MERGE INTO raw_data.hubspot.test_hubspot_api_contacts USING
(
  SELECT $1 canonical_vid,
         $77 ip_city,
         $63 email,
         $43 firstname,
         $82 lifecyclestage
    FROM  @RAW_HUBSPOT_CONTACTS     ( pattern=>'.*contacts.*[.]csv')           
) bar ON test_hubspot_api_contacts.id = bar.canonical_vid
    WHEN MATCHED THEN
        UPDATE SET 
                PROPERTY_EMAIL = bar.email, 
                PROPERTY_IP_CITY=  bar.ip_city,
                PROPERTY_FIRSTNAME= bar.firstname,
                PROPERTY_LIFECYCLESTAGE= bar.lifecyclestage
    WHEN NOT MATCHED THEN
        INSERT
            ( id,
              PROPERTY_IP_CITY, 
              PROPERTY_EMAIL,
              PROPERTY_FIRSTNAME,
              PROPERTY_LIFECYCLESTAGE) 
         VALUES
            ( bar.canonical_vid,
              bar.ip_city, 
              bar.email,
              bar.firstname,
              bar.lifecyclestage
            );
""",
]
default_args = dict(
    owner = dag_owner,
    start_date = datetime(2021, 3, 17))

dag = DAG(dag_name,
        default_args = default_args,
        catchup = False,
        schedule_interval = "0 */3 * * *") 

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

@send_alert( test = test)
def get_latest_Created_contacts(logger,*args,**kwargs):
    import time
    """
    Connect to Hubspot with API Key and get recent contacts created.    
    """
    #url = "https://api.hubapi.com/contacts/v1/contact/batch"
    url = "https://api.hubapi.com/contacts/v1/lists/all/contacts/recent"
    list_created=[]
    has_more = False 
    accum_req = 0
    ti = kwargs['ti']
    #First Call    
    hubspot_client = {"hapikey": hubspot_api_key,"count":100}
    response = requests.request("GET", url,   params=hubspot_client)
    accum_req=accum_req+1
    cnt_req=0
    response_json = response.json()

    has_more = response_json["has-more"]
    time_offset = response_json["time-offset"]
    last_created_contact = datetime.fromtimestamp(int(str(time_offset)[:-3]))    

    last_execution = response_json["contacts"][0]
    last_execution = last_execution["addedAt"]
    last_execution = datetime.fromtimestamp(int(str(last_execution)[:-3]))
    logger.info(f'Last execution:  {last_execution}')

    for itm in response_json["contacts"]:            
        added_at = itm["addedAt"]
        compare_added_at = datetime.fromtimestamp(int(str(added_at)[:-3]))
        if (currently_date < compare_added_at) :
            list_created.append(itm["canonical-vid"])  
        #logger.info(response.text)

    #Pagination
    while (has_more is True) and (currently_date < last_created_contact): #only records from currently date
        hubspot_client = {"hapikey": hubspot_api_key, "count":100, "timeOffset":time_offset}
        response = requests.request("GET", url,   params=hubspot_client)
        accum_req=accum_req+1
        try :
            response_json = response.json()
            for itm in response_json["contacts"]:            
                added_at = itm["addedAt"]
                compare_added_at = datetime.fromtimestamp(int(str(added_at)[:-3]))
                if (currently_date < compare_added_at) :
                    list_created.append(itm["canonical-vid"])                
                    
                    
            has_more = response_json["has-more"]
            time_offset = response_json["time-offset"]
            last_created_contact = datetime.fromtimestamp(int(str(time_offset)[:-3]))

            cnt_req+=1
            if cnt_req ==10:
                time.sleep(1)
        except:
            logger.info('Error parsing')
            logger.info(response.text)
            logger.info(response.status_code)
            if response.status_code <= 500:
                break
            else:
                time.sleep(10)
            
        

        #logger.info(response.text)

    Variable.set("HUBSPOT_CONTACT_LASTEXEC_STAGE",last_execution)
    dict = {'vid': list_created}  
       
    df = pd.DataFrame(dict)     
        
    # saving the dataframe 
    df.to_csv("{}/{}".format(input_files_path,output_file_name_created),index=False)
    
    logger.info(f'Accumulated requests : {accum_req}')
    ti.xcom_push(key = "accum_req", value = accum_req)
    return response.status_code   #Response code will be 202 if success, 400 if failure. https://developers.hubspot.com/docs/methods/contacts/batch_create_or_update

@send_alert( test = test)
def get_latest_Updated_contacts(logger,*args,**kwargs):
    import time
    """
    Connect to Hubspot with API Key and get recently updated contacts.
    """
    
    url = "https://api.hubapi.com/contacts/v1/lists/recently_updated/contacts/recent"
    list_updated=[]
    ti = kwargs['ti']
    has_more = False
    accum_req = ti.xcom_pull(task_ids = f"get_latest_Created_contacts", key = 'accum_req')
    hubspot_client = {"hapikey": hubspot_api_key,"count":100}    
    response = requests.request("GET", url,   params=hubspot_client)
    accum_req=accum_req+1
    cnt_req=0
    response_json = response.json()
    has_more = response_json["has-more"]
    time_offset = response_json["time-offset"]
    last_updated_contact = datetime.fromtimestamp(int(str(time_offset)[:-3])) 
    
    for itm in response_json["contacts"]:            
        added_at = itm["addedAt"]
        compare_added_at = datetime.fromtimestamp(int(str(added_at)[:-3]))
        if (currently_date < compare_added_at) :
            list_updated.append(itm["canonical-vid"])  
    
    #logger.info(response.text)
    

    #Pagination
    while (has_more is True) and (currently_date < last_updated_contact): #only records from currently date
        hubspot_client = {"hapikey": hubspot_api_key, "count":100, "timeOffset":time_offset}
        response = requests.request("GET", url,   params=hubspot_client)
        accum_req=accum_req+1
        #logger.info(response.text)
        try:
            response_json = response.json()
            for itm in response_json["contacts"]:            
                added_at = itm["addedAt"]
                compare_added_at = datetime.fromtimestamp(int(str(added_at)[:-3]))
                if (currently_date < compare_added_at) :
                    list_updated.append(itm["canonical-vid"]) 
            has_more = response_json["has-more"]
            time_offset = response_json["time-offset"]
            last_updated_contact = datetime.fromtimestamp(int(str(time_offset)[:-3]))   

            cnt_req+=1
            if cnt_req ==10:
                time.sleep(1)
        except:
            logger.info("ERROR in response")
            logger.info(response.text)
            logger.info(response.status_code)
            if response.status_code <= 500:
                break
            else:
                time.sleep(10)
    
    dict = {'vid': list_updated} 
    df = pd.DataFrame(dict)     
    # saving the dataframe 
    df.to_csv("{}/{}".format(input_files_path,output_file_name_updated),index=False)
    logger.info(f'Accumulated requests : {accum_req}')
    ti.xcom_push(key = "accum_req", value = accum_req)
    return response.status_code   #Response code will be 202 if success, 400 if failure. https://developers.hubspot.com/docs/methods/contacts/batch_create_or_update

@send_alert( test = test)
def get_contact_properties(logger,*args,**kwargs):
    import time
    """
    Connect to Hubspot with API Key and get recently updated contacts.
    """
    df_updared = pd.read_csv(f"{input_files_path}/{output_file_name_updated}")
    df_created = pd.read_csv(f"{input_files_path}/{output_file_name_created}")

    list_updated = df_updared['vid'].tolist()
    list_created = df_created['vid'].tolist()

    list_vid = list_updated + list_created
    list_vid = list(dict.fromkeys(list_vid))

    ti = kwargs['ti']
    logger.info(f'Vid list long : {len(list_vid)}')
    accum_req = ti.xcom_pull(task_ids = f"get_latest_Updated_contacts", key = 'accum_req')    
    list_vid_100 = (list(chunks(list_vid, 100)))

    contact_list=[]
    cnt_req = 0
    for itm in list_vid_100:        

        url = f"https://api.hubapi.com/contacts/v1/contact/vids/batch"

        hubspot_client = {"hapikey": hubspot_api_key,"vid":itm}    
        response = requests.request("GET", url,   params=hubspot_client)
        accum_req=accum_req+1

        response_json = response.json()

        
        for key in response_json: 
            dict2 = response_json.get(key)
            for key2 in dict2:        
                if key2 =='canonical-vid':
                    contact_dict={}
                    #print(f' {key2} ::  {dict2.get(key2)}')
                    contact_dict[key2]= dict2.get(key2)
                if key2 =='properties':
                    dict3 = dict2.get(key2)
                    for key3 in dict3:
                        #print(f' {key3} ::  {dict3.get(key3).get("value")}')
                        contact_dict[key3]= dict3.get(key3).get("value")
                    contact_list.append(contact_dict)        
    

        #logger.info(response.text)
        cnt_req+=1
        if cnt_req ==10:
            time.sleep(1)

    df = pd.DataFrame.from_dict(contact_list)
    '''
    updated_columns = {}
    for column in list(df.columns):
        new_column = {column:column.replace(" ","_").lstrip("(").rstrip(")").replace("xlsx","csv").replace("-","_").replace("(","").replace(")","").replace(".","").upper()}
        updated_columns.update(new_column)
    
    df.rename(columns = updated_columns,inplace=True)
    '''
    df.to_csv(f"{input_files_path}/contacts.csv".format(input_files_path),index=False,quotechar='"', quoting=csv.QUOTE_ALL)
    logger.info(f'Accumulated requests : {accum_req}')
    ti.xcom_push(key = "accum_req", value = accum_req)
    return response.status_code   #Response code will be 202 if success, 400 if failure. https://developers.hubspot.com/docs/methods/contacts/batch_create_or_update

@send_alert(test = test)
def load_to_s3(logger, file_name, ds_nodash, **kwargs):
    ti = kwargs['ti']    
    s3 = boto3.resource('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key)
    #set YYYYMM for bucket
    yyyymmdd = currently_date.strftime("%Y%m%d")

    bucket = 'roofstock-data-lake'; key = f'Airflow/HUBSPOT/{yyyymmdd}/contacts.csv'
    logger.info(f"Uploading {file_name} from Airflow local TO S3 location s3://{bucket}/{key}")
    s3.Bucket(bucket).upload_file(f"{input_files_path}/contacts.csv", key) #send to updated weekly files bucket

@send_alert( test = test)
def s3_to_snowflake(logger, file_name, ds_nodash, **kwargs):
    
    ti = kwargs['ti']


    #table = dict[filename] for filename in dict.keys() if filename = file_name
    schema = "HUBSPOT"
    yyyymmdd = currently_date.strftime("%Y%m%d")


    bucket = 'roofstock-data-lake'; key = f'Airflow/HUBSPOT/{yyyymmdd}'
    
    conn = snowflake_hook.get_cursor()
    conn.execute('USE DATABASE RAW_DATA')
    try:
        create_stage_query = f"""CREATE OR REPLACE STAGE RAW_DATA.{schema}.RAW_HUBSPOT_CONTACTS 
                        url = 's3://{bucket}/{key}'
                        credentials=(aws_key_id= '{aws_access_key_id}', aws_secret_key= '{aws_secret_access_key}')
                        file_format=(type=csv skip_header=1 NULL_IF='' FIELD_OPTIONALLY_ENCLOSED_BY='"')"""

        logger.info(create_stage_query)
        conn.execute(create_stage_query)  
    except Exception as e:
        raise AirflowException(e)    
    
@send_alert(test = test)
def completed_flag(logger,  ds_nodash, **kwargs):
    ti = kwargs['ti']    
    logger.info(f"Updating last execution date")
    #yyyymmdd = currently_date.strftime("%Y%m%d")
    stage_date = Variable.get('HUBSPOT_CONTACT_LASTEXEC_STAGE')
    stage_date = datetime.strptime(stage_date, '%Y-%m-%d %H:%M:%S')
    Variable.set("HUBSPOT_CONTACT_LASTEXEC",stage_date)    

get_latest_Created_contacts = PythonOperator( dag = dag,
                        task_id = f"get_latest_Created_contacts",
                        python_callable = get_latest_Created_contacts,
                        provide_context = True
                        #op_kwargs = {"file_name" : file}
                        )    

get_latest_Updated_contacts = PythonOperator( dag = dag,
                        task_id = f"get_latest_Updated_contacts",
                        python_callable = get_latest_Updated_contacts,
                        provide_context = True
                        #op_kwargs = {"file_name" : file}
                        )   

get_contact_properties = PythonOperator( dag = dag,
                        task_id = f"get_contact_properties",
                        python_callable = get_contact_properties,
                        provide_context = True
                        #op_kwargs = {"file_name" : file}
                        )   

load_to_s3 = PythonOperator( dag = dag,
                    task_id = f"load_to_s3",
                    python_callable = load_to_s3,
                    provide_context = True,
                    op_kwargs = {"file_name" : "contacts"}
                    )

s3_to_snowflake = PythonOperator( dag = dag,
                    task_id = f"s3_to_snowflake",
                    python_callable = s3_to_snowflake,
                    provide_context = True,
                    op_kwargs = {"file_name" : "contacts"}
                    )

Contacts_merge_snowflake = SnowflakeOperator( dag = dag,
                    task_id = f"Contacts_merge_snowflake",
                    sql = merge_query,
                    snowflake_conn_id="snowflake_conn"                    
                    )

completed_flag = PythonOperator( dag = dag,
                        task_id = f"completed_flag",
                        python_callable = completed_flag,
                        provide_context = True
                        #op_kwargs = {"file_name" : file}
                        )   

dummy = DummyOperator(task_id = "dummy", dag = dag)


dummy >> get_latest_Created_contacts >> get_latest_Updated_contacts >> get_contact_properties >>load_to_s3 >> s3_to_snowflake >> Contacts_merge_snowflake >> completed_flag