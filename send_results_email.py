import os
import logging
from datetime import datetime
import pytz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv
from gather_info import df_email_info, startTime
import pandas as pd
import numpy as np
from google.cloud import bigquery, storage

#load environmental variables
load_dotenv()
API_key = os.environ.get('SENDGRID_API_KEY')

#functions
def convert_filetime(dateString):
    date_time_obj = datetime.strptime(dateString, '%Y-%m-%d %H:%M:%S.%f')
    return date_time_obj.strftime("%Y%m%d_%H_%M_%S_%f")

def error_email(note):
    message = Mail(
        from_email='Results@testandgo.com',
        to_emails= 'dev_icatt@lts.com',
        subject='Result Email Error',
        html_content=f'''
                    <p>Issue with Result Email code has occured. {note} Please check logs.</p>
                    '''
    )
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        logger.debug(response.status_code)
        logger.debug(response.body)
        logger.debug(response.headers)
    except Exception as e:
        logger.exception(e.message)

def send_email(patient_name, participant_email, support_contact, study_json, API_key):
    message = Mail(
        from_email='Results@testandgo.com',
        to_emails= participant_email,
    )
    
    message.dynamic_template_data = {
        "name": patient_name, 
        "support_info": support_contact, 
        "study": study_json
        }
    message.template_id = 'd-14fd5cf89f6b43faa48a764d8657f2dc'  
 
    try:
        sg = SendGridAPIClient(API_key)
        response = sg.send(message)
        logger.info(response.status_code)
        logger.info(response.body)
        logger.info(response.headers)
        if response.status_code == 202:
            return "success"
    except Exception as e:
        logger.exception(e.message)

def convert_timestamp(dateString):
    date_time_obj = datetime.strptime(dateString, '%Y-%m-%d %H:%M:%S.%f')
    timestamp_timezone = str(date_time_obj.astimezone(pytz.timezone('America/New_York')))
    return timestamp_timezone

def upload_csv(csv_file):
    table_id = 'lts-palantir-hhs-exchange.kiosk.recover_reporting'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_account.json'
    client = bigquery.Client()
    load_job_configuration = bigquery.LoadJobConfig()
    load_job_configuration.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    load_job_configuration.schema = [
        bigquery.SchemaField('sample_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('email_sent', 'TIMESTAMP', mode='NULLABLE')
    ]

    # load_job_configuration.autodetect = True #schema provided above
    load_job_configuration.source_format = bigquery.SourceFormat.CSV
    load_job_configuration.skip_leading_rows = 1
    load_job_configuration.allow_quoted_newlines = True

    with open(csv_file, 'rb') as source_file:
        upload_job = client.load_table_from_file(
            source_file,
            destination=table_id,          
            location='us-central1',
            job_config=load_job_configuration
        )

    logger.info(upload_job.result())

def upload_logs(file):
    #define configurations
    bucket_name = 'recover-reporting'
    upload_file = file

    #Upload to Google cloud storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(upload_file)
    blob.upload_from_filename(upload_file)


if __name__ == "__main__":
    logger = logging.getLogger(__name__) 
    logger.setLevel(logging.DEBUG) 

    formatter = logging.Formatter('%(levelname)s %(filename)s %(asctime)s %(message)s')
    filename = f"logs/{startTime} send_email.log"
    filehandler = logging.FileHandler(filename=filename)
    filehandler.setFormatter(formatter)

    logger.addHandler(filehandler)

    logger.info('***************************************')
    logger.info('New Program Run for send_results_email')
    logger.info("Process started at: " + startTime)
    logger.info('***************************************')

    participants_to_email = df_email_info

    #send emails
    emailed_samples = []
    for i,r in participants_to_email.iterrows():
        patient_name = participants_to_email.at[i, 'participant_name']
        participant_email = participants_to_email.at[i, 'participant_email']
        study_json = participants_to_email.at[i, 'study_json']
        support_contact = '(800)402-0000'
        status = send_email(patient_name, participant_email, support_contact, study_json, API_key)
        if status == "success":
            emailed_samples.append(participants_to_email.at[i, 'sample_id'])

    #upload sent samples to bigquery
    #create test dataframe
    df_recover_reporting = pd.DataFrame (
        {
            'sample_id': np.array([],dtype=object),
            'email_sent': np.array([],dtype=object)
        }
    )

    df_recover_reporting['sample_id'] = emailed_samples
    df_recover_reporting['email_sent'] = convert_timestamp(str(datetime.now()))

    #export dataframe to csv
    recover_report = f'logs/successful_sends_{startTime}.csv'
    df_recover_reporting.to_csv(recover_report, index=False)
    
    
    #audit
    successful_sends = len(emailed_samples)
    email_sent_target = len(participants_to_email['participant_name'].tolist()) 
    if successful_sends != email_sent_target:
        content=f'{email_sent_target}  emails were supposed to be sent. {successful_sends} were sent.'
        error_email(content)
    else:
        #load csv into bigquery
        upload_csv(recover_report)

    logger.info('***************************************')
    logger.info('Process completed')
    logger.info('Successful Emails Sent: %s', successful_sends)
    logger.info('***************************************')

    upload_logs(filename)
    upload_logs(recover_report)