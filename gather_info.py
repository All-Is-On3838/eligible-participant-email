import os
from google.cloud import bigquery, storage
import pandas as pd
import numpy as np
import requests
import json
import logging
from dotenv import load_dotenv
from datetime import datetime

#load environmental variables
load_dotenv()

#functions
def convert_filetime(dateString):
    date_time_obj = datetime.strptime(dateString, '%Y-%m-%d %H:%M:%S.%f')
    return date_time_obj.strftime("%Y%m%d_%H_%M_%S_%f")

def get_geocode(full_address):
    key=os.environ.get('MAPQUEST_API_KEY')
    api_address = str(full_address)

    parameters = {
        "key":key,
        "location":api_address
    }

    response = requests.get("http://www.mapquestapi.com/geocoding/v1/address", params=parameters)
    data = response.text
    data_json = json.loads(data)['results']
    lat = str(data_json[0]['locations'][0]['latLng']['lat'])
    lng = str(data_json[0]['locations'][0]['latLng']['lng'])
    geocode = lng + "," + lat
    return geocode

def generate_osrm_url(study,patient):
    curl = 'http://router.project-osrm.org/route/v1/driving/'+ study + ";" + patient
    return curl

def get_distance(source, destinations):
    coordinates = str(source) + ";" + str(destinations)
    url = str(f'http://router.project-osrm.org/table/v1/driving/{coordinates}?sources=0&annotations=distance')
    try:
        r = requests.get(url)
        res = r.json()
        distance_m = res['distances'][0]
    except:
        logger.exception('OSRM error')
    return distance_m

def convert_distance(distance_m):
    distance_mi = []
    for m in distance_m[1:]:
        try:
            mi = m * 0.00062137
            distance_mi.append(mi)
        except:
            distance_mi.append(None)
    return distance_mi

def generate_json (list):
    json = []
    for group in list:
        host = group[0]
        address = group[1]
        cohort = group[2]
        group_dict = {
            "host": f'{host}', 
            "address": f'{address}', 
            "cohort": f'{cohort}'}
        json.append(group_dict)
    return json

def check_eligibility(cohort, age, pregnancy):
    if cohort == 'Pediatric' and age >= 18:
        return 'not eligible'
    elif cohort == 'Adult' and age < 18:
        return 'not eligible'
    elif cohort == 'Pregnancy' and pregnancy == 'No':
        return 'not eligible'
    else:
        return 'eligible'

def check_distance(distance_list):
    match = []
    for d_index, distance in enumerate(distance_list):
        if distance != None and distance <= 75:
            match.append(d_index)
        else:
            pass
    return match

def upload_logs(file):
    #define configurations
    bucket_name = 'recover-reporting'
    upload_file = file

    #Upload to Google cloud storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(upload_file)
    blob.upload_from_filename(upload_file)
    

startTime = convert_filetime(str(datetime.now()))
#now we will Create and configure logger 
#rotate logs by adding date
logger = logging.getLogger(__name__) 
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(levelname)s %(filename)s %(asctime)s %(message)s')
filename = f"logs/{convert_filetime(str(datetime.now()))} gather_info.log"
filehandler = logging.FileHandler(filename=filename)
filehandler.setFormatter(formatter)

logger.addHandler(filehandler)

logger.info('***************************************')
logger.info('New Program Run for gather_info')
logger.info("Process started at: " + startTime)
logger.info('***************************************')
  

#now we are going to import data from bigquery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_account.json'
bq_client = bigquery.Client()

#create dataframe to insert table
df_participants= pd.DataFrame()

# brings in resulted data from previous day
query = """
SELECT
  email, 
  INITCAP(CONCAT(first_name, " ", last_name)) AS name, 
  address1.string AS address, 
  city.string AS city, 
  state, 
  zip.string AS zip,
  K.sample_id AS sample_id
FROM `lts-palantir-hhs-exchange.radeas.Patient` P
JOIN `lts-palantir-hhs-exchange.radeas.Kit` K
  ON P.sample_id = K.sample_id
WHERE K.sample_id IN (SELECT test_id
                      FROM `lts-palantir-hhs-exchange.lts_data_transfer.hhs_staging` HHS
                      JOIN `lts-palantir-hhs-exchange.lts_data.site_matrix` SM
                        ON HHS.facility_id = SM.icatt_site_mapping_id
                      WHERE contract_holder = 'ICATT'
                        AND CAST(resulted_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                      )
AND email != 'secure.reports@radeas.com'
AND address1.string IS NOT NULL
"""

query_job = bq_client.query(query)
df_participants = query_job.to_dataframe()

#import studies
df_studies = pd.read_csv(r'enrolling_sites.csv', dtype=str)



df_eligible_participants = pd.DataFrame (
    {
        'participant_row_idx': np.array([],dtype=object),
        'local_studies_idx': np.array([],dtype=object)
    }
)

#if distance is <= 75 mi then add study name to df_detected['local_studies']
for i,row in df_participants.iterrows():
    patient_name = df_participants.at[i,'name']
  
    address = str(df_participants.at[i, 'address']) + "," + str(df_participants.at[i, 'city']) + "," + str(df_participants.at[i, 'state']) + "," + str(df_participants.at[i, 'zip'])
    source = get_geocode(address)
    #get list of coordinations to route distance to patient address
    destinations = [df_studies.at[i, 'LngLat'] for i,r in df_studies.iterrows()]
    geo_destinations = ";".join(destinations)

    #get route distances in meters then convert to miles
    meter_distances = get_distance(source, geo_destinations)
    mile_distances = convert_distance(meter_distances)

    #check for routes less than or equal to 75 miles
    study_match = check_distance(mile_distances)
    
    if len(study_match) > 0:
        df_eligible_participants.loc[len(df_eligible_participants.index)] = [i, study_match]
    else:
        name = df_participants.at[i, 'name']
        logger.info(f'no eligible studies for {name}')




#gather info needed to send email into dataframe
df_email_info = pd.DataFrame (
    {
        'sample_id': np.array([],dtype=object),
        'participant_name': np.array([],dtype=object),
        'participant_email': np.array([],dtype=object),
        'study_json': np.array([],dtype=object)
    }
)

eligible_participant_idx = list(enumerate(df_eligible_participants['participant_row_idx'].tolist()))
for pt in eligible_participant_idx:
    t_row = pt[0]
    p_row_idx = pt[1]
    local_studies = df_eligible_participants.at[t_row, 'local_studies_idx']
    
    if len(local_studies) > 0:
        studies = []
        for idx in local_studies:
            study_host = df_studies.at[idx, 'facility_name']
            study_address = df_studies.at[idx, 'city'] + ", " + df_studies.at[idx, 'state'] + " " + df_studies.at[idx, 'zip']
            if df_studies.at[idx, 'cohort'] == 'Adult':
                study_cohort = 'adult participants'
            elif df_studies.at[idx, 'cohort'] == 'Pediatric':
                study_cohort = 'pediatric participants'
            elif df_studies.at[idx, 'cohort'] == 'Pregnancy':
                study_cohort = 'participants who are or have been pregnant'
            else:
                study_cohort = 'participants'
            #format eligibility 
            studies.append((study_host, study_address, study_cohort))
        study_json = generate_json(studies)
        sample = df_participants.at[p_row_idx, 'sample_id']
        participant_name = df_participants.at[p_row_idx, 'name']
        participant_email = df_participants.at[p_row_idx, 'email']
        df_email_info.loc[len(df_email_info.index)] = [sample, participant_name, participant_email, study_json]

logger.info('***************************************')
logger.info('Process completed')
logger.info('***************************************')

upload_logs(filename)