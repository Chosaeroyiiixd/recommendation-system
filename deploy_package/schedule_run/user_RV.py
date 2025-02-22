import pandas as pd
import mysql.connector
from datetime import date
import numpy as np
import holidays
from pathlib import Path
import zipfile
import json


for_read_file_path = Path(__file__).parent.parent / 'for_read_file'


def reservation_count_fn():
    
    connection = mysql.connector.connect(
    host='data.db.haupcar.com',
    port = 25060,
    user="natdanai.k@haupcar.com",
    password= "nWCu(5.H5M#i&2,F",
    database = 'haupcar'
    )

    query_resv = ''' SELECT
                        u.userid,
                        u.birthdate,
                        u.sex,
                        u.usertype,
                        n.nationalityname as 'nationality',
                        r.stationid,
                        v.vehiclebrand,
                        v.vehiclemodel,
                        v.enginetype,
                        v.vehiclesize,
                        v.vehicletype,
                        r.reservestarttime 
                    FROM
                        haupcar.reservation r
                    INNER JOIN haupcar.vehicle v
                    ON r.vehicleid = v.vehicleid 
                    INNER JOIN haupcar.user u
                    ON r.userid = u.userid
                    LEFT JOIN haupcar.nationality n
                    ON u.nationalityid = n.nationalityid 
                    WHERE v.vehiclemodel != 'ONE'
                    AND v.vehiclecode NOT LIKE 'TEST-%'
                    AND v.vehiclecode NOT LIKE '%PENK%'
                    AND v.vehiclecode NOT LIKE 'SENG-%'
                    AND v.vehiclecode NOT LIKE 'EGAT-%'
                    AND v.vehiclecode NOT LIKE '%-TEST%'
                    AND v.vehiclecode NOT LIKE '%CPOD%'
                    AND v.vehiclecode NOT LIKE '%HASP%'
                    AND v.vehiclecode NOT LIKE '%ADSN%'
                    AND v.vehiclecode NOT LIKE '%EZY%'
                    AND v.vehiclemodel NOT LIKE '%(or equivalent)%'
                    AND v.vehiclemodel NOT LIKE '%PCX%'
                    AND v.vehiclecode NOT LIKE '%FOM%'
                    AND v.vehiclebrand != 'AJ'
                    AND r.category NOT LIKE '%MA%'
                    AND r.category NOT LIKE '%EXTERNAL%'
                    AND r.category NOT LIKE '%EXRENTAL%'
                    AND r.category NOT LIKE '%BATCH%'
                    AND r.reservationstate IN ('FINISH' , 'COMPLETE', 'DRIVE')
                    AND LOWER(u.email) NOT LIKE '%test%'
                    AND LOWER(u.email) NOT LIKE '%penk%'
                    AND LOWER(u.email) NOT LIKE '%haupcar%'
                    AND r.stationid NOT IN (202, 2800, 2801) '''

    cursor = connection.cursor()
    cursor.execute(query_resv)
    rows = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    resv_data = pd.DataFrame(rows, columns=column_names)

    today = date.today()
    resv_data['age'] = resv_data['birthdate'].apply(
    lambda row: today.year - row.year - ((today.month, today.day) < (row.month, row.day)))

    resv_data['age'] = resv_data['age'].apply(
        lambda row: '18-24' if 18 <= row <= 24 else
        '25-45' if 25 <= row <= 45 else
        '45+' if 100 >= row >= 46 else '25-45')
    
    train_station = [47, 77, 97, 150, 154, 158, 207, 214, 215, 216, 225, 226, 243, 257, 273, 325,
                        345, 347, 403, 415, 417, 433, 439, 547, 549, 575, 579, 643, 739, 779, 1869, 
                        1895, 1935, 1959, 2135, 2343, 2378, 2383, 2458, 2486, 2492, 2499, 2545, 2561,
                        2628, 2633, 2645, 2751, 2753, 2760, 2785, 2901, 2903, 2929, 3001, 3067, 3079, 
                        3111, 3146, 3175, 3351, 3356]
    mega_station = [7, 77, 135, 257, 277, 1927, 1929, 1969, 2011, 2017, 2021, 2023, 2031, 2033, 2119,
                        2121, 2123, 2125, 2127, 2129, 2131, 2217, 2231, 2251, 2253, 2277, 2279, 2283, 2307,
                        2319, 2397, 2417, 2419, 2420, 2421, 2424, 2430, 2432, 2433, 2435, 2437, 2439, 2442,
                        2443, 2447, 2454, 2474, 2475, 2496, 2504, 2517, 2520, 2586, 2652, 2661, 2702, 2847,
                        2865, 2877, 2919, 2922, 2964, 2970, 3005, 3060, 3217, 3221, 3331, 3414, 3415, 3416]
        

    resv_data['stationtype'] = resv_data['stationid'].apply(lambda row : 'TRAIN STATION' if row in train_station else
                                        'MEGA STATION' if row in mega_station else 'OTHER STATION')
    resv_data['enginetype'] = resv_data['enginetype'].apply(lambda row : 'EV' if row == 'BEV' else 'Non-EV')

    resv_data['timeofday'] =  resv_data['reservestarttime'].apply(lambda row : 'WORK HOURS' if row.hour in [6, 7, 8, 9 , 10, 11, 12, 13, 14, 15, 16, 17, 18] else
                                                        'LEISURE HOURS' if row.hour in [18, 19, 20, 21, 22] else
                                                        'LATE NIGHT' if row.hour in [23, 0, 1, 2, 3, 4, 5] else np.nan)
    resv_data['dayofweek'] = resv_data['reservestarttime'].apply(lambda row : 'WEEKEND' if row.day_name() in ['Saturday', 'Sunday'] else 'WEEKDAY')
    resv_data['holiday'] = resv_data['reservestarttime'].apply(lambda row : 'Yes' if row.date() in holidays.TH() else 'No')
    
    resv_data = resv_data.drop(columns=['age', 'sex', 'usertype', 'nationality', 'stationid', 'reservestarttime', 'birthdate'], axis=1)
    #-------------------------------------------------------------------------- Merge with Demographic Data -------------------------------------------------------------------#
    ungranular = pd.read_csv(for_read_file_path / 'ungranular.csv')

    resv_data = pd.merge(resv_data, ungranular, on='userid', how='inner')
    resv_data['age'] = resv_data['age'].apply(lambda row : '0-17' if 0 <= row <= 17 else
                                    '18-24' if 18 <= row <= 24 else
                                    '25-45' if 25 <= row <= 45 else
                                    '45+' if 45 <= row else row)
    resv_data['reservation_count'] = resv_data.groupby(list(resv_data.columns)).transform('size')
    resv_data = resv_data.drop_duplicates(list(resv_data.columns), keep = 'first').sort_values(by='reservation_count', ascending=False)
    resv_data = resv_data[['userid', 'age', 'sex', 'nationality', 'usertype', 'vehiclebrand', 'vehiclemodel', 'enginetype', 
                            'vehiclesize', 'vehicletype', 'stationtype', 'timeofday', 'dayofweek', 'holiday', 'reservation_count']]
    return resv_data


def view_count_fn():
    with zipfile.ZipFile(for_read_file_path / "mixpanel_export_data.zip", 'r') as zipf:
        with zipf.open("event_export_view.json") as file:
            mixpanel = json.load(file)

    json_mixpanel = []
    for i in range(len(mixpanel)):
        json_mixpanel.append({'properties' : mixpanel[i]['properties']})

    df_mixpanel = pd.DataFrame(json_mixpanel, columns=['properties'])
    df_mixpanel_normalized = pd.json_normalize(df_mixpanel['properties'])

    #---------- Drop Unnecessary Feature ----------#
    df_mixpanel_normalized_clean = df_mixpanel_normalized.drop(columns=['vehicleId'])

    #---------- Filter null ----------#
    df_mixpanel_normalized_clean = df_mixpanel_normalized_clean.replace('null', np.nan)
    mixpanel_data = df_mixpanel_normalized_clean[
                    df_mixpanel_normalized_clean[['$user_id', 'time', 'vehicleName', 'stationId']].notnull().all(axis=1)
    ][['time', 'vehicleName', 'stationId', '$user_id']]

    #---------- Change datetime format ----------#
    mixpanel_data['time'] = pd.to_datetime(mixpanel_data['time'], unit='s')

    #---------- Change Dtype ----------#
    mixpanel_data['vehicleName'] = mixpanel_data['vehicleName'].astype('str')
    mixpanel_data['stationId'] = mixpanel_data['stationId'].astype('int')
    mixpanel_data['$user_id'] = mixpanel_data['$user_id'].astype('int')

    #---------- Features Engineering ----------#
    mixpanel_data['dayofweek'] = mixpanel_data['time'].apply(lambda row : 'WEEKEND' if row.day_name() in ('Saturday', 'Sunday') else 'WEEKDAY')
    mixpanel_data['timeofday'] = mixpanel_data['time'].apply(lambda row : 'WORK HOURS' if row.hour in [6, 7, 8, 9 , 10, 11, 12, 13, 14, 15, 16, 17, 18] else
                                                            'LEISURE HOURS' if row.hour in [18, 19, 20, 21, 22] else
                                                            'LATE NIGHT' if row.hour in [23, 0, 1, 2, 3, 4, 5] else np.nan)
    mixpanel_data['holiday'] = mixpanel_data['time'].apply(lambda row : 'Yes' if row.date() in holidays.TH() else 'No')
    mixpanel_data['brand'] = mixpanel_data['vehicleName'].str.split(' ').str[0]
    mixpanel_data['model'] = mixpanel_data['vehicleName'].str.split(' ').str[1]

    #---------- Drop row ----------#
    mixpanel_data['vehicleName'] = mixpanel_data['vehicleName'].apply(lambda row: np.nan if 'moo' in row.lower() else row)
    mixpanel_data = mixpanel_data.dropna()  

    #---------- Rename features ----------#
    mixpanel_data = mixpanel_data.rename(columns={'stationId': 'stationid', '$user_id': 'userid', 'brand' : 'vehiclebrand', 'model' : 'vehiclemodel'})

    #---------- Drop features ----------#
    mixpanel_data = mixpanel_data.drop(columns=['time', 'vehicleName'])                 

    #---------- Re-order features ----------#
    mixpanel_data = mixpanel_data[['userid', 'stationid', 'vehiclebrand', 'vehiclemodel', 'timeofday', 'dayofweek', 'holiday']]

    #-------------------------------------------------------------------------- Merge with Demographic Data -------------------------------------------------------------------#
    ungranular = pd.read_csv(for_read_file_path / 'ungranular.csv')

    mixpanel_data = pd.merge(mixpanel_data, ungranular, on='userid', how='inner')

    mixpanel_data['age'] = mixpanel_data['age'].apply(lambda row : '0-17' if 0 <= row <= 17 else
                                    '18-24' if 18 <= row <= 24 else
                                    '25-45' if 25 <= row <= 45 else
                                    '45+' if 45 <= row else row)

    train_station = [47, 77, 97, 150, 154, 158, 207, 214, 215, 216, 225, 226, 243, 257, 273, 325,
    345, 347, 403, 415, 417, 433, 439, 547, 549, 575, 579, 643, 739, 779, 1869, 
    1895, 1935, 1959, 2135, 2343, 2378, 2383, 2458, 2486, 2492, 2499, 2545, 2561,
    2628, 2633, 2645, 2751, 2753, 2760, 2785, 2901, 2903, 2929, 3001, 3067, 3079, 
    3111, 3146, 3175, 3351, 3356]
    mega_station = [7, 77, 135, 257, 277, 1927, 1929, 1969, 2011, 2017, 2021, 2023, 2031, 2033, 2119,
    2121, 2123, 2125, 2127, 2129, 2131, 2217, 2231, 2251, 2253, 2277, 2279, 2283, 2307,
    2319, 2397, 2417, 2419, 2420, 2421, 2424, 2430, 2432, 2433, 2435, 2437, 2439, 2442,
    2443, 2447, 2454, 2474, 2475, 2496, 2504, 2517, 2520, 2586, 2652, 2661, 2702, 2847,
    2865, 2877, 2919, 2922, 2964, 2970, 3005, 3060, 3217, 3221, 3331, 3414, 3415, 3416]

    mixpanel_data['stationtype'] = mixpanel_data['stationid'].apply(lambda row : 'TRAIN STATION' if row in train_station else
                                                'MEGA STATION' if row in mega_station else 'OTHER STATION')

    mixpanel_data = mixpanel_data.drop(columns = ['stationid'])

    mixpanel_data = mixpanel_data[['userid', 'age', 'sex', 'nationality', 'usertype', 'stationtype', 'vehiclebrand', 'vehiclemodel', 'timeofday', 'dayofweek', 'holiday']]

    #---------- Join with vehicle -> enginetype, vehiclesize based on brand, model -----------#
    mixpanel_data = pd.merge(mixpanel_data, pd.read_csv(for_read_file_path / 'vehicle_info.csv'), 
                             on=['vehiclebrand', 'vehiclemodel'], how='inner')

    #---------- View Count ----------#
    mixpanel_data['view_count'] = mixpanel_data.groupby(list(mixpanel_data.columns)).transform('size')

    #---------- Drop Duplicate ----------#
    mixpanel_data = mixpanel_data.drop_duplicates(list(mixpanel_data.columns), keep = 'first').sort_values(by='view_count', ascending=False)

    #---------- Reorder ----------#
    mixpanel_data = mixpanel_data[['userid', 'age', 'sex', 'nationality', 'usertype', 'stationtype', 'vehiclebrand', 'vehiclemodel', 'vehicletype', 
                                'vehiclesize', 'enginetype', 'timeofday', 'dayofweek', 'holiday', 'view_count']]
    return mixpanel_data

def user_RV():
    resv_count = reservation_count_fn()
    view_count = view_count_fn()
    user_RV = pd.merge(resv_count, view_count, on = ['userid', 'age', 'sex', 'nationality', 'usertype', 'stationtype', 'vehiclebrand',
                'vehiclemodel', 'vehicletype', 'vehiclesize', 'enginetype', 'timeofday', 'dayofweek', 'holiday'], how = 'outer')
    user_RV['reservation_count'] = user_RV['reservation_count'].fillna(0)
    user_RV['view_count'] = user_RV['view_count'].fillna(0)
    user_RV.to_csv(for_read_file_path / 'user_RV.csv')
    return print('user_RV.csv updated!')

user_RV()