import pandas as pd
import numpy as np
import json
import joblib
from sklearn.preprocessing import LabelEncoder
from datetime import datetime
from geopy.distance import geodesic
import holidays
import mysql.connector
from mixpanel_utils import MixpanelUtils
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

class Recommendation:

    def __init__(self, userid, latitude, longitude, datetime):
        self.userid = userid
        self.latitude = latitude
        self.longitude = longitude
        self.datetime = datetime
        self.encode_file = joblib.load(r'deploy_package\pickle_file\label_encoders.pkl')
        self.normalized_file = joblib.load(r'deploy_package\pickle_file\normalization.pkl')
        self.model = joblib.load(r'deploy_package\pickle_file\model.pkl')
        self.user_RV_file = pd.read_csv(r'deploy_package\for_read_file\user_RV.csv')
        self.vehicleid_file = pd.read_csv(r'deploy_package\for_read_file\vehicleid_file.csv')
        self.assemble_file = pd.read_csv(r'deploy_package\for_read_file\assemble_file.csv')
        #self.prediction()
        self.predict_file = pd.read_csv(r'deploy_package\for_read_file\predict_output.csv')
        

# - Query table from MySQL

    def station_table_query_fn(self) -> pd.DataFrame:
        connection = mysql.connector.connect(
        host='data.db.haupcar.com',
        port = 25060,
        user="natdanai.k@haupcar.com",
        password= "nWCu(5.H5M#i&2,F",
        database = 'haupcar'
        )

        query_station = ''' SELECT
                        stationid, name, latitude, longitude 
                        FROM 
                        haupcar.station 
                        WHERE 
                        stationid NOT IN (202, 2800, 2801)
                        AND stationstatus = 'SERVICE'
                        AND LOWER(name) NOT LIKE '%p2p%' '''

        cursor = connection.cursor()
        cursor.execute(query_station)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        station_data = pd.DataFrame(rows, columns=column_names)
        station_data = station_data.drop_duplicates()
        return station_data
    

    # ---------------------------------------------------------------------------


    def user_table_query_fn(self) -> pd.DataFrame:
        connection = mysql.connector.connect(
        host='data.db.haupcar.com',
        port = 25060,
        user="natdanai.k@haupcar.com",
        password= "nWCu(5.H5M#i&2,F",
        database = 'haupcar'
        )

        query_user = ''' SELECT
                                u.userid,
                                DATE(u.birthdate) as 'birthday',
                                u.sex,
                                u.usertype,
                                n.nationalityname
                            FROM
                                haupcar.user u
                            INNER JOIN haupcar.nationality n ON
                                u.nationalityid = n.nationalityid
                            WHERE YEAR(u.birthdate) != '0001' '''

        cursor = connection.cursor()
        cursor.execute(query_user)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        user_data = pd.DataFrame(rows, columns=column_names)
        user_data['sex'] = user_data['sex'].replace('', np.nan)
        user_data['sex'] = user_data['sex'].replace('unspecify', np.nan)
        user_data['sex'] = user_data['sex'].fillna(user_data['sex'].mode().iloc[0])
        user_data['usertype'] = user_data['usertype'].apply(
            lambda row: 'STUDENT' if isinstance(row, str) and 'STUDENT' in row
            else 'UNSPECIFIED' if not row or pd.isna(row)
            else 'GENERAL')
        #user_data['usertype'] = user_data['usertype'].fillna(user_data['usertype'].mode().iloc[0])
        user_data['nationalityname'] = user_data['nationalityname'].fillna('Thai')
        user_data['nationalityname'] = user_data['nationalityname'].apply(lambda row : 'Thai' if 'Thai' in row else 'Not Thai')
        return user_data
    

    # ---------------------------------------------------------------------------


    def vehicle_table_query_fn(self) -> pd.DataFrame:
        connection = mysql.connector.connect(
        host='data.db.haupcar.com',
        port = 25060,
        user="natdanai.k@haupcar.com",
        password= "nWCu(5.H5M#i&2,F",
        database = 'haupcar'
        )

        query_vehicle = ''' SELECT
                            vehiclebrand, vehiclemodel, enginetype, vehiclesize, vehicletype
                        FROM haupcar.vehicle v
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
                            AND v.vehiclestatus != 'DISABLE'
                            AND v.vehiclebrand != 'AJ' 
                        GROUP BY vehiclebrand, vehiclemodel, enginetype, vehiclesize, vehicletype '''

        cursor = connection.cursor()
        cursor.execute(query_vehicle)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        vehicle_data = pd.DataFrame(rows, columns=column_names)
        vehicle_data['enginetype'] = vehicle_data['enginetype'].apply(lambda row : 'EV' if row == 'BEV' else 'Non-EV')
        return vehicle_data
    

    # ---------------------------------------------------------------------------

# - Extraction Function


    def user_extract(self) -> pd.DataFrame:

        user_table = self.user_table_query_fn().rename(columns = {'nationalityname' : 'nationality'})
        
        today = datetime.today()
        user_table['age'] = user_table['birthday'].apply(
            lambda row: today.year - row.year - ((today.month, today.day) < (row.month, row.day)))
        
        user_table['age'] = user_table['age'].apply(
                lambda row: '18-24' if 18 <= row <= 24 else
                '25-45' if 25 <= row <= 45 else
                '45+' if 100 >= row >= 46 else '25-45')
        
        if user_table['userid'].eq(self.userid).any():
            data_have_id = user_table[user_table['userid'] == self.userid]
            data_have_id = data_have_id.drop(columns=['userid', 'birthday'])
            return data_have_id
        else:
            default_data = {
                'age': [user_table['age'].mode().iloc[0]],
                'sex': [user_table['sex'].mode().iloc[0]],
                'usertype': [user_table['usertype'].mode().iloc[0]],
                'nationality': [user_table['nationality'].mode().iloc[0]],
            }
            data_no_id = pd.DataFrame(default_data)
            return data_no_id
    

# ---------------------------------------------------------------------------


# fn return station ที่ใกล้ที่สุด จาก ตำแหน่งปัจจุบันของ user โดยเอาข้อมูลจาก mixpanel -> pending, no lat long stored.
# generate ramdom lat long from station location. (gen by lat lon around station)
    def find_closest_stations(self):

        def find_distance(lat1, lon1, lat2, lon2):
            distance = geodesic((lat1, lon1), (lat2, lon2)).kilometers
            return distance
        
        distances = []
        user_latitude = self.latitude
        user_longitude = self.longitude
        all_stations = self.station_table_query_fn()
        for index, row in all_stations.iterrows():
            distance = find_distance(user_latitude, user_longitude, row['latitude'], row['longitude'])
            distances.append({'name' : row['name'], 'stationid' : row['stationid'], 'distance' : distance})
        
        # Sort stations by distance and get the top N
        closest_stations = pd.DataFrame(sorted(distances, key=lambda x: x['distance'])).query("distance <= 10")   # KM

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
        
        closest_station_id = all_stations[all_stations['stationid'].isin(closest_stations['stationid'])]
        closest_stations_distance = pd.merge(closest_station_id, closest_stations, on=['stationid', 'name'], how='inner')

        closest_stations_distance['stationtype'] = closest_stations_distance['stationid'].apply(lambda row : 'TRAIN STATION' if row in train_station else
                                                                                        'MEGA STATION' if row in mega_station else 'OTHER STATION')
 
        # return {'closest_station' : closest_stations.drop_duplicates(), 'closest_stationid' : closest_station_id.drop_duplicates(), 'distance' : distances}
        return closest_stations_distance


# ---------------------------------------------------------------------------


    def datetime_fn(self):
        
        datetime = pd.DataFrame([pd.to_datetime(self.datetime)], columns=['datetime'])

        datetime['timeofday'] =  datetime['datetime'].apply(lambda row : 'WORK HOURS' if row.hour in [6, 7, 8, 9 , 10, 11, 12, 13, 14, 15, 16, 17, 18] else
                                                           'LEISURE HOURS' if row.hour in [18, 19, 20, 21, 22] else
                                                           'LATE NIGHT' if row.hour in [23, 0, 1, 2, 3, 4, 5] else np.nan)
        datetime['dayofweek'] = datetime['datetime'].apply(lambda row : 'WEEKEND' if row.day_name() in ['Saturday', 'Sunday'] else 'WEEKDAY')
        datetime['holiday'] = datetime['datetime'].apply(lambda row : 'Yes' if row.date() in holidays.TH() else 'No')

        return datetime
    

#--------------------------------------------------------------------------------------------------------------


    def assemble(self):
        rv_fetch = self.user_RV_file
        rv_fetch = rv_fetch[rv_fetch['userid'] == self.userid]
        user = rv_fetch[['userid', 'age',	'sex', 'nationality', 'usertype']].drop_duplicates()
        rv = rv_fetch[['userid', 'age', 'sex', 'nationality',	'usertype',	'stationtype', 'vehiclebrand', 'vehiclemodel', 'enginetype', 'vehiclesize', 'vehicletype',
                                'timeofday', 'dayofweek', 'reservation_count', 'view_count']].drop_duplicates()
        station = self.find_closest_stations()['stationtype'].drop_duplicates()
        vehicle = self.vehicle_table_query_fn().drop_duplicates()
        datetime = self.datetime_fn() # in prod : input is datetime of user
        #assemble_rv = rv.merge(user, how='left').merge(station, how='left').merge(vehicle, how='left').merge(datetime, how='left')
        #assemble_all_data_test = user.merge(station, how='cross')
        assemble_all_data = user.merge(station, how='cross').merge(vehicle, how='cross').merge(datetime, how='cross').drop_duplicates()
        assemble_data = assemble_all_data.drop(['datetime'], axis=1)
        possible_combination = assemble_data.merge(rv, on=['userid', 'age', 'sex', 'nationality', 'usertype', 'stationtype', 'vehiclebrand', 'vehiclemodel', 'enginetype', 'vehiclesize', 'vehicletype',
                                'timeofday', 'dayofweek'], how='left')
        possible_combination['reservation_count'] = possible_combination['reservation_count'].fillna(0)
        possible_combination['view_count'] = possible_combination['view_count'].fillna(0)
        possible_combination = possible_combination.drop(['userid'], axis = 1)
        possible_combination = possible_combination.drop_duplicates()
        possible_combination = possible_combination.sort_values(by=['reservation_count', 'view_count'], ascending=[False, False])
        possible_combination.to_csv(r'deploy_package\for_read_file\assemble_file.csv', index=False)
        return possible_combination
    
    
# ---------------------------------------------------------------------------

    
    def encoded(self):
        encoded_data = pd.DataFrame(self.assemble_file)
        for col, encoder in self.encode_file.items():
            if col in encoded_data.columns:
                encoded_data[col] = [
                    encoder.transform([label])[0] if label in encoder.classes_ else -1
                    for label in encoded_data[col]
                ]
        return encoded_data
    

# ---------------------------------------------------------------------------


    def normalized(self):
        encoded_data = self.encoded()
        encoded_data_reindex = encoded_data[['age', 'sex', 'nationality', 'usertype', 'stationtype', 'vehiclebrand', 'vehiclemodel', 'enginetype', 'vehiclesize', 'vehicletype',
                                             'dayofweek', 'timeofday', 'holiday', 'reservation_count', 'view_count']]
        normalized_transform = self.normalized_file.transform(encoded_data_reindex)
        normalized = pd.DataFrame(normalized_transform, columns=['age', 'sex', 'nationality', 'usertype', 'stationtype', 'vehiclebrand', 'vehiclemodel', 'enginetype', 'vehiclesize', 'vehicletype',
                                             'dayofweek', 'timeofday', 'holiday', 'reservation_count', 'view_count'])
        return normalized
    

# ---------------------------------------------------------------------------


    def prediction(self):
        prepared_data = self.normalized()
        final_data = self.assemble_file
        prediction_score = self.model.predict(prepared_data)
        final_data['predicted_score'] = prediction_score
        final_data = final_data.sort_values(by='predicted_score', ascending=False)
        final_data.to_csv(r'deploy_package\for_read_file\predict_output.csvv')
        return final_data
    

# ---------------------------------------------------------------------------


    def vehicle_current_stationid(self):

        connection = mysql.connector.connect(
        host='data.db.haupcar.com',
        port = 25060,
        user="natdanai.k@haupcar.com",
        password= "nWCu(5.H5M#i&2,F",
        database = 'haupcar'
        )

        query_current_station = f''' SELECT
                                v.vehicleid,
                                v.host_stationid AS 'current_stationid',
                                s.name AS 'current_station'
                            FROM
                                haupcar.vehicle v
                            INNER JOIN haupcar.station s ON
                                v.host_stationid = s.stationid
                            WHERE
                                v.vehiclemodel != 'ONE'
                                AND v.vehiclecode NOT LIKE 'TEST-%'
                                AND v.vehiclecode NOT LIKE '%PENK%'
                                AND v.vehiclecode NOT LIKE 'SENG-%'
                                AND v.vehiclecode NOT LIKE 'EGAT-%'
                                AND v.vehiclecode NOT LIKE '%-TEST%'
                                AND v.vehiclecode NOT LIKE '%CPOD%'
                                AND v.vehiclecode NOT LIKE '%HASP%'
                                AND v.vehiclecode NOT LIKE '%ADSN%'
                                AND v.vehiclecode NOT LIKE '%EZY%'
                                AND v.vehiclestatus != 'DISABLE'
                                AND v.vehiclemodel NOT LIKE '%(or equivalent)%'
                                AND v.vehiclemodel NOT LIKE '%PCX%'
                                AND v.vehiclecode NOT LIKE '%FOM%'
                                AND v.vehiclebrand != 'AJ'
                                AND s.stationid NOT IN (202, 2800, 2801)
                                AND s.stationstatus = 'SERVICE' '''
        cursor = connection.cursor()
        cursor.execute(query_current_station)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        current_stationid = pd.DataFrame(rows, columns=column_names)

        return current_stationid
    

    # ---------------------------------------------------------------------------

    
    def available_vehicleid_in_10km_station(self):

        top10_model = self.predict_file[['vehiclebrand', 'vehiclemodel']].head(10)
        vehicleid_table = self.vehicleid_file
        # id_model = pd.merge(top10_model, vehicleid_table, on='vehiclemodel', how='left')[['vehicleid']]
    #     vehicleid_query = tuple(id_model['vehicleid'])
        closest_stationid_query = tuple(self.find_closest_stations()['stationid'].tolist())
        

        closest_stationid = self.find_closest_stations()['stationid'].tolist()
        # closest_stationname = pd.DataFrame(self.find_closest_stations()['closest_station']['name'])
        vehicle_current_station = self.vehicle_current_stationid()[['vehicleid', 'current_stationid', 'current_station']]
        vehicle_current_station = pd.merge(vehicle_current_station, vehicleid_table, on='vehicleid', how='inner')[['vehicleid', 'vehiclebrand', 'vehiclemodel', 'current_stationid', 'current_station']].drop_duplicates()
        vehicle_in_top5_station = vehicle_current_station[vehicle_current_station['current_stationid'].isin(closest_stationid)]
        # vehicle_in_top5_station = pd.merge(vehicle_in_top5_station, vehicleid_table, on=['vehiclebrand', 'vehiclemodel'], how='left')[['vehicleid', 'vehiclebrand', 'vehiclemodel', 'current_stationid', 'current_station']]
        
        top10_model_in_top5_station = pd.merge(vehicle_in_top5_station, top10_model, on='vehiclemodel', how='inner')
        vehicleid_in_station_tuple = tuple(top10_model_in_top5_station['vehicleid'])
        vehicleid_in_station_str =  "(0)" if len(vehicleid_in_station_tuple) == 0 else f"({vehicleid_in_station_tuple[0]})" if len(vehicleid_in_station_tuple) == 1 else vehicleid_in_station_tuple 
        vehicleid_in_station_list = [int(vehicleid_in_station_str.strip('()'))] if len(vehicleid_in_station_tuple) == 1 else list(vehicleid_in_station_tuple)

        connection = mysql.connector.connect(
        host='data.db.haupcar.com',
        port = 25060,
        user="natdanai.k@haupcar.com",
        password= "nWCu(5.H5M#i&2,F",
        database = 'haupcar'
        )

        query_driving = f''' SELECT
                            r.vehicleid
                        FROM
                            haupcar.reservation r
                        INNER JOIN haupcar.vehicle v 
                        ON r.vehicleid = v.vehicleid
                        WHERE
                            v.vehiclemodel != 'ONE'
                            AND v.vehiclecode NOT LIKE 'TEST-%'
                            AND v.vehiclecode NOT LIKE '%PENK%'
                            AND v.vehiclecode NOT LIKE 'SENG-%'
                            AND v.vehiclecode NOT LIKE 'EGAT-%'
                            AND v.vehiclecode NOT LIKE '%-TEST%'
                            AND v.vehiclecode NOT LIKE '%CPOD%'
                            AND v.vehiclecode NOT LIKE '%HASP%'
                            AND v.vehiclecode NOT LIKE '%ADSN%'
                            AND v.vehiclecode NOT LIKE '%EZY%'
                            AND v.vehiclestatus != 'DISABLE'
                            AND v.vehiclemodel NOT LIKE '%(or equivalent)%'
                            AND v.vehiclemodel NOT LIKE '%PCX%'
                            AND v.vehiclecode NOT LIKE '%FOM%'
                            AND v.vehiclebrand != 'AJ' 
                            AND r.reservationstate IN ('DRIVE')
                            AND r.vehicleid IN {vehicleid_in_station_str}
                            # AND r.stationid IN {closest_stationid_query} '''
        cursor = connection.cursor()
        cursor.execute(query_driving)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        driving_vehicleid = pd.DataFrame(rows, columns=column_names) # ได้รถที่ถูกขับอยู่ 


        query_reserved = f''' SELECT
                    r.vehicleid
                FROM
                    haupcar.reservation r
                INNER JOIN haupcar.vehicle v 
                ON r.vehicleid = v.vehicleid
                WHERE
                v.vehiclemodel != 'ONE'
                AND v.vehiclecode NOT LIKE 'TEST-%'
                AND v.vehiclecode NOT LIKE '%PENK%'
                AND v.vehiclecode NOT LIKE 'SENG-%'
                AND v.vehiclecode NOT LIKE 'EGAT-%'
                AND v.vehiclecode NOT LIKE '%-TEST%'
                AND v.vehiclecode NOT LIKE '%CPOD%'
                AND v.vehiclecode NOT LIKE '%HASP%'
                AND v.vehiclecode NOT LIKE '%ADSN%'
                AND v.vehiclecode NOT LIKE '%EZY%'
                AND v.vehiclestatus != 'DISABLE'
                AND v.vehiclemodel NOT LIKE '%(or equivalent)%'
                AND v.vehiclemodel NOT LIKE '%PCX%'
                AND v.vehiclecode NOT LIKE '%FOM%'
                AND v.vehiclebrand != 'AJ'
                AND r.reservationstate = 'RESERVE'
                AND r.stationid IN {closest_stationid_query}
                AND '{self.datetime_fn()['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')[0]}'
                BETWEEN r.reservestarttime AND r.reservestoptime '''
        cursor = connection.cursor()
        cursor.execute(query_reserved)
        rows = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        reserved_vehicleid = pd.DataFrame(rows, columns=column_names) # ได้รถที่ถูกจอง

        driving_reserved_vehicleid = pd.concat([driving_vehicleid, reserved_vehicleid], axis=0)['vehicleid'].tolist()

        available_vehicleid = [id for id in vehicleid_in_station_list if id not in driving_reserved_vehicleid]
        available_vehicleid = pd.merge(pd.DataFrame(available_vehicleid, columns=['vehicleid']), vehicle_current_station, on='vehicleid', how='inner')

        return {'top_10model' : top10_model, 'closest_stationid' : closest_stationid, 'vehicle_current_station': vehicle_current_station, 'vehicle_in_top5_station' : vehicle_in_top5_station}
    

    # ---------------------------------------------------------------------------


    def prediction_results(self):
        top_10_model_pred = self.available_vehicleid_in_10km_station()['top_10model']
        station_df = self.station_table_query_fn()
        closest_station = station_df[station_df['stationid'].isin(self.available_vehicleid_in_10km_station()['closest_stationid'])][['stationid', 'name']]
        vehicle_in_10km_station = self.available_vehicleid_in_10km_station()['vehicle_in_top5_station']
        distances = self.find_closest_stations()[['stationid', 'distance']].sort_values(by='distance', ascending=True)
        predicted_interestscore = self.predict_file[['vehiclebrand', 'vehiclemodel', 'predicted_score']].sort_values(by='predicted_score', ascending=False).head(10)
        vehicleinstation_match_top10model = pd.merge(vehicle_in_10km_station, top_10_model_pred, on=['vehiclebrand', 'vehiclemodel'], how='inner').merge(
                                                    distances, left_on='current_stationid', right_on='stationid', how='inner').merge(
                                                    predicted_interestscore, on=['vehiclebrand', 'vehiclemodel'], how='inner').drop('current_stationid', axis=1)
        result = vehicleinstation_match_top10model.sort_values(by=['distance', 'predicted_score'], ascending = [True, False])[
                                                                ['vehicleid', 'vehiclebrand', 'vehiclemodel', 'stationid', 'current_station', 
                                                                'distance', 'predicted_score']]


        return {'result':result, 'top10_model':top_10_model_pred, 'closest_station':closest_station, 'vehicle_in_top5_station': vehicle_in_10km_station}



if __name__ == "__main__":
    recommendation = Recommendation(userid = 272745, latitude = 13.7562386, longitude = 100.5332286,
                datetime = '2025-01-20 10:33:18').prediction_results()['result']
    print(recommendation)