import pandas as pd
import mysql.connector
from pathlib import Path

for_read_file_path = Path(__file__).parent.parent / 'for_read_file'
def vehicle_table_query_fn() -> pd.DataFrame:
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
    vehicle_data.to_csv(for_read_file_path / 'vehicle_info.csv')
    return print('vehicle_info.csv updated!')

vehicle_table_query_fn()