import mysql.connector
import pandas as pd
from pathlib import Path

for_read_file_path = Path(__file__).parent.parent / 'for_read_file'

def vehicleid_table_query_fn():
    connection = mysql.connector.connect(
    host='data.db.haupcar.com',
    port = 25060,
    user="natdanai.k@haupcar.com",
    password= "nWCu(5.H5M#i&2,F",
    database = 'haupcar'
    )

    query_vehicleid = ''' SELECT
                            vehicleid, vehiclebrand, vehiclemodel
                        FROM
                            haupcar.vehicle v 
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
                            AND v.vehiclestatus != 'DISABLE'
                            AND v.vehiclemodel NOT LIKE '%(or equivalent)%'
                            AND v.vehiclemodel NOT LIKE '%PCX%'
                            AND v.vehiclecode NOT LIKE '%FOM%'
                            AND v.vehiclebrand != 'AJ' '''

    cursor = connection.cursor()
    cursor.execute(query_vehicleid)
    rows = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    vehicleid_data = pd.DataFrame(rows, columns=column_names)
    vehicleid_data.to_csv(for_read_file_path / 'vehicleid_file.csv', index=False)
    return print('vehicleid_file.csv updated!')

vehicleid_table_query_fn()