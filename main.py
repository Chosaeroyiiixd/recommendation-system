
import sys
# sys.path.append(r'C:\Users\HAUPCAR\Desktop\AI\Recommendation Model\API')
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel, confloat, conint
from datetime import datetime
from deploy_package.Recommendation import Recommendation



app = FastAPI()

class input_validation(BaseModel):
    userid: conint(gt=0)
    latitude: confloat(ge=5.5, le=20.5)  # Thailand's latitude range
    longitude: confloat(ge=97.3, le=105.7)  # Thailand's longitude range
    datetime: datetime


@app.post('/') # JSON post request data will store at this decorator(@) then will pass through Recommendation argument.
async def Recommendations(post_request_json : input_validation): # async will let def run other task when traffic is calling the fn (like parallel)
    input_data = post_request_json.dict()
    predict = Recommendation(userid = input_data['userid'], 
                        latitude = input_data['latitude'], 
                        longitude = input_data['longitude'], 
                        datetime = input_data['datetime']).prediction_results()['result'].to_dict(orient="records")

    # predict = Recommendation(userid = 272745, 
    #                     latitude = 13.7562386, 
    #                     longitude = 100.5332286, 
    #                     datetime = "2025-01-20 10:33:18").prediction_results()['result'].head(5).to_dict(orient="records")
    
    return predict


