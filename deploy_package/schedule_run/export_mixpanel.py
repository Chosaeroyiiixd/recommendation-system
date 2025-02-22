from mixpanel_utils import MixpanelUtils
from datetime import date, timedelta
from pathlib import Path
import zipfile
import json
import tempfile
from dotenv import load_dotenv
import os


for_read_file_path = Path(__file__).parent.parent / 'for_read_file'

load_dotenv()

SERVICE_ACCOUNT_SECRET = os.getenv("SERVICE_ACCOUNT_SECRET")
SERVICE_ACCOUNT_USERNAME = os.getenv("SERVICE_ACCOUNT_USERNAME")
PROJECT_ID = os.getenv("PROJECT_ID")
TOKEN = os.getenv("TOKEN")


def export_mixpanel():
    
    today = (date.today()- timedelta(days=1)).strftime('%Y-%m-%d')

    mputils = MixpanelUtils(
    SERVICE_ACCOUNT_SECRET,
    service_account_username = SERVICE_ACCOUNT_USERNAME,
    project_id = PROJECT_ID,
    token = TOKEN)

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_filename = temp_file.name 

        mputils.export_events(
            temp_filename, 
            {'from_date': '2022-01-01', 'to_date': today, 'event': '["ShortTermDetail"]'}
        )

        with open(temp_filename, 'r') as f:
            response_json = f.read()

        with zipfile.ZipFile(for_read_file_path / "mixpanel_export_data.zip", 'w', zipfile.ZIP_DEFLATED) as zipf:
            with zipf.open('event_export_view.json', 'w') as zf:
                zf.write(response_json.encode('utf-8'))

    return print('event_export_view.zip saved!')

export_mixpanel()