from mixpanel_utils import MixpanelUtils
from datetime import date
from pathlib import Path

for_read_file_path = Path(__file__).parent

def export_mixpanel():

    today = date.today().strftime('%Y-%m-%d')
    mputils = MixpanelUtils(
    'AmayMOULtr9pNRKM5DUmXHX1BtlkxvTG',
    service_account_username='Haup_service_account.ec38c1.mp-service-account',
    project_id='2915624',
    token='aa99b9539bd2e14a21ce983142c9593e')
    mputils.export_events(for_read_file_path / 'event_export_view.json', 
                          {'from_date': '2022-01-01', 'to_date': today, 'event': '["ShortTermDetail"]'})

    return print('event_export_view.json updated!')

export_mixpanel()