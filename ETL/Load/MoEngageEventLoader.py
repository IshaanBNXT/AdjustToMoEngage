# TODO: Define Loaders for User Property as well as Events
import pandas as pd
import os
import asyncio
import aiohttp
from typing import List
from dotenv import load_dotenv
load_dotenv()

from ETL.Interfaces.LoaderInterface import Loader
from Resources.MoEngageDTO import MoEngageEventDTO

class MoEngageEventLoader(Loader):

    def __init__(self):
        # self.push_url = os.environ.get("MOENGAGE_JAVA_TRIGGER_ENDPOINT")
        self.push_url = os.environ.get("MOENGAGE_PROD_EVENT_ENDPOINT")
        self.concurrency = 100

    def upload_data(self, data: pd.DataFrame):
        event_list = self.prepare_data(data=data)
        results = asyncio.run(self.bulk_post(event_list))
        return results

    async def bulk_post(self, event_list: List[MoEngageEventDTO], concurrency: int=None):
        if not concurrency:
            concurrency = self.concurrency
        semaphore = asyncio.Semaphore(concurrency)
        async with aiohttp.ClientSession() as session:
            async def bound_post(prop):
                async with semaphore:
                    # return await self.post_single_data_bnxt(session, prop)
                    return await self.post_single_data_moengage(session, prop)

            tasks = [bound_post(prop) for prop in event_list]
            return await asyncio.gather(*tasks, return_exceptions=True)

    async def post_single_data_bnxt(self, session, payload: MoEngageEventDTO):
        try:
            async with session.post(self.push_url, json=payload.to_bnxt_dict()) as response:
                response_text = await response.text()
                if response.status != 200:
                    print(f"Failed for {payload.user_id}: {response.status} - {response_text}")
                return {
                    "user_id": payload.user_id,
                    "status": response.status,
                    "response": response_text
                }
        except Exception as e:
            print(f"Exception for {payload.user_id}: {e}")
            return {
                "user_id": payload.user_id,
                "error": str(e)
            }

    async def post_single_data_moengage(self, session: aiohttp.ClientSession, payload: MoEngageEventDTO):
        try:
            app_id = os.environ.get("MOENGAGE_PROD_EVENT_PARAM_APP_ID")
            token = os.environ.get("MOENGAGE_PROD_EVENT_TOKEN")
            headers = {
                "Authorization":"Basic "+token,
                "Content-Type":"application/json"
            }
            params = {
                "app_id":app_id
            }
            async with session.post(url=self.push_url, params=params, headers=headers, json=payload.to_moengage_dict()) as response:
                response_text = await response.text()
                if response.status != 200:
                    print(f"Failed for {payload.user_id}: {response.status} - {response_text}")
                return {
                    "user_id": payload.user_id,
                    "status": response.status,
                    "response": response_text
                }
        except Exception as e:
            print(f"Exception for {payload.user_id}: {e}")
            return {
                "user_id": payload.user_id,
                "error": str(e)
            }


    def prepare_data(self, data) -> List[MoEngageEventDTO]:
        event_list = []
        for index, row in data.iterrows():
            event = row['{event_name}']
            tracker = row['{tracker_name}']
            if tracker in ['Organic']:
                campaign = "Organic"
                network = "Organic"
            elif tracker in ['Unattributed']:
                campaign = row['{fb_install_referrer_campaign_name}']
                network = row['{fb_install_referrer_publisher_platform}']
            elif "::" in tracker:
                tracker_split = tracker.split("::")
                campaign = tracker_split[1]
                network = tracker_split[0]
            else:
                # Edge Cases like User ID 1966658 in 04-05-2025 CSV
                # First Tracker is just "Website", no Campaign Name, Network is "Website"
                campaign = tracker
                network = tracker
            event_obj = MoEngageEventDTO(
                user_id=row['[userId]'],
                event=event,
                event_campaign=campaign,
                event_network=network
            )
            event_list.append(event_obj)
        return event_list