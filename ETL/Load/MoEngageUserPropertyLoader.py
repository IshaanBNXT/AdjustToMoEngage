# TODO: Define Loaders for User Property as well as Events
import pandas as pd
import os
import asyncio
import aiohttp
from typing import List
from dotenv import load_dotenv
load_dotenv()

from ETL.Interfaces.LoaderInterface import Loader
from Resources.MoEngageDTO import MoEngageUserPropertyDTO

class MoEngageUserPropertyLoader(Loader):

    def __init__(self):
        # self.push_url = os.environ.get("MOENGAGE_JAVA_TRIGGER_ENDPOINT")
        self.push_url = os.environ.get("MOENGAGE_PROD_USER_ENDPOINT")
        self.concurrency = 100

    def upload_data(self, data: pd.DataFrame):
        user_props = self.prepare_data(data=data)
        results = asyncio.run(self.bulk_post(user_props))
        return results

    async def bulk_post(self, user_props: List[MoEngageUserPropertyDTO], concurrency: int=None):
        if not concurrency:
            concurrency = self.concurrency
        semaphore = asyncio.Semaphore(concurrency)
        async with aiohttp.ClientSession() as session:
            async def bound_post(prop):
                async with semaphore:
                    # return await self.post_single_data_bnxt(session, prop)
                    return await self.post_single_data_moengage(session, prop)
                    

            tasks = [bound_post(prop) for prop in user_props]
            return await asyncio.gather(*tasks, return_exceptions=True)

    async def post_single_data_bnxt(self, session, payload: MoEngageUserPropertyDTO):
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

    async def post_single_data_moengage(self, session: aiohttp.ClientSession, payload: MoEngageUserPropertyDTO):
        try:
            app_id = os.environ.get("MOENGAGE_PROD_USER_PARAM_APP_ID")
            token = os.environ.get("MOENGAGE_PROD_USER_TOKEN")
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

    def prepare_data(self, data) -> List[MoEngageUserPropertyDTO]:
        user_props = []
        for index, row in data.iterrows():
            first_tracker = row['{first_tracker_name}']
            if first_tracker in ['Organic']:
                campaign = "Organic"
                network = "Organic"
            elif first_tracker in ['Unattributed']:
                campaign = row['{fb_install_referrer_campaign_name}']
                network = row['{fb_install_referrer_publisher_platform}']
            elif "::" in first_tracker:
                first_tracker_split = first_tracker.split("::")
                campaign = first_tracker_split[1]
                network = first_tracker_split[0]
            else:
                # Edge Cases like User ID 1966658 in 04-05-2025 CSV
                # First Tracker is just "Website", no Campaign Name, Network is "Website"
                campaign = first_tracker
                network = first_tracker
            user_prop_obj = MoEngageUserPropertyDTO(
                user_id=row['[userId]'],
                first_campaign=campaign,
                first_network=network
            )
            user_props.append(user_prop_obj)
        return user_props