import datetime
from typing import List

from ETL.Extract.AWSSingleFileExtractor import AWSSingleFileExtractor
from ETL.Transform.AdjustSignUpFirstTrackerTransformer import AdjustSignUpFirstTrackerTransformer
from ETL.Transform.AdjustOneTimeEventsTransformer import AdjustOneTimeEventsTransformer
from ETL.Load.MoEngageUserPropertyLoader import MoEngageUserPropertyLoader

def process_single_file(file_date: datetime.date = datetime.date.today()) -> bool:
    success = False
    file_date_str = file_date.strftime("%d-%m-%Y")

    extractor = AWSSingleFileExtractor(
        filepath=f"https://bnxt-adjust-backups.s3.ap-south-1.amazonaws.com/Adjust-Merged-CSV-{file_date_str}.csv.gz",
        compression='gzip'
    )
    signup_transformer = AdjustSignUpFirstTrackerTransformer(remove_test_users=True, test_user_id_upper_limit=9999)
    events_transformer = AdjustOneTimeEventsTransformer(remove_test_users=True, test_user_id_upper_limit=9999)
    # TODO: Uploader Definition and Implementation
    # user_property_uploader = MoEngageUserPropertyLoader(destination, creds)
    # event_uploader = MoEngageEventLoader(destination, creds)

    raw_data = extractor.read_data()
    signup_data = signup_transformer.transform_data(raw_data)
    events_data = events_transformer.transform_data(raw_data)
    # TODO: Uploader Definition and Implementation
    # user_property_uploader.load_data(signup_data)
    # event_uploader.load_data(events_data)

    success = True

    return success


def get_dates_in_range(start: datetime.date, end: datetime.date, exclude_start: bool = False, exclude_end: bool = False) -> List:
    dates = []
    if (start > end) or (start == end and (exclude_end or exclude_start)):
        return dates
    if start == end:
        return [start]
    day_range = (start-end).days
    day_range_start = 1 if exclude_start else 0
    day_range_end = 0 if exclude_end else 1
    for i, day in enumerate(range(day_range_start, day_range + day_range_end)):
        dates.append(start + datetime.timedelta(days=day))
        # print(dates[i].strftime("%d-%m-%Y"))
    # print(dates)
    return dates


def process_files_in_range(start: datetime.date, end: datetime.date, exclude_start: bool = False, exclude_end: bool = False) -> int:
    count = 0
    dates = get_dates_in_range(start=start, end=end, exclude_start=exclude_start, exclude_end=exclude_end)
    for date in dates:
        success = process_single_file(file_date=date)
        count += 1 if success else 0
    print(f"Processed {count} files out of {len(dates)}")
    return count


if __name__ == '__main__':
    # process_single_file()
    import time
    import datetime

    # print(type(datetime.date.today()))

    # print(get_dates_in_range(datetime.date(2025,5,4), datetime.date.today()))
    # print("-"*10)
    # print(get_dates_in_range(datetime.date(2025,5,4), datetime.date.today(),True))
    # print("-"*10)
    # print(get_dates_in_range(datetime.date(2025,5,4), datetime.date.today(),False,True))
    # print("-"*10)
    # print(get_dates_in_range(datetime.date(2025,5,4), datetime.date.today(),True,True))
    print(process_files_in_range(datetime.date(2025,5,4), datetime.date.today()))