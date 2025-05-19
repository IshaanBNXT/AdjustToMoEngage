from ETL.Extract.AWSSingleFileExtractor import AWSSingleFileExtractor
from ETL.Transform.AdjustSignUpFirstTrackerTransformer import AdjustSignUpFirstTrackerTransformer
from ETL.Transform.AdjustOneTimeEventsTransformer import AdjustOneTimeEventsTransformer
from ETL.Load.MoEngageUserPropertyLoader import MoEngageUserPropertyLoader

def main():

    extractor = AWSSingleFileExtractor(
        filepath="https://bnxt-adjust-backups.s3.ap-south-1.amazonaws.com/Adjust-Merged-CSV-15-05-2025.csv.gz",
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


if __name__ == '__main__':
    main()