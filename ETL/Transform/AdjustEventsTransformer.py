import pandas as pd

from ETL.Interfaces.TransformerInterface import Transformer
from Resources.Config import EVENT_NAMES, EVENTS_COLUMNS, DEFAULT_TEST_USER_ID_UPPER_LIMIT


class AdjustEventsTransformer(Transformer):

    def __init__(self, remove_test_users: bool=True, test_user_id_upper_limit: int=DEFAULT_TEST_USER_ID_UPPER_LIMIT):
        self.remove_test_users = remove_test_users
        self.test_user_id_upper_limit = test_user_id_upper_limit

    def transform_data(self, data: pd.DataFrame):
        raw_df = data
        if self.remove_test_users:
            raw_df = raw_df[raw_df['[userId]'] > self.test_user_id_upper_limit]
        events_df = raw_df[raw_df['{activity_kind}'] == 'event']
        events_onetimeevents_df = events_df[events_df['{event_name}'].isin(EVENT_NAMES)]
        events_onetimeevents_sorted_df = events_onetimeevents_df.sort_values(by=['[userId]','{created_at}'], ascending=False)
        events_onetimeevents_sorted_min_cols_df = events_onetimeevents_sorted_df[EVENTS_COLUMNS]

        return events_onetimeevents_sorted_min_cols_df
