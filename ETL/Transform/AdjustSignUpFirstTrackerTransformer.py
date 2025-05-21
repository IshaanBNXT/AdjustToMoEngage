import pandas as pd

from ETL.Interfaces.TransformerInterface import Transformer
from Resources.Config import SIGN_UP_FIRST_TRACKER_COLUMNS, DEFAULT_TEST_USER_ID_UPPER_LIMIT


class AdjustSignUpFirstTrackerTransformer(Transformer):

    def __init__(self, remove_test_users: bool=True, test_user_id_upper_limit: int=DEFAULT_TEST_USER_ID_UPPER_LIMIT):
        self.remove_test_users = remove_test_users
        self.test_user_id_upper_limit = test_user_id_upper_limit

    def transform_data(self, data: pd.DataFrame,):
        raw_df = data
        if self.remove_test_users:
            raw_df = raw_df[raw_df['[userId]'] > self.test_user_id_upper_limit]
        sign_up_users_df = raw_df[raw_df['{event_name}'] == 'Signup_stage1']
        sign_up_sorted_df = sign_up_users_df.sort_values(by=['[userId]','{created_at}'], ascending=False)
        sign_up_sorted_unique_df = sign_up_sorted_df.drop_duplicates(subset=['[userId]'], keep='first')
        sign_up_sorted_unique_min_cols_df = sign_up_sorted_unique_df[SIGN_UP_FIRST_TRACKER_COLUMNS]

        return sign_up_sorted_unique_min_cols_df