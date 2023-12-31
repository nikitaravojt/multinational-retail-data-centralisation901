from typing import Any
import pandas as pd
import numpy as np
import database_utils as utils
import data_extraction as ext
import re

class DataCleaning():

    """Methods named with preceding double underscores are
    marked as private and used to clean a target column in the
    dataframe. They are called by the clean_user_data() method
    to clean each column in succession, by marking the invalid
    data entries as np.nan.
    """

    def __date_cleaning(self, df, target_col):
        from dateutil.parser import parse

        def parse_flexible_date(date_str):
            # Try to parse each date entry using dateutil.parser.parse.
            # If entry raises a ValueError within the parser, set entry to NaT.
            try:
                parsed_date = parse(date_str)
                return parsed_date
            except ValueError:
                return pd.NaT

        df[target_col] = df[target_col].astype(str).apply(parse_flexible_date)
        df[target_col] = pd.to_datetime(df[target_col], format="%Y-%m-%d", errors='coerce')

        return df



    def __country_code_cleaning(self, row):
        """Private method to clean country codes.
        Codes must be 2 characters long and only contain letters.
        """
        code = row['country_code']
        if len(code) != 2 or not code.isalpha():
            return np.nan
        else:
            return code


    def __phone_number_cleaning(self, row):
        """Private method to clean all phone numbers, as follows:
        1. Check for pre-existing +1, +44, +49 or 001 leading codes and
        adjust country_code accordingly.
        2. Remove leading codes if present to begin cleaning.
        3. Remove all special chars (e.g., parantheses, hyphens, dots).
        4. Remove all extensions.
        5. If there is a leading zero, remove it unless its a US number.
        6. Add the leading (country code) based on value of country_code.
        7. Remove all whitespaces.
        8. Check that number length is 12 or 13 for UK and DE numbers, 
        and that it is 12 for US numbers (this includes the country code).
        """

        number = str(row['phone_number'])
        if row['country_code'] == np.nan:
            country_code = 'NULL'
        else:
            country_code = str(row['country_code']).upper()

        # Some numbers may originate from a different country despite its df['country_code']:
        if number.startswith('+1') or number.startswith('001'):
            country_code = 'US'
        elif number.startswith('+44'):
            country_code = 'GB'
        elif number.startswith('+49'):
            country_code = 'DE'

        # Remove +1, +44, or +49 codes at the start of the number (if present)
        number = re.sub(r'^\+1|^(\+44)|^\+49|^001', '', number)

        # Remove all special chars (e.g., parentheses, hyphens and dots)
        number = re.sub(r'[\(\)-\.\s]', '', number)

        # If there is an 'x' present (denoting an extension), remove it and all digits after it
        number = re.sub(r'x\d+', '', number)

        # If there is a leading zero, remove it, unless it is a US number
        if country_code != 'US':
            number = re.sub(r'^0', '', number)  

        # Add the +1, +44, or +49 country code based on the value of country_code
        if country_code == 'GB':
            number = '+44' + number
        elif country_code == 'DE':
            number = '+49' + number
        elif country_code == 'US':
            number = '+1' + number

        # Remove all whitespaces from all entries
        number = re.sub(r'\s', '', number)

        # If total length of number (depending on country) is invalid, or
        # if number contains any alphabetical chars, set it to np.nan
        if (country_code == 'GB' and len(number) not in {12, 13}) or \
        (country_code == 'DE' and len(number) not in {12, 13}) or \
        (country_code == 'US' and len(number) != 12) or \
        any(char.isalpha() for char in number):
            number = np.nan

        return number




    def clean_user_data(self, df):

        """Workflow:
        -One by one, mark all invalid data entries from all cols as np.nan.
        -Each of these will be in its own private method.
        -Create final method to call each of these cleaning methods one by one,
        finishing by purging any and all rows containing np.nan. Return cleaned_df.
        """
        # df = df.drop_duplicates(inplace=True) # remove any exact duplicates
        df['country_code'] = df.apply(self.__country_code_cleaning, axis=1)
        df['phone_number'] = df.apply(self.__phone_number_cleaning, axis=1)
        df = self.__date_cleaning(df, target_col='date_of_birth')
        df = self.__date_cleaning(df, target_col='join_date')
        # repalace all NULLs with np.nan


        # Drop all rows containing NULL using masking
        # null_mask = df.isna() | (df == 'NULL') # bitwise OR
        # df_clean = df[~null_mask.any(axis = 1)]

        return df


db_connector1 = utils.DatabaseConnector()
extractor1 = ext.DataExtractor()
df = extractor1.read_rds_table(db_connector1, "legacy_users")

cleaner = DataCleaning()
clean_data = cleaner.clean_user_data(df)





# clean_data.info()
# null_entries = clean_data[clean_data['join_date'].isnull()]
# print(null_entries)

# print(list(clean_data['phone_number']))
# print(clean_data.isna().sum())
# print(clean_data['country_code'].unique())
# clean_data.head()

# to fix: NULL vals, date errors, incorrect info and rows with wrong info?
# first and last: no numbers, first letter capitalised
# dob ideal: yyyy-mm-dd
# email ideal: str@str.str
# address ideal: replace "\n" with ", "?
# country code: no numbers
# phone number ideal: 