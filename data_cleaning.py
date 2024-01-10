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

    def __name_cleaning(self, df, target_col):
        """
        """
        valid_name_pattern = re.compile(r'^[A-Za-zÄäÖöÜüßé.\'\s-]+$')

        df[target_col] = df[target_col].apply(lambda x: x.capitalize() \
                                if valid_name_pattern.match(str(x)) else np.nan)

        return df


    def __email_cleaning(self, df, target_col):
        """
        """
        # Some entries are valid but mistakenly use @@ instead of @:
        df[target_col] = df[target_col].str.replace(r'@@', '@', regex=True)

        # Check for str@str.str pattern
        email_pattern = re.compile(r'^[\w\.-]+@[a-zA-Z\d\.-]+\.[a-zA-Z]{2,}$')

        df[target_col] = df[target_col].apply( \
            lambda x: x if email_pattern.match(str(x)) else np.nan)

        return df


    def __country_cleaning(self, df, target_col):
        """
        """
        valid_countries = ['Germany', 'United Kingdom', 'United States']

        df[target_col] = df[target_col].apply(\
            lambda x: x.strip() if str(x).strip() in valid_countries else np.nan)

        return df


    def __address_cleaning(self, row):
        """Private method to clean customer addresses. 
        1. All valid entries have address lines separated by
        a \n character. Replace all these with ", ".
        2. If an entry does not have a comma anywhere in the
        string, as a valid address would, then it is invalidated.   
        """
        address = row['address']
        address = re.sub(r'\n', ', ', address)
        if "," not in address:
            address = np.nan

        return address
    

    def __date_cleaning(self, df, target_col, date_format):
        """
        """
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
        df[target_col] = pd.to_datetime(df[target_col], format=date_format, errors='coerce')

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
        3. Remove all parantheses, hyphens, dots.
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
        df['country_code'] = df.apply(self.__country_code_cleaning, axis=1)
        df['phone_number'] = df.apply(self.__phone_number_cleaning, axis=1)
        df['address'] = df.apply(self.__address_cleaning, axis=1)
        df = self.__date_cleaning(df, target_col='date_of_birth', date_format="%Y-%m-%d")
        df = self.__date_cleaning(df, target_col='join_date', date_format="%Y-%m-%d")
        df = self.__name_cleaning(df, target_col='first_name')
        df = self.__name_cleaning(df, target_col='last_name')
        df = self.__email_cleaning(df, target_col='email_address')
        df = self.__country_cleaning(df, target_col='country')
        df = df.drop_duplicates() # remove any exact duplicates

        # Standardise all invalid entries (make them all np.nan)
        df = df.fillna(np.nan)
        df.replace('NULL', np.nan, inplace=True)

        return df


    def clean_card_data(self, df):
        """ Workflow:
        - Remove any df cols that are not: card_number, expiry_date,
        card_provider or date_payment_confirmed.
        - Clean card_provider. Invalidate all entries that are not in 
        provider list.
        - Cast all card_number entries to str type.
        - Clean card_number row-wise (using .apply()). 
        - Clean expiry_date.
        - Clean date_payment_confirmed using __date_cleaning() helper. 
        
        Replace all NULLs with np.nan. Execute df.fillna(np.nan) also.
        """
        provider_card_lengths = {
            'Diners Club / Carte Blanche': 14,
            'American Express': 15,
            'JCB 16 digit': 16,
            'JCB 15 digit': 15,
            'Maestro': 12,
            'Mastercard': 16,
            'Discover': 16,
            'VISA 19 digit': 19,
            'VISA 16 digit': 16,
            'VISA 13 digit': 13
        }
        valid_providers = list(provider_card_lengths.keys())

        # Cleaning card provider col
        def __clean_card_provider(row):
            provider = str(row['card_provider'])
            if provider in valid_providers:
                return provider
            else:
                return np.nan
            
        def __clean_card_number(row):
            num = str(row['card_number'])
            provider = str(row['card_provider'])

            if (provider in valid_providers) and \
                (len(num) == provider_card_lengths[provider]) and \
                (num.isdigit()):
                return num
            else:
                return np.nan

        def __clean_expiry_date(df, target_col):
            # Using regex instead of __date_cleaning() as its parser
            # gets confused over the mm/yy date format.
            expiry_regex = r'\d{2}/\d{2}'
            
            def process(entry):
                match = re.match(expiry_regex, entry)
                if match:
                    return entry
                else: 
                    return np.nan
            
            df[target_col] = df[target_col].astype(str).apply(process)

            return df


        # Ops
        df = df[['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']]
        df['card_number'] = df['card_number'].astype(str)

        df['card_provider'] = df.apply(__clean_card_provider, axis=1)
        df['card_number'] = df.apply(__clean_card_number, axis=1)
        df = self.__date_cleaning(df, target_col='date_payment_confirmed', date_format="%Y-%m-%d")
        df = __clean_expiry_date(df, "expiry_date")

        df = df.drop_duplicates() # remove any exact duplicates
        df = df.fillna(np.nan)
        df.replace('NULL', np.nan, inplace=True)

        return df


    def clean_store_data(self, df):
        """ Cleans the store data as follows.
        1. 'lat' col is dropped, since it is empty.
        2. Address col is cleaned using the previously-
        defined __address_cleaning method.
        3. Longitude, Latitude and Staff Numbers are cleaned 
        using pd.to_numeric(), which sets all non-numerical 
        entries to NaN.
        4. Locality is cleaned using __name_cleaning method.
        All valid entries are then capitalised.
        5. Store Code is cleaned using a regex which matches
        to the valid store code pattern.
        6. Opening Date is cleaned using __date_cleaning method.
        7. Country code is cleaned using __country_code_cleaning.
        8. Store Type and Continent entries are only valid if 
        they are contained in their validity lists.
        9. Finally, all exact duplicates are removed, all NULL
        values are replaced with np.nan.
        """
        df = df.drop(columns=['lat'])

        df['address'] = df.apply(self.__address_cleaning, axis=1)
        df['longitude'] = pd.to_numeric(df["longitude"], errors="coerce")
        df = self.__name_cleaning(df, target_col='locality')
        df['locality'] = df['locality'].str.title()
        
        store_code_regex = re.compile(r'^[A-Z]{2,3}-[0-9A-Z]{8}$')
        df['store_code'] = df["store_code"].apply(lambda x: x if pd.notna(x) \
                                    and bool(store_code_regex.match(str(x))) else np.nan)

        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df['staff_numbers'] = df['staff_numbers'].astype('Int64')

        df = self.__date_cleaning(df, target_col='opening_date', date_format="%Y-%m-%d")

        valid_store_types = ["Web Portal", "Local", "Super Store", "Mall Kiosk", "Outlet"]
        df["store_type"] = df["store_type"].apply(\
                lambda x: x.strip() if str(x).strip() in valid_store_types else np.nan)
        
        df['latitude'] = pd.to_numeric(df["latitude"], errors="coerce")

        df['country_code'] = df.apply(self.__country_code_cleaning, axis=1)

        valid_continents = ["Europe", "America"]
        df["continent"] = df["continent"].apply(\
                lambda x: x.strip() if str(x).strip() in valid_continents else np.nan)
        
        df = df.drop_duplicates() # remove any exact duplicates
        df = df.fillna(np.nan)
        df.replace('NULL', np.nan, inplace=True)

        return df





