import pandas as pd
import numpy as np
import database_utils as utils
import data_extraction as ext
import re


class DataCleaning():

    """Data cleaning class to clean pandas dataframes for
    the AiCore MRDC project.
    
    Methods named with preceding double underscores are
    marked as private and used to clean a target column in a
    given dataframe. The class contains methods which clean
    company customer, card, store, products, orders and date
    events data. All methods are unique to this specific 
    dataset and their dataframe columns.
    """

    def __name_cleaning(self, df, target_col):
        """Method to clean customer first/last names. A regex
        is used to match entries to a alphabetical word
        where capitalised letters, lowercase letters and German
        umlauts are allowed. Hyphens, dots, single quotes and 
        spaces are allowed. Entry matches are kept in dataframe
        and are capitalised, non-matches are set to np.nan (NaN).
        Cleaned dataframe is returned.
        """
        valid_name_pattern = re.compile(r'^[A-Za-zÄäÖöÜüßé.\'\s-]+$')

        df[target_col] = df[target_col].apply(lambda x: x.capitalize() \
                                if valid_name_pattern.match(str(x)) else np.nan)

        return df


    def __email_cleaning(self, df, target_col):
        """Method to clean dataframe email entries. A regex
        is used to match for a str1@str2.str3 pattern. str1 
        and str2 allows for letters, numbers, dots and hyphens. 
        str3 allows only for two or more letters (domain).
        Method also replaces some rogue entries containing '@@' 
        with '@'. Entry matches are kept in dataframe
        and are capitalised, non-matches are set to np.nan (NaN).
        Cleaned dataframe is returned.
        """
        # Some entries are valid but mistakenly use @@ instead of @:
        df[target_col] = df[target_col].str.replace(r'@@', '@', regex=True)

        # Check for str@str.str pattern
        email_pattern = re.compile(r'^[\w\.-]+@[a-zA-Z\d\.-]+\.[a-zA-Z]{2,}$')

        df[target_col] = df[target_col].apply( \
            lambda x: x if email_pattern.match(str(x)) else np.nan)

        return df


    def __country_cleaning(self, df, target_col):
        """Method to clean 'country' column in a given 
        dataframe. If entry matches to allowed countries 
        in valid_countries, the entry is kept. Otherwise,
        entry is set to np.nan. Cleaned dataframe is 
        returned.
        """
        valid_countries = ['Germany', 'United Kingdom', 'United States']

        df[target_col] = df[target_col].apply(\
            lambda x: x.strip() if str(x).strip() in valid_countries else np.nan)

        return df


    def __address_cleaning(self, row):
        """Method to clean customer addresses. All valid entries 
        have address lines separated by a \n character, which are
        replaced with ", ". If an entry does not have a comma anywhere 
        in the string, as a valid address would, then it is invalidated. 
        Invalid entries are set to np.nan. Valid entries are unchanged. 
        Return: cleaned address.  
        """
        address = row['address']
        address = re.sub(r'\n', ', ', address)
        if "," not in address:
            address = np.nan

        return address
    

    def __date_cleaning(self, df, target_col, date_format):
        """Method to clean date patterns using dateutil.parser
        class. Parser is applied to the target_col and all
        invalid date entries are converted to pd.NaT (Not-a-Time
        type). The modified column is converted to a pandas
        datetime type. 
        Return: cleaned dataframe.
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
        """Method to clean country codes.
        Codes must be 2 characters long and only contain letters.
        Invalid entries set to np.nan, otherwise remain 
        unchanged. Cleaned code is returned.
        """
        code = row['country_code']
        if len(code) != 2 or not code.isalpha():
            return np.nan
        else:
            return code


    def __phone_number_cleaning(self, row):
        """Method to clean GB, US and DE phone numbers, as follows:
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
        Return cleaned phone number.
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

    def __regex_matcher(self, entry, regex):
        """Helper method, takes an entry
        and matches it to a regex. If entry
        matches to regex pattern, it is returned,
        otherwise np.nan is returned."""

        match = re.match(regex, entry)
        if match:
            return entry
        else:
            return np.nan
        
    def __in_list(self, entry, target_list):
        """Helper method, takes an entry
        and checks if it is contained within
        target_list. If true, entry is returned,
        otherwise np.nan is returned."""

        if entry in target_list:
            return entry
        else:
            return np.nan


    def clean_user_data(self, df):
        """Method to clean user (customer) data. Helper
        methods are combined to parse and clean entries from
        every column in the dataframe. After cleaning, all
        exact duplicates are dropped and the invalid 
        entries (e.g., 'NULL' {str}) are standardised 
        (set to np.nan).
        Cleaned dataframe returned.
        """
        uuid_regex = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        spam_entry_regex = r'^(?![A-Za-z0-9]{10}$).*$' # negative assert, as this is the pattern for invalid entries

        cols_to_drop = ['index']
        if all(col in df.columns for col in cols_to_drop):
            df = df.drop(columns=cols_to_drop)
            print("Columns dropped successfully.")
        else:
            print("One or more columns do not exist.")        

        df['country_code'] = df.apply(self.__country_code_cleaning, axis=1)
        df['phone_number'] = df.apply(self.__phone_number_cleaning, axis=1)
        df['address'] = df.apply(self.__address_cleaning, axis=1)
        df = self.__date_cleaning(df, target_col='date_of_birth', date_format="%Y-%m-%d")
        df = self.__date_cleaning(df, target_col='join_date', date_format="%Y-%m-%d")
        df = self.__name_cleaning(df, target_col='first_name')
        df = self.__name_cleaning(df, target_col='last_name')
        df = self.__email_cleaning(df, target_col='email_address')
        df = self.__country_cleaning(df, target_col='country')
        df["company"] = df["company"].astype(str).apply(self.__regex_matcher, \
                                                        args=(spam_entry_regex, ))
        df['user_uuid'] = df['user_uuid'].astype(str).apply(self.__regex_matcher, \
                                args=(uuid_regex, ))
        df = df.drop_duplicates() # remove any exact duplicates

        # Standardise all invalid entries (make them all np.nan)
        df = df.fillna(np.nan)
        df.replace('NULL', np.nan, inplace=True)
        df.replace('Null', np.nan, inplace=True)
        df.replace('null', np.nan, inplace=True)
        df.dropna(inplace=True, how='all')
        df.dropna(inplace=True, thresh=9)

        return df


    def clean_card_data(self, df):
        """Method to clean customer card data, as follows:
        - Remove any df cols that are not: card_number, expiry_date,
        card_provider or date_payment_confirmed.
        - Clean card_provider. Invalidate all entries that are not in 
        provider list.
        - Cast all card_number entries to str type.
        - Clean card_number row-wise. Conditions for entry to be valid:
        must be digits only, associated provider must be in the provider
        list, and length of number must match to the provider in
        provider_card_lengths.  
        - Clean expiry_date. Regex applied here to ensure mm/yy format.
        - Clean date_payment_confirmed using __date_cleaning() helper. 
        - Remove exact duplicates and standardise invalid entries.
        Return: cleaned df.
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
            
        # def __clean_card_number_legacy(row):
        #     num = str(row['card_number'])
        #     provider = str(row['card_provider'])

        #     if (provider in valid_providers) and \
        #         (len(num) == provider_card_lengths[provider]) and \
        #         (num.isdigit()):
        #         return num
        #     else:
        #         return np.nan
            
        def __clean_card_number(row):
            num = str(row)
            num = num.replace("?", "")
            if num.isnumeric():
                return num
            else:
                # print(num)
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
        # df['card_number'] = df.apply(__clean_card_number, axis=1)
        df['card_number'] = df['card_number'].str.replace(r'\D+', '')
        df['card_number'] = df['card_number'].str.replace('?', '')
        df = self.__date_cleaning(df, target_col='date_payment_confirmed', date_format="%Y-%m-%d")
        df = __clean_expiry_date(df, "expiry_date")

        df = df.drop_duplicates() # remove any exact duplicates
        # df = df.fillna(np.nan)
        # df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)
        # df.dropna(inplace=True, subset=['date_payment_confirmed'])
        print(len(df))

        return df


    def clean_store_data(self, df):
        """Method to clean the store data, as follows.
        1. 'lat' col is dropped, since it's empty.
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
        Return: cleaned df.
        """
        cols_to_drop = ['lat', 'index']
        if all(col in df.columns for col in cols_to_drop):
            df = df.drop(columns=cols_to_drop)
            print("Columns dropped successfully.")
        else:
            print("One or more columns do not exist.")


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
        df.replace('Null', np.nan, inplace=True)
        df.replace('null', np.nan, inplace=True)
        df.dropna(inplace=True, how="all")

        return df

    
    def __convert_product_weights(self, df, target_col):
        """Hepler method to clean and validate the "weight" column
        of the products dataframe. 
        """

        def weights_validation(weight):
            weight = str(weight)
            match = re.match(r'(?:(\d+)\s*x\s*)?([\d.]+)\s*([a-zA-Z]+)', weight)
            if match:
                multiplier, value, unit = match.groups()
                multiplier = int(multiplier) if multiplier else 1
                value = float(value)

                if unit == "kg":
                    return multiplier * value
                elif unit == "g" or unit == "ml":
                    return multiplier * (value / 1000)
                elif unit == "oz":
                    return multiplier * (value * 0.0283495)  # 1 oz = 0.0283495 kg
                else:
                    return np.nan
            else:
                return np.nan
        
        df[target_col] = df[target_col].astype(str).apply(weights_validation)

        return df


    def clean_products_data(self, df):
        """Method to clean the products dataframe. 
        For the product_name column, it is next to impossible to create
        a generalised regex for cleaning the name of a product, as the 
        valid entries are so unique and can be valid if the entry contains 
        one or many words, numbers, hyphens etc. Therefore, spam entries
        such as "LB3D71C025" or "9SX4G65YUX" were invalidated using a 
        regex that matches to a 10-character str containing numbers and/or 
        all uppercase chars. This, of course, does not invalidate spam entries 
        that are not 10 characters long.
        Return: cleaned df.
        """
        def clean_removed(entry):
            if entry == "Still_avaliable" or entry == "Still_available":
                return False
            elif entry == "Removed":
                return True
            else:
                return np.nan

        # Required regex
        product_price_regex = r'^£\d+\.\d{2}$'
        uuid_regex = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        product_code_regex = r'^[A-Za-z]\d{1,2}-\d+[a-zA-Z]?$'
        spam_entry_regex = r'^(?![A-Za-z0-9]{10}$).*$' # negative assert, as this is the pattern for invalid entries

        # Drop empty col
        cols_to_drop = ['Unnamed: 0']
        if all(col in df.columns for col in cols_to_drop):
            df = df.drop(columns=cols_to_drop)
            print("Columns dropped successfully.")
        else:
            print("One or more columns do not exist.")

            
        # Validating and cleaning individual columns
        df["product_name"] = df["product_name"].astype(str).apply(self.__regex_matcher, args=(spam_entry_regex, ))
        df["product_price"] = df["product_price"].astype(str).apply(self.__regex_matcher, args=(product_price_regex, ))
        df = self.__convert_product_weights(df, "weight")
        df["category"] = df["category"].astype(str).apply(self.__in_list, \
                         args=(['toys-and-games','sports-and-leisure','pets',\
                         'homeware','health-and-beauty','food-and-drink','diy'], ))
        df['EAN'] = df['EAN'].astype(str).apply(lambda x: x if x.isdigit() else np.nan)
        df = self.__date_cleaning(df, target_col='date_added', date_format="%Y-%m-%d")
        df['uuid'] = df['uuid'].astype(str).apply(self.__regex_matcher, \
                                args=(uuid_regex, ))
        df['removed'] = df['removed'].astype(str).apply(clean_removed)
        df['product_code'] = df['product_code'].astype(str).apply(self.__regex_matcher, \
                                                args=(product_code_regex, ))
        
        df = df.drop_duplicates() # remove any exact duplicates
        df = df.fillna(np.nan)
        df.replace('NULL', np.nan, inplace=True)
        df.replace('nan', np.nan, inplace=True)
        df.dropna(inplace=True)
            
        return df


    def clean_orders_table(self, df):
        """Method to clean and validate the orders table.
        Columns defined in orders_drop_cols are dropped and
        the table is reindexed.
        Return: cleaned dataframe.
        """
        orders_drop_cols = ["first_name", "last_name", "1", "index", "level_0"]

        # Check if all columns exist before dropping
        if all(col in df.columns for col in orders_drop_cols):
            df = df.drop(columns=orders_drop_cols)
            print("Columns dropped successfully.")
        else:
            print("One or more columns do not exist.")

        df = df.reindex()
        
        return df


    def clean_date_events(self, df):
        """Method to clean and validate the date events dataframe.
        Return: cleaned dataframe.
        """
        # Required regex
        timestamp_regex = r'^([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$'
        year_regex = r'^(19\d{2}|20\d{2})$'
        date_uuid_regex = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

        # Cleaning ops
        df["timestamp"] = df["timestamp"].astype(str).apply(self.__regex_matcher, args=(timestamp_regex, ))
        df['month'] = df['month'].astype(str).apply(lambda x: x if x.isdigit() and 1 <= float(x) <= 12 else np.nan)
        df['day'] = df['day'].astype(str).apply(lambda x: x if x.isdigit() and 1 <= float(x) <= 31 else np.nan)
        df["year"] = df["year"].astype(str).apply(self.__regex_matcher, args=(year_regex, ))
        df["time_period"] = df["time_period"].astype(str).apply(self.__in_list, \
                            args=(["Evening", "Morning", "Midday", "Late_Hours"], ))
        df["date_uuid"] = df["date_uuid"].astype(str).apply(self.__regex_matcher, args=(date_uuid_regex, ))

        df = df.drop_duplicates() # remove any exact duplicates
        df = df.fillna(np.nan)
        df.replace('NULL', np.nan, inplace=True)
        df.dropna(inplace=True)

        return df   