import pandas as pd
import numpy as np
import database_utils as utils
import data_extraction as ext
import data_cleaning as clean

db_connector1 = utils.DatabaseConnector()
extractor1 = ext.DataExtractor()
df = extractor1.read_rds_table(db_connector1, "legacy_users")


# regex_uk_phone = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
# regex_de_phone = '^((((((00|\+)49[ \-/]?)|0)[1-9][0-9]{1,4})[ \-/]?)|(((00|\+)49\()|\(0\))[1-9][0-9]{1,4}\)[ \-/]?))[0-9]{1,7}([ \-/]?[0-9]{1,5})?)$'
# regex_us_phone = '^[\\(]{0,1}([0-9]){3}[\\)]{0,1}[ ]?([^0-1]){1}([0-9]){2}[ ]?[-]?[ ]?([0-9]){4}[ ]*((x){0,1}([0-9]){1,5}){0,1}$'

import pandas as pd
import numpy as np
import re

nan_arr = []
nan_count = 0

def validate_and_clean_phone(row):
    global nan_count, nan_arr

    original_number = str(row['phone_number'])
    if row['country_code'] == np.nan:
        country_code = 'NULL'
    else:
        country_code = str(row['country_code']).upper()

    # Some numbers may originate from a different country despite its df['country_code']:
    if original_number.startswith('+1') or original_number.startswith('001'):
        country_code = 'US'
    elif original_number.startswith('+44'):
        country_code = 'GB'
    elif original_number.startswith('+49'):
        country_code = 'DE'

    # Remove +1, +44, or +49 codes at the start of the number if present
    cleaned_number = re.sub(r'^\+1|^(\+44)|^\+49|^001', '', original_number)

    # Remove all parentheses, hyphens, and dots in that order
    cleaned_number = re.sub(r'[\(\)-\.\s]', '', cleaned_number)
    # cleaned_number = re.sub(r'[^\w\d\s]', '', cleaned_number)

    # If there is an 'x' present (denoting an extension), remove it and all digits after it
    cleaned_number = re.sub(r'x\d+', '', cleaned_number)

    # If there is a leading zero, remove it, unless it is a US number 
    if country_code != 'US':
        cleaned_number = re.sub(r'^0', '', cleaned_number)  

    # Add the +1, +44, or +49 country code based on the value of country_code (DE, US, or UK)
    if country_code == 'GB':
        cleaned_number = '+44' + cleaned_number
    elif country_code == 'DE':
        cleaned_number = '+49' + cleaned_number
    elif country_code == 'US':
        cleaned_number = '+1' + cleaned_number

    # Remove all whitespaces from all entries
    cleaned_number = re.sub(r'\s', '', cleaned_number)

    # If the total length of GB, DE, and US numbers is not 13, 13, and 12 respectively, set the entry to np.nan
    if (country_code == 'GB' and len(cleaned_number) not in {12, 13}) or \
       (country_code == 'DE' and len(cleaned_number) not in {12, 13}) or \
       (country_code == 'US' and len(cleaned_number) != 12) or \
       any(char.isalpha() for char in cleaned_number):
        cleaned_number = np.nan
        nan_arr.append(f"Original: {original_number}, Cleaned: {cleaned_number}, From: {country_code}")
        nan_count += 1

    return f"Original: {original_number}, Cleaned: {cleaned_number}, From: {country_code}"


db_connector1 = utils.DatabaseConnector()
extractor1 = ext.DataExtractor()
df = extractor1.read_rds_table(db_connector1, "legacy_users")

# Apply the validation function to the DataFrame
df['phone_number'] = df.apply(validate_and_clean_phone, axis=1)

# Print the results
# for result in df['phone_number']:
#     print(result)
# print("nans:", nan_count)

for nan in nan_arr:
    print(nan)
print(len(nan_arr))