import re
import pandas as pd
import numpy as np
import data_extraction as ext

ext1 = ext.DataExtractor()


def regex_matcher(entry, regex):
    """Helper method, takes an entry
    and matches it to a regex. If entry
    matches to regex pattern, it is returned,
    otherwise np.nan is returned."""
    
    match = re.match(regex, entry)
    if match:
        return entry
    else:
        print(entry)
        return np.nan

regex = r'^(?![A-Za-z0-9]{10}$).*$'

# product_df = ext1.extract_from_s3(uri='s3://data-handling-public/products.csv')
# product_df["product_name"] = product_df["product_name"].astype(str).apply(regex_matcher, args=(regex, ))


def clean_card_number(row):
    num = str(row)
    num = num.replace("?", "")
    return num

test = clean_card_number("3?45643?134?55")
print(test)
