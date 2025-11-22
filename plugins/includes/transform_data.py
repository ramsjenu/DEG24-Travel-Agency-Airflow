
import logging

import numpy as np
import pandas as pd

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(20)


def transform_data(df):
    """
    Transforms the given Pandas DataFrame by performing specific data
      processing operations.

    Parameters
    ----------
    df : pd.DataFrame
        A Pandas DataFrame in Parquet format that contains the data to be
          transformed.

    Returns
    -------
    pd.DataFrame
        A new Pandas DataFrame with the transformed data.
    """
    logging.info("Starting data transformation.")

    logging.debug("Extracting currency details.")
    currency_details = df['currencies'].apply(extract_currency_details)
    df = pd.concat([df, currency_details], axis=1)

    logging.debug("Extracting country names and native names.")
    df['country_name'] = df['name'].apply(lambda x: x.get('common'))
    df['official_country_name'] = df['name'].apply(lambda x: x.get('official'))
    df['common_native_names'] = df['name'].apply(lambda x:
                                                 extract_all_common_native_name
                                                 (x.get('nativeName')))

    logging.debug("Extracting languages and country codes.")
    df['languages'] = df['languages'].apply(lambda x: extract_languages(x))
    df['country_code'] = df['idd'].apply(lambda x: generate_country_codes(x))

    logging.debug("Simplifying continent and capital columns.")
    df['continents'] = df['continents'].str[0]
    df['capital'] = df['capital'].str[0]

    logging.debug("Dropping columns: 'name', 'idd', 'currencies'.")
    df = df.drop(columns=['name', 'idd', 'currencies'])

    desired_order = [
        'country_name', 'independent', 'unMember', 'startOfWeek',
        'official_country_name', 'common_native_names',
        'currency_code', 'currency_name', 'currency_symbol',
        'country_code', 'capital', 'region', 'subregion',
        'languages', 'area', 'population', 'continents'
    ]
    logging.debug("Reordering columns.")
    df = df[desired_order]

    logging.info("Data transformation completed.")
    return df


def extract_currency_details(row):
    if isinstance(row, dict) and len(row) > 0:
        valid_entry = {key: value for key, value in row.items()
                       if value is not None}
        if valid_entry:
            code = list(valid_entry.keys())[0]
            details = valid_entry[code]
            logging.debug("Currency details found: code=%s, \
                          details=%s", code, details)
            return pd.Series({
                'currency_code': code,
                'currency_name': details.get('name', None),
                'currency_symbol': details.get('symbol', None)
            })
    logging.debug("No valid currency details found.")
    return pd.Series({'currency_code': None, 'currency_name': None,
                      'currency_symbol': None})


def extract_languages(language):
    if isinstance(language, dict):
        result = ", ".join(str(x) for x in language.values() if x is not None)
        logging.debug("Extracted languages: %s", result)
        return result
    logging.debug("No valid language data found.")
    return None


def extract_all_common_native_name(native_name):
    if isinstance(native_name, dict):
        result = ", ".join(entry.get('common', '') for entry in
                           native_name.values() if isinstance(entry, dict) and
                           'common' in entry)
        logging.debug("Extracted native names: %s", result)
        return result
    logging.debug("No valid native names found.")
    return None


def generate_country_codes(idd):
    if isinstance(idd, dict):
        root = idd.get('root', '')
        suffixes = idd.get('suffixes', [])
        if isinstance(suffixes, (list, np.ndarray)):
            result = " ".join([root + suffix for suffix in suffixes])
            logging.debug("Generated country codes: %s", result)
            return result
    logging.debug("No valid country code data found.")
    # return None
    return "Unknown"
