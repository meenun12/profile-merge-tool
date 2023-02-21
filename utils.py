import datetime
import re
from typing import List, Dict, Union, Optional, Any

def create_difference(from_data: dict, to_data: dict) -> dict:

    """This method returns the difference b/w two dictionaries

        Args:
            from_data (dict): takes a dictionary
            to_data (dict): takes a dictionary

        Returns:
            dict : returns the difference b/w two dictionaries

    """

    diff_data = {}

    if not from_data and to_data:
        return diff_data

    for key in from_data:
        if from_data[key] is not None and to_data[key] in [None, '']:
            if isinstance(from_data[key], list):
                diff_data[key] = unique_list(from_data[key])
            else:
                diff_data[key] = from_data[key]
        elif isinstance(from_data[key], list) and isinstance(to_data[key], list):

            from_data[key] = unique_list(from_data[key])
            to_data[key] = unique_list(to_data[key])

            if key == 'industries' and len(to_data[key]) >= 1 :
                diff_data[key] = filter_industries(from_data_industries=from_data[key], to_data_industries=to_data[key])
            else:
                diff_data[key] = unique_list(list(set(from_data[key]) - set(to_data.get(key, []))))
        elif isinstance(from_data[key], dict) and isinstance(to_data[key], dict):

            for key2, value2 in from_data[key].items():

                if from_data[key][key2] and to_data[key][key2]:
                    if isinstance(from_data[key][key2], list) and isinstance(to_data[key][key2], list):
                        diff_data[key] = {}
                        diff_data[key][key2] = unique_list(set(from_data[key][key2]) - set(to_data[key][key2]))
                else:
                    diff_data[key] = {key2: value2 for key2, value2 in from_data[key].items() if key2 in to_data[key]}
                    diff_data[key] = unique_dict(diff_data[key])

    return diff_data

