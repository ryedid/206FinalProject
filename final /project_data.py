
import requests
from bs4 import BeautifulSoup
import json

def load_json(filename):
    '''
    Loads a JSON cache from filename if it exists and returns dictionary
    with JSON data or an empty dictionary if the cache does not exist

    Parameters
    ----------
    filename: string
        the name of the cache file to read in

    Returns
    -------
    dict
        if the cache exists, a dict with loaded data
        if the cache does not exist, an empty dict
    '''
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        return {}

def write_json(filename, dict):
    '''
    Encodes dict into JSON format and writes
    the JSON to filename to save the search results

    Parameters
    ----------
    filename: string
        the name of the file to write a cache to
    
    dict: cache dictionary

    Returns
    -------
    None
        does not return anything
    '''  
    with open(filename, 'w') as file:
        json.dump(dict, file, indent=2)

    pass