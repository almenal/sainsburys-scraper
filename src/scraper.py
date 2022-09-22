#!/bin/env python3

from pathlib import Path
from time    import time, sleep
import json

import scraper_utils as utils
from urls import urls

import pandas          as pd
import pyarrow.parquet as pq
import pyarrow         as pa
from bs4      import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by   import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui  import Select
from selenium.common.exceptions     import ElementClickInterceptedException

# region Constants -----------

sainsburys_home = "https://www.sainsburys.co.uk/shop/gb/groceries"
output_parquet = Path(__file__).parents[1] / 'data' / 'sainsburys-prices.parquet'
directory_tree_path = Path(__file__).parents[1] / 'data' / 'directory_tree.json'
directory_tree = json.loads(directory_tree_path.read_text()) \
                 if directory_tree_path.exists() \
                 else None

# endregion  -----------


def main():
    browser_session = start_headless_browser()
    open_main_screen(browser_session)
    accept_cookies(browser_session)
    #TODO Re-design directory tree
    if directory_tree is None:
        directory_tree = build_directory_tree(browser_session)
    to_do_list = initialise_checker(directory_tree)
    for category in to_do_list.keys():
        #TODO Part of re-designed directory tree
        navigate_to_category_page(browser_session, category)
        set_page_size_to_max(browser_session)
        #TODO `determine_pages_to_check` Should be easy enough
        num_iter = determine_pages_to_check(browser_session)
        for _ in range(num_iter):
            page_source = BeautifulSoup(browser_session.page_source)
            items_df = utils.scrape_items(page_source)
            save_to_parquet(items_df)
            next_page(browser_session)


# region Functions -----------

def start_headless_browser():
    opts = webdriver.FirefoxOptions()
    opts.headless = True
    return webdriver.Firefox(options=opts)

def open_main_screen(driver, main_page = sainsburys_home):
    driver.get(main_page)

def accept_cookies(driver):
    a = ActionChains(driver)
    sleep(5)
    #TODO Turn into Explicit Wait
    try:
        cookies_button_xpath = """//*[@id="onetrust-accept-btn-handler"]"""
        accept_cookies_button = driver.find_element(By.XPATH, cookies_button_xpath)
        accept_cookies_button.click()
    except:
        pass

def build_directory_tree(driver, out_path=directory_tree_path):
    out_path.write_text(json.dumps())

def initialise_checker(dirtree):
    """Given a nested dict of mixed lists and strings, return a dictionary 
    where the keys are the nodes and all values are set to False."""
    status_dict = {}
    if isinstance(dirtree, dict):
        for key,value in dirtree.items():
            status_dict.update( initialise_checker(value) )
    elif isinstance(dirtree, list):
        string_values = [val for val in dirtree if isinstance(val,str)]
        status_dict.update({key:False for key in string_values})
        dict_values   = [val for val in dirtree if isinstance(val,dict)]
        for inner_dict in dict_values:
            status_dict.update( initialise_checker(inner_dict) )
    return status_dict

def set_page_size_to_max(driver):
    """Finds the 'pageSize' drop-down menu and selects the highest
    value (usually 120). Makes scraping easier as more data can be downloaded
    at once."""
    try:
        page_size_button = driver.find_element(By.XPATH, '//*[@id="pageSize"]')
    except:
        page_size_button = driver.find_element(By.ID, "pageSize")
    # Probably not really needed but just in case
    page_size_values = [
        int(e.get_attribute('value')) for e in
        page_size_button.find_elements(By.TAG_NAME, 'option')
    ]
    max_page_size = str(max(page_size_values))
    selector_obj = Select(page_size_button)
    try:
        selector_obj.select_by_value(max_page_size)
    except ElementClickInterceptedException as e:
        # Sometimes selecting the page size is not possible 
        # because of the 'Accept cookies' banner
        cookies_button_xpath = """//*[@id="onetrust-accept-btn-handler"]"""
        accept_cookies_button = driver.find_element(By.XPATH, cookies_button_xpath)
        accept_cookies_button.click()
    finally:
        selector_obj.select_by_value(max_page_size)

def save_to_parquet(df, out_fname = output_parquet):
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path = out_fname)

# endregion  -----------

if __name__ == "__main__":
    main()