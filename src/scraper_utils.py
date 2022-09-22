#!/bin/env python3
"""
~~ SAINSBURY'S SCRAPER ~~
=========================

Utils to scrape and collect sainsburys product info.
Main
"""

import requests
from pathlib import Path
from time import time
from datetime import datetime

import bs4
import pandas as pd
from bs4 import BeautifulSoup
from urls import urls

class_abbvs = {"c":"class", "id":"id"}
sainburys_home = "https://www.sainsburys.co.uk/shop/gb/groceries"


# region Navigate site tree - Selenium ----------------------------------

# TODO: Use Selenium code in notebook to put it all together:
#
#  1. Open sainsburys.co.uk in a headless browser session to avoid opening window
#   1.1 Wait and accept cookies!
#  2. Locate 'Groceries' button with XPath (always same location)
#  3. Go item by item in the list 
#     - keep track of navigation progress in global variable
#     - depth first
#        - Get all `megaNavListItem` objects visible and look for deltas
#          after hovering mouse. If there is an increase, keep navigating
#          Consider leaf when there is no difference
#     - First build tree of directories, then navigate one by one and mark off
#  4. Click, wait to load -> parse with functions above
#   4.1. Select 120 items per page
#   4.2. Read total products available
#   4.3. Based on total products, decide how many iterations of clicking on 'next'
#   4.5. Get page source and turn into BeautifulSoup
#   4.5. use utils.scrape_products to extract info

# endregion



# region Scrape HTML page with grid ----------------------------------

def url_to_soup(url, verbose = False):
    t0 = time()
    resp = requests.get(url)
    t1 = time()
    if verbose:
        print(f"Page downloaded in {t1 - t0:.6f}s")
    soup = BeautifulSoup(resp._content.decode("utf-8"), 'html.parser')
    return soup

def scrape_items(soup):
    # First assume structure is repeated and page is first div of body
    page = soup.body.div
    # Check that `page` is actually page and fall back to find_all (slower)
    if not page.attrs == {'id':'page'}:
        page = soup.find_all(id="page")[0]
    grid_items = page.find_all(attrs={"class":"gridItem"})
    items = []
    for i,g in enumerate(grid_items):
        title, thubmnail = scape_item_thumbnail(g)
        price_unit       = scrape_price_per_unit(g)
        price_measure    = scrape_price_per_measure(g)
        items.append({
            'title'         : title,
            'thubmnail'     : thubmnail,
            'price_unit'    : price_unit,
            'price_measure' : price_measure
        })
    items_df = (
        pd.DataFrame.from_dict(items)
        .assign(scraping_date = str(datetime.now()))
    )
    return items_df

def scape_item_thumbnail(grid_item):
    grid_descendants = list(grid_item.find_all("h3")[0].descendants)
    descendants_skip_newlines = [g for g in grid_descendants if g != "\n"]
    # Unpack into individual components
    _, title, thumbnail_link = descendants_skip_newlines
    thumbnail_path = check_thumbnail_in_local(thumbnail_link)
    return title.strip(), thumbnail_path

def check_thumbnail_in_local(link):
    #TODO make a local DB for thumbnails and only download unsaved ones
    # thumbnail_request = requests.get(link)
    return ""

def scrape_price_per_unit(grid_item):
    return ''.join(
        grid_item
        .find_all(attrs={"class":"pricePerUnit"})[0]
        .strings # alternative just use .string which returns a `str`
    ).strip()

def scrape_price_per_measure(grid_item):
    return ''.join(
        grid_item
        .find_all(attrs={"class":"pricePerMeasure"})[0].strings
    ).strip()

# endregion


# region [OLD] Navigate site tree - BeautiulSoup ----------------------------------

def navigate_root(home_url=sainburys_home):
    """We consider the root of the Sainsbury's sites to be the site from which
    all others can be reached. In this page, we can access the first branches
    of each category (Fruit&Veg, Meat, etc). They are all inside a div of class
    `megaNavListItem`, but some of the items there point to uninteresing sites
    (e.g. Favourites, Discover, etc). The interesting sites have `href` 
    attributes that point to sainsburys.co.uk sites, which we will retrieve.

    The maximum recursion is 4 
        (e.g. Groceries > Hair care > Natural > Shampoo)
    """
    url_response = requests.get(home_url).content.decode()
    home_soup = BeautifulSoup(url_response, 'html.parser')
    megaNavListItem_children = [
        fetch_href(child)
        for child in home_soup.find_all(attrs = {'class':'megaNavListItem'})
    ]
    # `fetch_href` might return (None,None) tuples; get rif of those
    children_w_link = dict(
        (title,link) for title,link in megaNavListItem_children 
        if title is not None
    )
    
    # return children_w_link
    children = {}
    for title,link in megaNavListItem_children:
        if title is None or link is None:
            continue
        if not link.startswith('https://www.sainsburys.co.uk/'):
            continue
        navigate_leaf_lvl_1(link)

def fetch_href(bs_element):
    """Extract the href from a `megaNavListItem` item"""
    for child in bs_element.children:
        if not hasattr(child, 'strings'):
            continue
        title = [s.strip() for s in child.strings if s != "\n"][0]
        if hasattr(child, "attrs"):
            href = child.get('href', None)
            return (title, href)
    return None,None

def navigate_leaf_lvl_1(breadcrumbNav):
    for item in breadcrumbNav.descendants:
        # Guard clauses
        if getattr(item, 'name', '') != 'li':
            return None
        item_contents = getattr(item, 'contents', '')[0]
        if not hasattr(item_contents, 'attrs'):
            return None
        href = item_contents.attrs.get('href', '')
        if not href:
            return None
        

# endregion


# region Old ----------------------------------

class TagNavigator(object):
    def __init__(self, bs:BeautifulSoup):
        self.bs = bs

    def navigate(self, breadcrumbs):
        """Breadcrumbs should indicate the path from page body separated by '/'
        where each node of the page tree is preceded by the attribute type
        e.g.
        id:page/id:main/id:content/id:productsContainer/
            id:producLister/c:[productLister,gridView]
        """
        steps = [lvl.split(":") for lvl in breadcrumbs.split("/")]
        tag = self.bs.body
        for attr,step in steps:
            if step.startswith("[") and step.endswith("]"):
                step = step[1:-1].split(",")
            tag = fetch_tag(tag,
                            child_value = step,
                            child_attr = class_abbvs[attr])
        self.stem = tag

def fetch_tag(bs_tag, child_value, child_attr = "id"):
    """[DEPRECATED]:
    just use page.find_all(attrs = {'class':'productNameAndPromotions'})
    """
    valid_tag = [
        tag for tag in bs_tag.contents
        if isinstance(tag, bs4.element.Tag) and
           tag.attrs.get(child_attr, "") == child_value
    ]
    return valid_tag[0]

def __main():
    print(f"There are {len(urls)} categories")
    for j, (category, url_list) in enumerate(urls.items()):
        print(f"[{j+1}/{len(urls)}] {category}...")
        soups = map(lambda u: url_to_soup(u, verbose = True), url_list)
        items_df = pd.concat([scrape_items(soup) for soup in soups])
        items_df['category'] = category
        print("Saving to parquet...", end  = " ")
        t0 = time()
        save_to_parquet(items_df)
        print(f"done. Elapsed: {time() - t0:.6f}")

# endregion

# selenium.webdriver.remote.webelement.WebElement
