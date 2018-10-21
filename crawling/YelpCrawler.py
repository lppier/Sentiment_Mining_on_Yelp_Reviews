from bs4 import BeautifulSoup
import dateutil.parser as parser
import requests
import json
import re
import locale
from datetime import datetime, timedelta
import time
import random
import logging
import traceback


BASE_URL = "https://www.yelp.com"
DATA_FILE = "data/yelp_restaurant_top10.json"
PAGE_SIZE = 20
REVIEW_LIMIT = 10
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
RETRY_ATTEMPTS = 5

RESTAURANT_CATEGORIES = {
    "chinese": {
        "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=chinese"
    },
    "japanese": {
        "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=japanese"
    },
    "indian": {
        "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=indian"
    },
    "french": {
        "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=french"
    },
}

CRAWL_TEMPLATE_DICT = {
    "author": {"css_selector": "li.user-name", "type": "string"},
    "reviewRating": {"css_selector": "div.i-stars", "type": "rating"},
    "description": {"css_selector": "div.review-content > p", "type": "string"},
    "datePublished": {"css_selector": "span.rating-qualifier", "type": "date"}
}

SENTIMENT_MAP = {
    1: "negative",
    2: "negative",
    3: "negative",
    4: "positive",
    5: "positive"
}


def setup_logger(name):
    # create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    return logger

logger = setup_logger("YelpCrawler")

def string_to_isoformatdate(datestring):
    if datestring.find("hours ago") > -1:
        hours = int(datestring.replace(" hours ago", ""))
        isodate = (datetime.today() - timedelta(hours=hours)).isoformat()[:10]
    elif datestring.find("days ago") > -1:
        days = int(datestring.replace(" days ago", ""))
        isodate = (datetime.today() - timedelta(days=days)).isoformat()[:10]
    elif datestring.find("yesterday") > -1:
        isodate = (datetime.today() - timedelta(days=1)).isoformat()[:10]
    else:
        date = parser.parse(datestring)
        isodate = str(date.isoformat())[:-9]
    return isodate


def currency_to_float(currency_string):
    return 0.0 if currency_string == "-" else locale.atof(currency_string.strip("$"))


def string_to_integer(string):
    return 0 if string == "-" else locale.atoi(string)


def string_to_float(string):
    return 0.0 if string == "-" else locale.atof(string)


def get_string(element, css_selector):
    try:
        data_element = element.select_one(css_selector)
        return data_element.get_text(separator="\n").strip().replace(u'\xa0', u' ')
    except AttributeError:
        logger.error("element: {} is not exists".format(css_selector))
        return ""


def get_rating(element, css_selector):
    try:
        data_element = element.select_one(css_selector)
        rating = data_element.attrs["title"][0]
        return string_to_integer(rating)
    except AttributeError:
        logger.error("element: {} is not exists".format(css_selector))
        return 0


def get_date(element, css_selector):
    try:
        data_element = element.select_one(css_selector)

        # remove unwanted element (Updated Review)
        unwanted_element = data_element.select_one("small.bullet-before.has-archived-review")
        if unwanted_element:
            unwanted_element.decompose()

        return string_to_isoformatdate(data_element.get_text().strip())
    except Exception:
        logger.error(traceback.print_exc())
        return ""



def fetch_restaurant_reviews(category, index, url, id, name, save_data=False):
    
    attempt = 0
    reviews_found = False
    while attempt < RETRY_ATTEMPTS and not reviews_found:
        attempt += 1
        logger.info("Attempt No {}".format(attempt))

        # add random sleep to prevent robot blocking
        time.sleep(random.randint(6, 20))
            
        page_no = 0
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        review_elements = soup.select("div.review--with-sidebar")
        reviews_count = len(review_elements)
        reviews_found = (reviews_count >= 1)

        records = {}
        while reviews_count > 1:

            # add random sleep to prevent robot blocking
            time.sleep(random.randint(2, 5))

            records[str(page_no * PAGE_SIZE)] = {
                "id":  id,
                "name": name,
                "category": category,
                "servesCuisine": get_string(soup, "span.category-str-list > a"),
                "priceRange": get_string(soup, "dd.nowrap.price-description"),
                "address": {
                    "addressLocality": "United Kingdom",
                    "addressRegion": get_string(soup, "div.map-box-address > span.neighborhood-str-list"),
                    "streetAddress": get_string(soup, "div.map-box-address > strong.street-address > address"),
                    "postalCode": None,
                    "addressCountry": "GB"
                },
                "context": "http://schema.org/",
                "image": soup.select_one("img.photo-box-img").attrs["src"],
                "type": "Restaurant",
                "telephone": get_string(soup, "span.biz-phone"),
                "review": []
            }    
            reviews = []
            logger.info("Fetch reviews from page {}".format(page_no + 1))
            logger.info("Review count: {}".format(len(review_elements)))
            for element in review_elements:
                try:
                    if element.attrs["data-review-id"]:
                        review = {
                            "reviewId": element.attrs["data-review-id"]
                        }
                        for aspect in CRAWL_TEMPLATE_DICT:
                            element_attrib = CRAWL_TEMPLATE_DICT[aspect]
                            if element_attrib["type"] == "string":
                                review[aspect] = get_string(element, element_attrib["css_selector"])
                            if element_attrib["type"] == "date":
                                review[aspect] = get_date(element, element_attrib["css_selector"])
                            if element_attrib["type"] == "rating":
                                rating = get_rating(element, element_attrib["css_selector"])
                                review[aspect] = {
                                    "ratingValue": rating
                                }
                                review["sentiment"] = SENTIMENT_MAP[rating]
                        reviews.append(review)
                except KeyError:
                    logger.error(traceback.print_exc())
                    pass

            records[str(page_no * PAGE_SIZE)]["review"] = reviews
            
            page_no += 1
            page = requests.get(url + "&start={}".format(page_no * PAGE_SIZE))
            soup = BeautifulSoup(page.content, 'html.parser')
            review_elements = soup.select("div.review--with-sidebar")
            reviews_count = len(review_elements)

        logger.info("---------------------------------------")
        
        if save_data:
            with open("./data/yelp_{}_{}_{}.json".format(category, index, name), "w") as outfile:
                json.dump(records, fp=outfile, indent=4, ensure_ascii=False)

    return records


def fetch_all_restaurants_reviews(category, url, limit=4):
    logger.info("=======================================")
    logger.info("Fetch all {} restaurants".format(category))
    logger.info("=======================================")
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    elements = soup.select("div.search-result")

    logger.info("restaurant count: {}".format(len(elements)))

    index = 0
    for element in elements:
        if index >= limit:
            break
        restaurant_element = element.select_one("a.biz-name")
        restaurant_link = BASE_URL + restaurant_element.attrs["href"]
        restaurant_id = restaurant_element.attrs["data-hovercard-id"]
        restaurant_name = restaurant_element.text

        logger.info("Fetch reviews of {}".format(restaurant_name))
        fetch_restaurant_reviews(category, index, restaurant_link, restaurant_id, restaurant_name, save_data=True)
        index += 1


if __name__ == "__main__":
    for category in RESTAURANT_CATEGORIES:
        fetch_all_restaurants_reviews(category, RESTAURANT_CATEGORIES[category]["url"], REVIEW_LIMIT)
        