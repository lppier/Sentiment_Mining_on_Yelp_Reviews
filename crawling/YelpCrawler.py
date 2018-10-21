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


BASE_URL = "https://www.yelp.com"
DATA_FILE = "data/yelp_restaurant_top10.json"
PAGE_SIZE = 20
REVIEW_LIMIT = 10

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
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    return 0.0 if currency_string == "-" else locale.atof(currency_string.strip("$"))


def string_to_integer(string):
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    return 0 if string == "-" else locale.atoi(string)


def string_to_float(string):
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    return 0.0 if string == "-" else locale.atof(string)


def get_string(element, css_selector):
    data_element = element.select_one(css_selector)
    return data_element.get_text(separator="\n").strip().replace(u'\xa0', u' ')


def get_rating(element, css_selector):
    data_element = element.select_one(css_selector)
    rating = data_element.attrs["title"][0]
    return string_to_integer(rating)


def get_date(element, css_selector):
    data_element = element.select_one(css_selector)

    # remove unwanted element (Updated Review)
    unwanted_element = data_element.select_one("small.bullet-before.has-archived-review")
    if unwanted_element:
        unwanted_element.decompose()

    return string_to_isoformatdate(data_element.get_text().strip())


def fetch_restaurant_reviews(url, id, name, category, save_data=False):
    
    page_no = 0
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    reviews = soup.select("div.review--with-sidebar")
    reviews_count = len(reviews)

    records = {}

    while reviews_count > 1:
        records[str(page_no * PAGE_SIZE)] = {
            "id":  id,
            "name": name,
            "category": category,
            "servesCuisine": soup.select_one("span.category-str-list > a").text.strip(),
            "priceRange": soup.select_one("dd.nowrap.price-description").text.strip(),
            "address": {
                "addressLocality": "United Kingdom",
                "addressRegion": soup.select_one("div.map-box-address > span.neighborhood-str-list").text.strip(),
                "streetAddress": soup.select_one("div.map-box-address > strong.street-address > address").get_text(separator="\n"),
                "postalCode": None,
                "addressCountry": "GB"
            },
            "@context": "http://schema.org/",
            "image": soup.select_one("img.photo-box-img").attrs["src"],
            "@type": "Restaurant",
            "telephone": soup.select_one("span.biz-phone").text.strip(),
            "review": []
        }    
        reviews = []
        logger.info("Fetch reviews from page {}".format(page_no + 1))
        for review in reviews:
            try:
                if review.attrs["data-review-id"]:
                    review = {
                        "reviewId": review.attrs["data-review-id"]
                    }
                    for aspect in CRAWL_TEMPLATE_DICT:
                        element_attrib = CRAWL_TEMPLATE_DICT[aspect]
                        if element_attrib["type"] == "string":
                            review[aspect] = get_string(review, element_attrib["css_selector"])
                        if element_attrib["type"] == "date":
                            review[aspect] = get_date(review, element_attrib["css_selector"])
                        if element_attrib["type"] == "rating":
                            review[aspect]["ratingValue"] = get_rating(review, element_attrib["css_selector"])
                            review["sentiment"] = SENTIMENT_MAP[review[aspect]]
                    reviews.append(review)
            except(KeyError):
                pass

        records[str(page_no * PAGE_SIZE)]["review"] = reviews
        
        page_no += 1
        page = requests.get(url + "&start={}".format(page_no * PAGE_SIZE))
        soup = BeautifulSoup(page.content, 'html.parser')
        reviews = soup.select("div.review--with-sidebar")
        reviews_count = len(reviews)

    logger.info("---------------------------------------")
    
    if save_data:
        with open("./data/yelp_{}_{}.json".format(category, name), "w") as outfile:
            json.dump(reviews, fp=outfile, indent=4)

    return reviews


def fetch_all_restaurants_reviews(category, url, limit=4):
    logger.info("=======================================")
    logger.info("Fetch all {} restaurants reviews".format(category))
    logger.info("=======================================")
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    elements = soup.select("div.search-result")

    index = 0
    reviews = []
    for element in elements:
        if index >= limit:
            break
        restaurant_element = element.select_one("a.biz-name")
        restaurant_link = BASE_URL + restaurant_element.attrs["href"]
        restaurant_id = restaurant_element.attrs["data-hovercard-id"]
        restaurant_name = restaurant_element.text

        logger.info("Fetch reviews of {}".format(restaurant_name))
        review = fetch_restaurant_reviews(restaurant_link, restaurant_id, restaurant_name, category, save_data=True)
        reviews.append(review)
        index += 1
    return reviews


if __name__ == "__main__":
    for category in RESTAURANT_CATEGORIES:
        data = fetch_all_restaurants_reviews(category, RESTAURANT_CATEGORIES[category]["url"], REVIEW_LIMIT)
        