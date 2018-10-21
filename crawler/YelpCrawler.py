from bs4 import BeautifulSoup
import requests
import json
import re
import time
import random
import traceback
from utils.logger import setup_logger
from utils.utils import string_to_integer, string_to_isoformatdate
from utils.connection import open_connection, close_connection, query

logger = setup_logger("YelpCrawler")


BASE_URL = "https://www.yelp.com"
PAGE_SIZE = 20
MAX_RETRY_ATTEMPT = 50
MIN_REVIEWS_PER_PAGE = 100

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

    # add random sleep to prevent robot blocking
    time.sleep(random.randint(3, 10))
        
    page_no = 0
    attempt = 0
    total_reviews_count = 0
    records = dict()
    condition = True

    while condition:
        page = requests.get(url + "&start={}".format(page_no * PAGE_SIZE))
        soup = BeautifulSoup(page.content, 'html.parser')
        review_elements = soup.select("div.review--with-sidebar")
        reviews_count = len(review_elements) - 1
        total_reviews_count += reviews_count

        logger.info("Fetch reviews of {}".format(name))
        logger.info("Inner attempt No {}".format(attempt))
        logger.info("---------------------------------------")
        
        if reviews_count > 0:
            logger.info("Page {} reviews count: {}".format(page_no + 1, reviews_count))
            logger.info("Total reviews count: {}".format(total_reviews_count))

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
                except KeyError as err:
                    traceback.print_exc()
                    logger.error(err)
                    pass

            records[str(page_no * PAGE_SIZE)]["review"] = reviews

        if total_reviews_count < MIN_REVIEWS_PER_PAGE:
            if reviews_count > 0:
                page_no += 1
            else:
                attempt += 1
        else:
            condition = False

    logger.info("---------------------------------------")
    
    if save_data:
        with open("./data/yelp_{}_{}_{}.json".format(category, index, name), "w") as outfile:
            json.dump(records, fp=outfile, indent=4, ensure_ascii=False)

    return records


def fetch_all_restaurants_reviews(category, url, limit=4):

    attempt = 0
    condition = True

    while condition:
        attempt += 1
        logger.info("Main attempt no {}".format(attempt))
        logger.info("=======================================")
        logger.info("Fetch all {} restaurants".format(category))
        logger.info("=======================================")
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        elements = soup.select("div.search-result")
        elements_count = len(elements)
        logger.info("restaurant count: {}".format(elements_count))

        index = 0
        for element in elements:
            if index >= limit:
                break
            restaurant_element = element.select_one("a.biz-name")
            restaurant_link = BASE_URL + restaurant_element.attrs["href"]
            restaurant_id = restaurant_element.attrs["data-hovercard-id"]
            restaurant_name = restaurant_element.text

            fetch_restaurant_reviews(category, index, restaurant_link, restaurant_id, restaurant_name, save_data=True)
            index += 1
        
        condition = not (elements_count > 0)
        


