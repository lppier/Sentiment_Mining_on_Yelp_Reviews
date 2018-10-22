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


USER_AGENTS = [
    "Mozilla/5.0 (Windows0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko	IE 11",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko	IE 11",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/69.0.3497.81 Chrome/69.0.3497.81 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:63.0) Gecko/20100101 Firefox/63.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36 OPR/55.0.2994.61",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/61.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (Windows NT 6.1; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:52.0) Gecko/20100101 Firefox/52.0",
    "Mozilla/5.0 (X11; CrOS x86_64 10895.56.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.95 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Mobile/15E148 Safari/604.1	Mobile",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36 OPR/56.0.3051",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 YaBrowser/18.9.0.3467 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; rv:62.0) Gecko/20100101 Firefox/62.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36	Chrome 65.06-bit"
]

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



def fetch_restaurant_reviews(category, index, url, id, name, max_reviews_count, save_data=False):

    # add random sleep to prevent robot blocking
    time.sleep(random.randint(10, 30))
        
    page_no = 0
    attempt = 0
    total_reviews_count = 0
    records = dict()
    condition = True

    # get approximate English reviews count
    english_reviews_count = int(max_reviews_count * 0.9)
    logger.info("---------------------------------------")
    logger.info("Fetch reviews of {}".format(name))
    logger.info("Reviews: {}".format(max_reviews_count))
    logger.info("English Reviews (approx): {}".format(english_reviews_count))
    logger.info("---------------------------------------")

    while condition:
        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }
        page = requests.get(url + "&start={}".format(page_no * PAGE_SIZE), headers=headers)
        soup = BeautifulSoup(page.content, 'html.parser')
        review_elements = soup.select("div.review--with-sidebar")
        reviews_count = len(review_elements) - 1
        total_reviews_count += reviews_count

        logger.info("Inner attempt No {}".format(attempt + 1))
        logger.info("---------------------------------------")
        
        if reviews_count > 0:
            logger.info("Page {} reviews: {}".format(page_no + 1, reviews_count))
            logger.info("Total reviews: {}".format(total_reviews_count))

            # add random sleep to prevent robot blocking
            time.sleep(random.randint(5, 15))

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

        if total_reviews_count < english_reviews_count:
            if reviews_count > 0:
                page_no += 1
            else:
                attempt += 1
        else:
            condition = False

    logger.info("---------------------------------------")
    
    if save_data:
        filename = "./data/yelp_{}_{}_{}.json".format(category, index, name)
        with open(filename, "w") as outfile:
            logger.info("Save reviews to {}".format(filename))
            json.dump(records, fp=outfile, indent=4, ensure_ascii=False)
            logger.info("---------------------------------------")

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
        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }
        page = requests.get(url, headers=headers)
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
            max_reviews_count_element = get_string(element, "div.biz-rating.biz-rating-large.clearfix > span.review-count.rating-qualifier")
            max_reviews_count = int(max_reviews_count_element.replace(" reviews", "").strip())
            fetch_restaurant_reviews(category, index, restaurant_link, restaurant_id, restaurant_name, max_reviews_count, save_data=True)
            index += 1
        
        condition = not (elements_count > 0)
        


