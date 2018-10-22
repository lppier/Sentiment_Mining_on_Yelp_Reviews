import locale
from crawler.YelpCrawler import fetch_all_restaurants_reviews


locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

RESTAURANT_CATEGORIES = {
    # "chinese": {
    #     "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=chinese"
    # },
    # "japanese": {
    #     "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=japanese"
    # },
    # "indian": {
    #     "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=indian"
    # },
    "french": {
        "url": "https://www.yelp.com/search?find_desc=Restaurants&find_loc=London&start=0&sortby=review_count&cflt=french"
    },
}

REVIEW_LIMIT = 10


if __name__ == "__main__":
    for category in RESTAURANT_CATEGORIES:
        fetch_all_restaurants_reviews(category, RESTAURANT_CATEGORIES[category]["url"], REVIEW_LIMIT)
        