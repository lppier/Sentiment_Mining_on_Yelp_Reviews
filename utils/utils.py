from datetime import datetime, timedelta
import dateutil.parser as parser
import locale

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

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
