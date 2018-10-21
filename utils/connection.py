import io
import pycurl
from utils.logger import setup_logger
import stem.process
import random


logger = setup_logger("connection")

SOCKS_PORT = 7000

EXIT_NODES = [
    {"code": "{gb}", "country": "United Kingdom"},
    {"code": "{us}", "country": "United States"},
    {"code": "{sg}", "country": "Singapore"},
    {"code": "{my}", "country": "Malaysia"},
    {"code": "{id}", "country": "Indonesia"},
    {"code": "{kr}", "country": "South Korea"},
    {"code": "{au}", "country": "Australia"},
    {"code": "{at}", "country": "Austria"},
    {"code": "{de}", "country": "Germany"},
    {"code": "{fr}", "country": "France"},
    {"code": "{ca}", "country": "Canada"},
    {"code": "{br}", "country": "Brazil"},
    {"code": "{ar}", "country": "Argentina"},
    {"code": "{pl}", "country": "Poland"},
]


def print_bootstrap_lines(line):
  if "Bootstrapped " in line:
    logger.info(line)


def query(url):
  """
  Uses pycurl to fetch a site using the proxy on the SOCKS_PORT.
  """

  output = io.BytesIO()

  query = pycurl.Curl()
  query.setopt(pycurl.URL, url)
  query.setopt(pycurl.PROXY, 'localhost')
  query.setopt(pycurl.PROXYPORT, SOCKS_PORT)
  query.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS5_HOSTNAME)
  query.setopt(pycurl.WRITEFUNCTION, output.write)

  try:
    query.perform()
    return output.getvalue()
  except pycurl.error as exc:
    return "Unable to reach %s (%s)" % (url, exc)


def open_connection():
    exit_node = random.choice(EXIT_NODES)
    logger.info("Connect through TOR, exit node={}".format(exit_node["country"]))

    tor_process = stem.process.launch_tor_with_config(
    config = {
        'SocksPort': str(SOCKS_PORT),
        'ExitNodes': exit_node["code"],
    },
    init_msg_handler = print_bootstrap_lines,
    )
    return tor_process


def close_connection(process):
    process.kill()
