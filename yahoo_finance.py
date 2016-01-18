from datetime import datetime, timezone
import requests
import csv

class YahooFinance(object):
  def __init__(self, symbol, start_period='1-3-1950', end_period=None):
    self.symbol = symbol
    self.start_period = start_period
    self.end_period = end_period
    self.response = self._form_url(symbol, start_period, end_period)

  def _form_url(self, symbol, start_period, end_period):
    start_date, end_date = self._extract_date_bounds(start_period, end_period)
    start_month = (start_date.month - 1)
    start_day = start_date.day
    start_year = start_date.year
    end_month = (end_date.month - 1)
    end_day = end_date.day
    end_year = end_date.year
    url = """http://real-chart.finance.yahoo.com/table.csv?s=%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=d&ignore=.csv""" %(symbol, start_month, start_day, start_year, end_month, end_day, end_year)
    return self._download_file(url, symbol)

  def _extract_date_bounds(self, start_date='1-3-1950', end_date=None):
    start_date = datetime.strptime(start_date, "%m-%d-%Y").date()
    if not end_date:
      end_date = datetime.utcnow().date()
    else:
      end_date = datetime.strptime(end_date, "%m-%d-%Y").date()
    return (start_date, end_date)

  def _download_file(self, url, symbol):
    local_filename = "-".join(("yh-hist", symbol, "csv"))
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename
