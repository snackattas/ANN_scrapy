import scrapy
import datetime
from bs4 import BeautifulSoup
from unidecode import unidecode
from collections import namedtuple



class ANN_spider(scrapy.Spider):
    name = "ANN_spider"

    def start_requests(self):
        """Entrypoint for spider.  Takes in start_date and end_date command line args.
        Then yields the urls to parse"""

        start_date_argument = getattr(self, 'start_date', None)
        end_date_argument = getattr(self, 'end_date', None)
        if not start_date_argument or not end_date_argument:
            self.logger.error("Either arguments are missing or not using the right tags.\n"\
            "Ex of how to call the spider: \n"\
            "\tscrapy crawl ANN_spider -o anime.json -a start_date='1/1/2016' -a end_date='2/28/2016'\n\n"\
            "\tDates must be in form month/day/4-digit year\n"\
            "\tYear must be >=2012\n"\
            "\tstart_date and end_date must be <=60 days apart")
            return

        start_date_tuple = self.parse_date_argument(start_date_argument)
        end_date_tuple = self.parse_date_argument(end_date_argument)
        if not start_date_tuple or not end_date_tuple:
            return

        start_date = self.check_date(start_date_tuple.day,
            start_date_tuple.month, start_date_tuple.year)
        end_date = self.check_date(end_date_tuple.day,
            end_date_tuple.month, end_date_tuple.year)
        if not start_date or not end_date:
            return

        valid_range = self.check_date_range(start_date, end_date)
        if not valid_range:
            return

        base_url = "https://www.animenewsnetwork.com/herald/hp_more?d={}"
        for single_date in self.date_iterator(start_date, end_date):
            url = base_url.format(single_date)
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response):
        """Carries out parsing of url"""

        soup = BeautifulSoup(response.body, 'html.parser')
        content = soup.find(id="main")
        container_divs = content.find_all("div", class_="wrap")
        for div in container_divs:
            title = unidecode(div.find("a").text)
            topics_span = div.find('span', class_="topics").find_all("span")
            topics = [topic.text for topic in topics_span]
            time = div.find("time")['datetime']
            print time
            yield {
                'Title': title,
                'Tags': topics,
                "Time": time}
        self.log('Saved file')


    def date_iterator(self, start_date, end_date):
        """Iterates on dates passed in
        Params:
            start_date: date object
            end_date: date object
        Returns:
            A generator object that contains a string of the date in a specific format"""

        for n in range(int((end_date - start_date).days)+1):
            yield (start_date + datetime.timedelta(n)).strftime("%Y-%m-%d")


    def parse_date_argument(self, date_argument):
        """Parses date passed in from command line
        Params:
            date_argument: string with 2 /'s
        Returns:
            Three integers: day, month, year, in that order
        On error:
            Returns false"""

        if not isinstance(date_argument, str):
            self.logger.error("Argument %s is not a string.  Please use double quotes" %s (date_argument))
            return False

        parsed_date = date_argument.split("/")
        if not len(parsed_date) == 3:
            self.logger.error("Did not find 2 /'s in %s" % (date_argument))
            return False

        parsed_successfully = True
        for piece in parsed_date:
            if not piece:
                self.logger.error("Found null piece in date %s" % (date_argument))
                parsed_successfully = False
        if not parsed_successfully:
            return False

        Date = namedtuple('Date', ['day', 'month', 'year'])
        date = Date(int(parsed_date[1]), int(parsed_date[0]), int(parsed_date[2]))
        return date


    def check_date(self, day, month, year):
        """Makes sure date passed in is an actual valid calendar date, and that the year passed in is >=2012
        Params:
            day: integer
            month: integer
            year: integer
        Returns:
            Python date object
        On error:
            Returns false"""

        try:
            date = datetime.date(year=year, month=month, day=day)
        except:
            self.logger.error("Date %s/%s/%s is invalid" % (day, month, year))
            return False

        todays_year = datetime.datetime.now().year
        if not 2012 <= year <= todays_year:
            self.logger.error("Year %s is not between 2012 and %s" % (year, todays_year))
            return False
        return date


    def check_date_range(self, start_date, end_date):
        """Makes sure date range is not negative and is <=60 days
        Params:
            start_date: python date object
            end_date: python date object
        Returns:
            True
        On error:
            False"""

        date_range = (end_date - start_date).days
        if date_range < 0:
            self.logger.error("Start date must be first command line argument and End date second")
        if not 0 <= date_range <= 60:
            self.logger.error("Date range must not be more than 60 days")
            return False
        return True
