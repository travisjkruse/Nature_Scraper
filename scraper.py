import scrapy
import numpy as np
import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request
import re
from scrapy.selector import Selector
import pandas as pd
import csv

# --- Variables
# Most recent year
now = datetime.datetime.now()
recent_year = now.year
# Nature URL
base_nature_url = 'http://www.nature.com'
basic_nature_url = base_nature_url + '/nature/archive/?year='
#Settings
prefer_articles = False
time_delay = 0.75
past_year = 2009
#File names
file_name = 'nature_data_' + str(past_year) + '.csv'
issue_url_file = 'issue_urls_' + str(past_year) + '.csv'
article_url_file = 'article_urls_' + str(past_year) + '.csv'
metrics_url_file = 'metric_urls_' + str(past_year) + '.csv'

# Create a list of years to expand Nature URL
year_list = np.arange(recent_year, past_year-1, -1)
# Convert INT to STR
string_years = np.char.mod('%d', year_list)
# Create STR for URL
included_years = '-'.join(string_years)
nature_url = basic_nature_url + included_years
#nature_url = basic_nature_url + only_year

columns = ['article_title',
           'article_volume',
           'article_issue',
           'article_date',
           'article_citations',
           'article_attention',
           'article_tweets',
           'article_views']
data_array = pd.DataFrame(np.zeros((1, 8)), columns=columns)
data_array.to_csv(file_name, sep=",")

class NatureIssuesSpider(scrapy.Spider):
    name = 'nature_spider'
    download_delay = time_delay
    handle_httpstatus_list = [401]
    start_urls = [nature_url]

    def parse(self, response):
        issue_urls = []
        LINK_SELECTOR = './/li//li/a/@href'

        hxs = Selector(response)
        issue_urls.extend(hxs.xpath(LINK_SELECTOR).extract())

        with open(issue_url_file, 'w') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(issue_urls)

        for one_issue in issue_urls:
            if 'supp' not in one_issue:
                yield Request(base_nature_url + one_issue, callback=self.parse_issues)

    def parse_issues(self, response):

        if prefer_articles:
            article_selector = './/div[@id="af"]'
        else:
            article_selector = './/div[@id="lt"]'

        article_urls = []

        LINK_SELECTOR = article_selector + '//hgroup/h1/a/@href'

        article_urls.extend(response.xpath(LINK_SELECTOR).extract())

        with open(article_url_file, 'a') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(article_urls)

        for one_article in article_urls:
            yield Request(base_nature_url + one_article, callback=self.parse_articles)

    def parse_articles(self, response):
        metrics_selector = './/li[@class="article-metrics"]/a/@href'

        metrics = response.xpath(metrics_selector).extract()

        with open(metrics_url_file, 'a') as myfile:
            wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
            wr.writerow(metrics)

        yield Request(base_nature_url + metrics[0], callback=self.parse_metrics)

    def parse_metrics(self, response):
        title_selector = './/meta[@name="DC.title"]/@content'
        date_selector = './/meta[@name="DC.date"]/@content'
        volume_selector = './/meta[@name="prism.volume"]/@content'
        issue_selector = './/meta[@name="prism.issue"]/@content'
        citation_selector = './/div[@class="citation-count"]/text()'
        citation_pattern = re.compile(r'(\d+)')
        attention_selector = './/img[@alt="altmetric-donut"]/@src'
        attention_pattern = re.compile(r'(?<=score=)\d+')
        tweets_selector = './/div[@class="altmetric-twitter"]/b/text()'
        views_selector = './/span[@class="total"]/text()'

        article_title = response.xpath(title_selector).extract()[0]
        article_date = response.xpath(date_selector).extract()[0]
        article_volume = response.xpath(volume_selector).extract()[0]
        article_issue = response.xpath(issue_selector).extract()[0]
        article_tweets = response.xpath(tweets_selector).extract()
        article_views = response.xpath(views_selector).extract()

        if article_tweets:
            article_tweets = article_tweets[0]
        else:
            article_tweets = 0

        if article_views:
            article_views = article_views[0]
            article_views = int(article_views.replace(",", ""))
        else:
            article_views = 0

        article_title = re.sub(r'[^a-zA-Z0-9]', ' ', article_title)
        #article_title = article_title.replace(",","").encode('utf-8').decode()

        citation_data = []
        citation_extract = response.xpath(citation_selector).extract()
        for citation_number in citation_extract:
            one_citation_num = re.search(citation_pattern, citation_number)
            if one_citation_num:
                citation_data.extend(one_citation_num.group(0))
        if not citation_data:
            citation_data.extend('0')
        citation_max = max(citation_data)
        article_citations = citation_max

        attention_data = []
        attention_extract = response.xpath(attention_selector).extract()
        for attention_number in attention_extract:
            one_attention_num = re.search(attention_pattern, attention_number)
            if one_attention_num:
                attention_data.extend(one_attention_num.group(0))
        if not attention_data:
                attention_data.extend('0')
        attention_max = max(attention_data)
        article_attention = attention_max

        data_row = pd.DataFrame([[article_title,
                                  article_volume,
                                  article_issue,
                                  article_date,
                                  article_citations,
                                  article_attention,
                                  article_tweets,
                                  article_views]],
                                columns=columns)

        with open(file_name, 'a') as f:
            data_row.to_csv(f, header=False)

        pass

if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3"
    })
    process.crawl(NatureIssuesSpider)
    process.start()