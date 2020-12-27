import os
import scrapy
import requests
import json
from elasticsearch_dsl import Index, Search, Mapping, Keyword, Text, Document, Long, analyzer, tokenizer
from elasticsearch_dsl.connections import connections
from scrapy.crawler import CrawlerProcess
from urls import domain, crawl, extract_content, detect_language, create_description
from urllib.parse import urlparse
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from language import languages

from flask import Flask, request, jsonify
from flask import Flask
app = Flask(__name__)

# -----------------------------------------CONFIG------------------------------------------------

hosts = [os.getenv("HOST")]
http_auth = (os.getenv("USERNAME"), os.getenv("PASSWORD"))
port = os.getenv("PORT")

client = connections.create_connection(hosts=['localhost'])

# -----------------------------------------Index and mapping------------------------------------------------

en_analyzer = analyzer('my_analyzer',
    tokenizer=tokenizer('trigram', 'nGram', min_gram=3, max_gram=3),
    filter=['lowercase']
)

class Page(Document):
    title = Text(en_analyzer)
    domain = Text()
    url = Text()
    description = Text(en_analyzer)
    body = Text(en_analyzer)
    weight = Long()

    class Index:
        name = 'web-en'

    def save(self, ** kwargs):
        return super().save(** kwargs)

pages = Page._index.as_template('pages', order=0)
pages.save()
Page.init()

# -----------------------------------------Spiders------------------------------------------------

class PageSpider(scrapy.Spider):
    name = "page-spider"
    handle_httpstatus_list = [301, 302, 303] # redirection allowed

    es_client=None # elastic client
    redis_conn=None # redis client

    def parse(self, response):
        yield pipeline(response, self)

class SiteSpider(scrapy.spiders.CrawlSpider):
    name = "site-spider"

    handle_httpstatus_list = [301, 302, 303] # redirection allowed

    rules = (
        # Extract all inner domain links with state "follow"
        Rule(LinkExtractor(), callback='parse_items', follow=True, process_links='links_processor'),
    )

    def links_processor(self,links):
        """
        A hook into the links processing from an existing page, done in order to not follow "nofollow" links
        """
        ret_links = list()
        if links:
            for link in links:
                if not link.nofollow:
                    ret_links.append(link)
        return ret_links

    def parse_items(self, response):
        """
        Parse and analyze one url of website.
        """
        yield pipeline(response, self)


def pipeline(response, spider) :
    # skip rss or atom urls
    if not response.css("html").extract_first() :
        return
    # get domain
    domain_name = domain(response.url)

    # extract title
    title = response.css('title::text').extract_first()
    title = title.strip() if title else ""

    # extract description
    description = response.css("meta[name=description]::attr(content)").extract_first()
    description = description.strip() if description else "NAN"

    # get main language of page, and main content of page
    lang = detect_language(response.body)
    if lang not in languages :
        return ('Language not supported')
    body, boilerplate = extract_content(response.body, languages.get(lang))

    # weight of page
    weight = 3
    if not title and not description :
        weight = 0
    elif not title :
        weight = 1
    elif not description :
        weight = 2
    if body.count(" ") < boilerplate.count(" ") or not create_description(body) :
      # probably bad content quality
      weight -= 1

    newPage = Page(title=title, url=response.url, domain=domain_name, description=description, body=body, weight=weight)
    # every document has an id in meta
    newPage.meta.id = response.url
    # save the document into the cluster
    newPage.save()

# -----------------------------------------CONTROLLERS------------------------------------------------

@app.route('/')
def root():
    return 'ONLINE'

@app.route("/indexs", methods=['POST'])
def index():
    """
    URL : /index
    Index a new URL in search engine.
    Method : POST
    Form data :
        - url : the url to index [string, required]
    Return a success message.
    """
    # get POST data
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    if "url" not in data :
        return ('No url specified in POST data')

    # launch exploration job
    index_page(data["url"])

    return "Indexing started"

def index_page(link):

    try:
        link = crawl(link).url
    except:
        return 0

    process = CrawlerProcess({
      'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36",
          'DOWNLOAD_TIMEOUT':100,
          'DOWNLOAD_DELAY':0.25,
          'ROBOTSTXT_OBEY':True,
          'HTTPCACHE_ENABLED':False,
          'REDIRECT_ENABLED':False,
          'SPIDER_MIDDLEWARES' : {
              'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware':True,
              'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware':True,
              'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware':True,
              'scrapy.extensions.closespider.CloseSpider':True
          },
          'CLOSESPIDER_PAGECOUNT':500 #only for debug
    })

    process.crawl(PageSpider, start_urls = [link,], es_client=client)
    process.start()


@app.route("/indexf", methods=['POST'])
def index_full():

    # get POST data
    data = dict((key, request.form.get(key)) for key in request.form.keys())
    if "url" not in data :
        return ('No url specified in POST data')

    # launch exploration job
    index_site(data["url"])

    return "Full website index started"

def index_site(link):

    print("Index full website at : %s"%link)

    # get final url after possible redictions
    try :
        link = crawl(link).url
    except :
        return 0

    process = CrawlerProcess({
      'USER_AGENT': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36",
          'DOWNLOAD_TIMEOUT':100,
          'DOWNLOAD_DELAY':0.25,
          'ROBOTSTXT_OBEY':True,
          'HTTPCACHE_ENABLED':False,
          'REDIRECT_ENABLED':False,
          'SPIDER_MIDDLEWARES' : {
              'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware':True,
              'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware':True,
              'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware':True,
              'scrapy.extensions.closespider.CloseSpider':True
          },
          'CLOSESPIDER_PAGECOUNT':500 #only for debug
    })

    process.crawl(SiteSpider, allowed_domains=[urlparse(link).netloc], start_urls = [link,], es_client=client)
    process.start()


@app.route("/search", methods=['GET'])
def search():
    user_query = request.args.get("query")

    if user_query is None :
        return ('No query specified in POST data')

    req = Search(using=client, index='web-en', doc_type='page').sort({ "_score" : {"order" : "desc"}}).query('match', body=user_query)
    response = req.scan()

    res = []

    for hit in response:
        res.append({
            "url" : hit.url, 
            "descrption" : hit.body[0:500]
        })

    return jsonify(res)
    