import scrapy
from scrapethissite.items import ScrapethissiteItem


class ScrapeThisSiteSpider(scrapy.Spider):
    name = "scrape_this_site"
    allowed_domains = ["www.scrapethissite.com"]
    start_urls = ["https://www.scrapethissite.com/pages/simple/"]

    def parse(self, response):
        countries = response.css('div.country')

        scrape_Item = ScrapethissiteItem()
        _id = 1
        for country in countries:
            
            # scrape_Item['_id'] = _id
            scrape_Item['country_name'] = country.css('.country-name ::text').getall()[1].strip()
            scrape_Item['capital'] = country.css('.country-capital ::text').get()
            scrape_Item['population'] = country.css('.country-population ::text').get()
            scrape_Item['area'] = country.css('.country-area ::text').get()
            # _id += 1
            yield scrape_Item