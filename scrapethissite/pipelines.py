# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
import pymongo
from scrapy.utils.project import get_project_settings

settings = get_project_settings()

class ScrapethissitePipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        population = int(adapter.get('population'))
        if population > 10**9:
            value = round(population / 10 ** 9, 2)
            adapter['population'] = str(value) + 'B'
        elif int(population) > 10**6:
            value = round(population / 10 ** 6, 2)
            adapter['population'] = str(value) + 'M'
        elif int(population) > 10**3:
            value = round(population / 10 ** 3, 2)
            adapter['population'] = str(value) + 'K'

        area = adapter.get('area')


        adapter['area'] = str(area) + ' Sq Km'

        return item

class save_to_mysql:
    def __init__(self) -> None:
        self.create_connection()
    
    def create_connection(self):
        self.connection = mysql.connector.connect(
            host = settings.get('MYSQL_HOST'),
            user = settings.get('MYSQL_USER'),
            password = settings.get('MYSQL_PASS'),
            database = settings.get('MYSQL_DB'),
            port = settings.get('MYSQL_PORT'),
        )
        self.curr = self.connection.cursor()
        self.create_table()
    
    def create_table(self):
        self.curr.execute(
            '''
                CREATE TABLE IF NOT EXISTS scrape_this_site(
                id INT AUTO_INCREMENT PRIMARY KEY,
                country_name VARCHAR(255),
                capital varchar(255),
                population VARCHAR(255),
                area VARCHAR(255)
                );
            '''
        )
    
    def process_item(self, item, spider):
        self.insert(item)
        return item
    
    def insert(self, item):
        self.curr.execute(
            '''
                INSERT INTO scrape_this_site 
                (country_name, capital, population, area) 
                values (%s, %s, %s, %s)
            ''',(
                    item['country_name'],
                    item['capital'],
                    item['population'],
                    item['area']
                ))
        self.connection.commit()
        # self.close()

    # def close(self):
    #     self.curr.close()
    #     self.connection.close()

class save_to_mongodb:
    def __init__(self) -> None:
        self.create_connection()
    
    def create_connection(self):
        self.myclient = pymongo.MongoClient(
            settings.get('MONGO_HOST'),
            settings.get('MONGO_PORT')
        )
        self.mydb = self.myclient[settings.get('MONGO_DB')]
        self.mycol = self.mydb[settings.get('MONGO_COLLECTION')]
        print('connection made')

    def process_item(self, item, spider):
        self.insert(item)
        return item

    def insert(self, item):
        self.mycol.insert_one({
                               "country_name" : item['country_name'],
                               "capital" : item['capital'],
                               "population" : item['population'],
                               "area" : item['area']})
        # yield item