import scrapy
import utils
import json
import re
import pandas as pd
import numpy as np
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from pprint import pprint
import scrapy.utils.misc
import scrapy.core.scraper
from datetime import datetime
from urllib.parse import urljoin








class companyInfo(scrapy.Spider):
    name = "spider_name"
    emails = []
    tickers = set()
    yielded_items = []



    custom_settings = {
        'DOWNLOAD_DELAY' : 3
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': 'GUC=AQEBCAFmGgRmQ0IbnwQY&s=AQAAAKLDlKNv&g=Zhi03A; A1=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; A3=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; A1S=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; cmp=t=1712895202&j=0&u=1---; gpp=DBAA; gpp_sid=-1; axids=gam=y-4xxv6wNE2uLV3R3wnwCSDr9yhGa8PG9Q~A&dv360=eS1HTFptMUZORTJ1RV8ycnJrNDlzZXhhWjlZZ3FMTHBPZ35B&ydsp=y-bPseNsZE2uJGSLskQjy_5hxlMrExlFYR~A&tbla=y-Y1emzVNE2uJjqx6na35SunYrZytb56mL~A; tbla_id=176b75d2-0183-4331-acbd-a702316ba2be-tuctbe8ec2b; PRF=t%3DBSEM%26newChartbetateaser%3D0%252C1714104814126; __gpi=UID=00000d5bb92b24a5:T=1712895285:RT=1712897653:S=ALNI_MZ_TjFlBfVZk4xZ9BQU9bXNMyPk5Q; __eoi=ID=8820ac10553d1ee5:T=1712895285:RT=1712897653:S=AA-AfjZbX7sClQBSFtcQwVCbK9eH',
        'origin': 'https://finance.yahoo.com',
        'referer': 'https://finance.yahoo.com/news/skye-bioscience-uplists-nasdaq-global-120000819.html?.tsrc=fin-srch',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }
    headers_1 = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': 'GUC=AQEBCAFmGgRmQ0IbnwQY&s=AQAAAKLDlKNv&g=Zhi03A; A1=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; A3=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; A1S=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; cmp=t=1712895202&j=0&u=1---; gpp=DBAA; gpp_sid=-1; YHB__li_dcdm_c=.com; YHB__lc2_fpi=5fb552a76ef3--01hv8854s04j1gna0gwrs3ygh9; YHB__lc2_fpi_meta=%7B%22w%22%3A1712895202081%7D; axids=gam=y-4xxv6wNE2uLV3R3wnwCSDr9yhGa8PG9Q~A&dv360=eS1HTFptMUZORTJ1RV8ycnJrNDlzZXhhWjlZZ3FMTHBPZ35B&ydsp=y-bPseNsZE2uJGSLskQjy_5hxlMrExlFYR~A&tbla=y-Y1emzVNE2uJjqx6na35SunYrZytb56mL~A; tbla_id=176b75d2-0183-4331-acbd-a702316ba2be-tuctbe8ec2b; PRF=t%3DBSEM%26newChartbetateaser%3D0%252C1714104814126; __gpi=UID=00000d5bc1ce02cf:T=1712911663:RT=1712911663:S=ALNI_MarV_ikUQaqwOkedDMc6BwNpIzjCQ; __eoi=ID=78bdf55e4994fe23:T=1712911663:RT=1712911663:S=AA-AfjYxIkd9mn8MuoxC2ICsfZHe',
        'referer': 'https://finance.yahoo.com/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }
    headers_2 = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': 'GUC=AQEBCAFmGgRmQ0IbnwQY&s=AQAAAKLDlKNv&g=Zhi03A; A1=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; A3=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; A1S=d=AQABBLDg4WQCEPYWQAzj-Bns2i81QZFCTkIFEgEBCAEEGmZDZpKab2UB_eMBAAcIsODhZJFCTkI&S=AQAAAjorJjq9Pj14udVPof0VkfQ; cmp=t=1712895202&j=0&u=1---; gpp=DBAA; gpp_sid=-1; axids=gam=y-4xxv6wNE2uLV3R3wnwCSDr9yhGa8PG9Q~A&dv360=eS1HTFptMUZORTJ1RV8ycnJrNDlzZXhhWjlZZ3FMTHBPZ35B&ydsp=y-bPseNsZE2uJGSLskQjy_5hxlMrExlFYR~A&tbla=y-Y1emzVNE2uJjqx6na35SunYrZytb56mL~A; tbla_id=176b75d2-0183-4331-acbd-a702316ba2be-tuctbe8ec2b; PRF=t%3DBSEM%26newChartbetateaser%3D0%252C1714104814126; __gpi=UID=00000d5bb92b24a5:T=1712895285:RT=1712897653:S=ALNI_MZ_TjFlBfVZk4xZ9BQU9bXNMyPk5Q; __eoi=ID=8820ac10553d1ee5:T=1712895285:RT=1712897653:S=AA-AfjZbX7sClQBSFtcQwVCbK9eH',
        'origin': 'https://finance.yahoo.com',
        'referer': 'https://finance.yahoo.com/news/skye-bioscience-uplists-nasdaq-global-120000819.html?.tsrc=fin-srch',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }


    def __init__(self, df):
        self.df = df
        
    def start_requests(self):

        suffixes_to_remove = ["Co", "Company", "Inc", "Incorporated", "Ltd", "Corp", "Ltd.", "Inc.", "Corp.", "LP", "L.P.", "LLC", ",","."]
        # Construct a regular expression pattern to match any of the suffixes surrounded by word boundaries
        pattern = r'\b(?:' + '|'.join(suffixes_to_remove) + r')\b'
        pattern = re.compile(pattern, re.IGNORECASE)

        for index,row in self.df.iterrows():

            print(f"=>Processing row: {index}")

            if "(" in row['company_name']:  
                company, ticker = row['company_name'].split('(')
                ticker = ticker.strip()
                ticker = ticker.rstrip(')')
            else:
                company = row['company_name']
                ticker = ""
                
            cleaned_company_name = company.replace('\xa0','')
            # Use re.sub() to replace matched suffixes with an empty string
            company_name_wo_sff = re.sub(pattern, ' ', cleaned_company_name.replace(',','').replace('.','').replace("'",'')) 

            yield scrapy.Request(
                url=f"https://query1.finance.yahoo.com/v1/finance/search?q={company}&lang=en-US&region=US&quotesCount=6&newsCount=2&listsCount=2&enableFuzzyQuery=true&quotesQueryId=tss_match_phrase_query&multiQuoteQueryId=multi_quote_single_token_query&newsQueryId=news_cie_vespa&enableCb=true&enableNavLinks=true&enableEnhancedTrivialQuery=true&enableResearchReports=true&enableCulturalAssets=true&enableLogoUrl=true&researchReportsCount=2",
                callback=self.parse_searchresults,
                meta={'ticker': ticker,"full_company_name":cleaned_company_name.strip(),"company_name":company_name_wo_sff.strip(),'found':False},
                method="GET",
                headers=self.headers,
            )

    def parse_searchresults(self,response):

        parsed_data = json.loads(response.text)
        pprint(type(parsed_data["quotes"]))
    
        
        

        if (response.meta["ticker"] == "PRIVATE"):
            for i in parsed_data['news']:
                if response.meta["company_name"].lower() in i["title"].lower():
                    response.meta["found"] = True
                    yield scrapy.Request(
                        url=i["link"],
                        method="GET",
                        headers=self.headers_1,
                        callback=self.parse_news,
                        meta={
                            'ticker': response.meta["ticker"],
                            "full_company_name":response.meta["full_company_name"],
                            "company_name":response.meta["company_name"],
                            "url": i["link"]
                            }
                    )

        else:
            if (response.meta["ticker"] == "") or (pd.notnull(response.meta["ticker"]) == False):
                print(f"in{True}")
                for i in parsed_data["quotes"]:
                    if "longname" in i:
                        print([response.meta["company_name"]])
                        if (response.meta["full_company_name"].lower() in i["longname"].lower()) or (response.meta["company_name"].lower() in i["longname"].lower()):
                            print("doublein")
                            response.meta["ticker"] = i['symbol']
                            print(response.meta["ticker"])
                            break
            
            news_count = 0
            if (len(parsed_data['news']) == 0) and  ((pd.notnull(response.meta["ticker"]) != False) and (response.meta["ticker"]!="PRIVATE")):
                response.meta["found"] = True
                yield scrapy.Request(
                        url=f"https://finance.yahoo.com/quote/{response.meta['ticker']}/profile",
                        headers=self.headers_2,
                        method="GET",
                        callback=self.parse_profiles,
                        meta={
                            'ticker': response.meta["ticker"],
                            "news":False,
                            "full_company_name":response.meta["full_company_name"],
                            "company_name":response.meta["company_name"],
                            'url': f"https://finance.yahoo.com/quote/{response.meta['ticker']}/profile"
                            }
                    )   
            for i in parsed_data['news']:
                news_count +=1
                print(response.meta["ticker"])
                if (("relatedTickers" not in i.keys()) and (response.meta["ticker"] not in self.tickers)) or ((response.meta["ticker"] not in self.tickers) and news_count == len(parsed_data['news'])):
                    response.meta["found"] = True
                    yield scrapy.Request(
                        url=f"https://finance.yahoo.com/quote/{response.meta['ticker']}/profile",
                        headers=self.headers_2,
                        method="GET",
                        callback=self.parse_profiles,
                        meta={
                            'ticker': response.meta["ticker"],
                            "news":False,
                            "full_company_name":response.meta["full_company_name"],
                            "company_name":response.meta["company_name"],
                            'url': f"https://finance.yahoo.com/quote/{response.meta['ticker']}/profile"
                            }
                    )
                    break
                elif "relatedTickers" in i.keys():
                    print("in2",response.meta["ticker"])
                    if response.meta["ticker"] in i["relatedTickers"]:
                        response.meta["found"] = True
                        self.tickers.add(response.meta["ticker"])
                        yield scrapy.Request(
                            url=i["link"],
                            method="GET",
                            headers=self.headers_1,
                            callback=self.parse_news,
                            meta={
                            'ticker': response.meta["ticker"],
                            "full_company_name":response.meta["full_company_name"],
                            "company_name":response.meta["company_name"],
                            "url": i["link"]
                            }
                        )
        if response.meta["found"] == False:
            self.yielded_items.append({
                    "company_full_name": response.meta["full_company_name"],
                    "company_name": response.meta["company_name"],
                    "ticker": "",
                    "email": "" ,
                    "email_2":"",
                    "context":  "",
                    "context_2": "",
                    "company_address": "",
                    "company_phone": "",
                    "company_website": "",
                    "CFO_full_name": "",
                    "CFO_first_name": "",
                    'URLs':[]
                    

                    })

    
    def parse_news(self,response):

        
        webpage_text = response.text
        selector = Selector(response)

        # Define a regular expression pattern to match email addresses
        email_pattern = r'[\w\.-]+@[\w\.-]+'

        # Find all matches of the email pattern in the webpage text
        email_matches = re.findall(email_pattern, webpage_text)
        email_matches = list(set(email_matches))
        print(email_matches)
        for i in email_matches:
            if i not in ["n@keyframes","n@media"]:
                parent_element_xpath = f'//a[contains(text(), "{i}")]/parent::*'
                # Extract parent element

                content = selector.xpath(parent_element_xpath + '//text()').getall()
                if i in content:
                    content.remove(i)
                if len(content) == 0:
                    content = selector.xpath(parent_element_xpath + '//parent::*//text()').getall()


                # Extract text content from parent element
                if i in self.emails:
                    for row in self.yielded_items:
                        if row["ticker"] == response.meta['ticker'] and row["email"] == i :
                            row["context_2"]= " ".join(content)

                else:
                    self.emails.append(i)
                    self.yielded_items.append({
                    "company_full_name": response.meta["full_company_name"],
                    "company_name": response.meta["company_name"],
                    "ticker": response.meta['ticker'],
                    "email": i ,
                    "context":  " ".join(content),
                    "context_2": "",
                    "URLs":[response.meta['url']]


                    })
        if (response.meta["ticker"] != "PRIVATE"):
            yield scrapy.Request(
                url=f"https://finance.yahoo.com/quote/{response.meta['ticker']}/profile",
                headers=self.headers_2,
                method="GET",
                callback=self.parse_profiles,
                meta={
                    'ticker': response.meta["ticker"],
                    "news":True,
                    "full_company_name":response.meta["full_company_name"],
                    "company_name":response.meta["company_name"],
                    "url": response.meta['url']  
                    }
                                       
            )
        else:
            self.yielded_items.append({
                    "company_address":"",
                    "company_phone": "",
                    "company_website": "",
                    "CFO_full_name": "",
                    "CFO_first_name": "",
                    'URLs' : []
                    

                    })

        
    def parse_profiles(self,response):

        selector = Selector(response)
        address = selector.xpath("//div[@data-test='qsp-profile']//p[1]/text()[not(ancestor::a[@href[contains(., 'tel:') or contains(., 'http')]])]").getall()
        phone = selector.xpath("//a[contains(@href,'tel:')]/text()").get()
        website = selector.xpath("//div[@data-test='qsp-profile']//a[contains(@href,'https:')]/text()").get()
        CFO_name = selector.xpath("//tr[contains(.,'Chief Financial Officer') or contains(.,'CFO')or contains(.,'Financial Officer')]//td[1]//text()").get()
        pattern = r'(Mr\.|Mrs\.|Ms\.|Miss\.|Dr\.|Prof\.|Sir|Madam)?\s*([A-Za-z]+)\s*(?:([A-Za-z]+(?: [A-Za-z]+)*)\s+([A-Za-z]+(?: [A-Za-z]+)*))?\s*(?:(I{1,3}|IV|V?I{1,3})?)'

        if CFO_name != None:
            # Use the pattern to search for matches in the full name
            match = re.match(pattern, CFO_name)

            if match:
            # Extract the matched groups
                title = match.group(1)
                first_name = match.group(2)
                middle_name = match.group(3) if match.group(3) else ""
                last_name = match.group(4) if match.group(4) else ""
                suffix = match.group(5) if match.group(5) else ""
            else:
                first_name = ""
        else:
            first_name = ""
        if len(address) == 0:
            address = selector.xpath("//div[contains(@class,'address')]//text()").getall()
        if website == None:
            website = selector.xpath("//a[@aria-label='website link']//text()").get()
        
        print([response.meta['company_name'],address,phone,website,CFO_name])
        if response.meta["news"] == True:
            for row in self.yielded_items:
                print(row)
                if row["ticker"] == response.meta['ticker']:
                    row["company_address"] = " ".join(address)
                    row["company_phone"] = phone
                    row["company_website"] = website
                    row["CFO_full_name"] = CFO_name
                    row["CFO_first_name"] = first_name
                    if 'URLs' in row.keys():
                        row['URLs'].append(response.meta['url'])
                    else:
                       row['URLs'] = [] 
        else:
            self.yielded_items.append({
                    "company_full_name": response.meta["full_company_name"],
                    "company_name": response.meta["company_name"],
                    "ticker": response.meta['ticker'],
                    "email": "" ,
                    "context":  "",
                    "context_2": "",
                    "email_2":"",
                    "company_address":" ".join(address),
                    "company_phone": phone,
                    "company_website": website,
                    "CFO_full_name":CFO_name,
                    "CFO_first_name": first_name,
                    "URLs": []
                    

                    })
        if website:
            yield scrapy.Request(
                url=website,
                method="GEt",
                headers=self.headers_2,
                callback=self.parse_company_site,
                meta={
                    "website" :website,
                    "ticker":response.meta['ticker']
                }
            )

    def parse_company_site(self,response):
        
        selector = Selector(response)
        investors_link = selector.xpath("//a[contains(.,'Investor')]/@href").get()
        print(investors_link)
        if investors_link == '#' or '':
            investors_link = selector.xpath("//a[contains(@href,'investor')]/@href").get()
        try:
            yield scrapy.Request(
                    url=investors_link,
                    method="GET",
                    headers=self.headers_2,
                    callback=self.parse_investors_email,
                    meta={
                        "ticker":response.meta['ticker'],
                        'url' : investors_link
                    }
                )
        except:
            try:
                base_url = "https://example.com"
                relative_url = "/investor/overview"
                full_url = urljoin(base_url, relative_url)
                yield scrapy.Request(
                        url=full_url,
                        method="GET",
                        headers=self.headers_2,
                        callback=self.parse_investors_email,
                        meta={
                            "ticker":response.meta['ticker'],
                            'url' : investors_link
                        }
                    )
            except:
                for row in self.yielded_items:
                    if row["ticker"] == response.meta['ticker']:
                        row["email_2"] = ''

    
    def parse_investors_email(self,response):

        webpage_text = response.text
        selector = Selector(response)
        # selector.r
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        email_matches = re.findall(email_pattern, webpage_text)
        email_matches = list(set(email_matches))
        print(email_matches)
        for row in self.yielded_items:
            if row["ticker"] == response.meta['ticker']:
                if len(email_matches) >0:
                    row["email_2"] = email_matches[0]
                    row["URLs"].append(response.meta['url'])


        
            
    
            
    def closed(self, reason):
        # Convert the class-level list to a DataFrame
        df = pd.DataFrame(self.yielded_items)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Write the DataFrame to a CSV file
        df.to_csv(f"{timestamp}_results.csv", index=False)


file = str(input("Enter file path to scrape: "))
try:
    df = utils.read_csv(r'{}'.format(file))
except Exception as e:
    print(f"[INFO] {e}")

print(df)


df.columns = df.columns.str.replace('[^a-zA-Z0-9]', '')

def warn_on_generator_with_return_value_stub(spider, callable):
        pass
scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
process = CrawlerProcess()
process.crawl(companyInfo, df=df)
process.start()
process.join()
print("finished")
# while True:
#     pass
