'''

Author : Vipul Patel
Description : Create extraction process and save data in excel format

'''

import json
import pandas as pd
import requests
import scrapy
from scrapy.http import HtmlResponse


class DataExtractor(scrapy.Spider):
    name = 'cdw'
    currentPage = 1
    dataStore = list()

    def __init__(self, name=None, category='', subcategory='', **kwargs):
        super().__init__(name, **kwargs)
        self.category = category
        self.subcategory = subcategory.replace('_', ' ')

    def start_requests(self):
        link = 'https://www.cdw.com/'
        self.header = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0",
            "cookie": 'A8A8F83D13EA4F8B917AA5F211762060=8FB96EDC0C9F416091C974BC59CC8BA9; BA9AA5C91598458BA251A10B273627B6=963C05964868404692721ED16FA44193; dtCookie=v_4_srv_15_sn_0F129A947543C73E03B2474754AD94A2_perc_100000_ol_0_mul_1_app-3A9daca7aae537f807_1_rcs-3Acss_0; rxVisitor=16537395930795O0CPKLJ7LK3FHDHSKGH9G0RRMTGM9H0; newVisitor=true; optimizelyEndUserId=oeu1653739593938r0.5597821963846366; _fbp=fb.1.1653739598526.2106446063; _gcl_au=1.1.980506266.1653739600; _li_dcdm_c=.cdw.com; _lc2_fpi=464f01cb1133--01g458ze7bm3nt40x9y7ed17tz; 265B10B0AD854F37A02410D8F39E9723=FBFA7EFB-73F6-4231-9E8C-9BF8DD4D67A7; 70549A6B29AD46CF90DA19BC17972419=V-fNqsx7CEdmXXIk2MV-3Z92FzhIfJnVmXIMsUOJ4uKJE8k4AdvA7tF6iPerbn5U5GWO_4L2uhTEyXoozPnpyYXQLt81; _rdt_uuid=1653739603505.7ae351a2-85e8-418f-8ff4-d9943deac90b; AMCVS_6B61EE6A54FA17010A4C98A7%40AdobeOrg=1; s_cc=true; _hjSessionUser_68143=eyJpZCI6IjVlMmJkMTllLTlkOWUtNTA2ZC05NWM3LTU4N2Q4NGUzYWQ2MiIsImNyZWF0ZWQiOjE2NTM3Mzk2MjgyMjIsImV4aXN0aW5nIjp0cnVlfQ==; B7D99A893A2F4D6EB8C66F2801F35BF6=True; _hjSessionUser_540806=eyJpZCI6IjYxZjlhOWE0LWE0ZTAtNTJiMC1hZWFhLWRjYTFlZmU0ZjM1OCIsImNyZWF0ZWQiOjE2NTM3Mzk2MTI5OTcsImV4aXN0aW5nIjp0cnVlfQ==; _abck=B005B2EAD4683CEA3A4B87C8704896AA~0~YAAQ9IfTF0qTe/CAAQAAdqWEEwflHPdtBywD6/YvR8pGwaDE2/F9wdBpSJdp4CIedrU0TBTOmX/iMW2UGHDq3OvL/yfSwUFci7K/pbrqJ4Wbo3vsC0OArgDUOW4I2aJPVILwe4lObcyGoLe9/jrPfT1QFTuLPdnh2soBhFVwhMmnM5zvqlfz4+oZiSExCcY8413Zwc54dcsvYV1XNC8ELNzVyt3KYPklVdfp4/kNYhsmO3mAvyeLUT5JIZxw/89S0HoFsvV44Qg3iSC3GMzPSl5kcU5aaKujvrCRqT0vylIW4mDSkvzB7uvfvgHXq3g9d/ZL9lzNf/AOIEn4Gdrdsgie6cyxF3F2NqUC6YIce1xH7oiVZc9+dUVjSD+Aw5yOERYzBW5c+GOFO6azS/ZKYCa8C7rt~-1~-1~-1; bm_sz=54989B3FA4A93C825DE699B99ABB53E9~YAAQ9IfTF02Te/CAAQAAdqWEEw+8R35DY9cp8sOJ9La7SPKSi5fC3SJRR5yOzsi/WX1I9PcvAeb9+2aarIJq3DCBWvUJxgaKWUQ6zvSpNGX7xcWBqcOzobq9njpawkhtKUnMcxO1lvAnMaju8fwDQX6lXz2624sKdl1vKU92y4PwD8We/JRNmFaa3HjC9VD6QZKA03Gn3SegPa4H55os0LgIH6Qh8zb6avw57ueZTlnRLapi4dqXGQnUQabPecxvXKdCcnnGKdNVrdfajebRTmAS0W8v4EAr1RhOEVA/CwI=~4470837~3553092; AMCV_6B61EE6A54FA17010A4C98A7%40AdobeOrg=1585540135%7CMCMID%7C41136128999932284400242078953892179488%7CMCAAMLH-1654494672%7C12%7CMCAAMB-1654494672%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1653897072s%7CNONE%7CvVersion%7C4.4.0; bm_mi=A93F84F8CBBC55A7A9628BBF0150F5C3~YAAQ9IfTF3KTe/CAAQAAC7KEEw/wVZhYAMXtjmmFY3CYutEdlqkyNJxv6ysm3L9wcvo/c57oVWv6ns0ZKCJH52OJbMK7ZiusIgq2CEIFxENhEQIQlHlXS4Q/kK5G5d7+biA2tekfR4AivTqpqonCy0RB6QSMdJ5Towa3GVPAmkMsijXLg5M1w+MQZxKTvp++PQC/V4mTS3AOuStL9Cn5lHZt+7odwpP71T+Al1ZiACOa7xVetRovHqBX0WKKGEmupw844hdZCTuz83Iu1EIJP3A0jxz8P0Q2ZSBcFYPIMvldoIrUfeuXccxCJNP4pko=~1; ak_bmsc=B70CBD2819C583D3D7D6E3A1748B25E6~000000000000000000000000000000~YAAQ9IfTF3uTe/CAAQAANbOEEw+VC3GU060VNMynPjtVbSr1xC18DXEYeIRR0KDOUSeXvFVXCAYo70G0bLRY7XSSvJM+jLH5QT/oT/uuGE3XBXPMbJz0rRB8fw4660yiVJ/rhA71Anb9LJb6/5nwn5bVPV75LngUwHWrIDE8VNnVAezvw0Cpgtq85mkvz9Y7jH3fnQMXY2e7C1MuOC6Kn6CyQLNq1TppyQaUjufF92ir1DIRG8kZJmrF9hhh+UK/kOQW8TLKgS7jdZiKajp+yFxvjcwonNqOtrDsMlTktUbUauA0/OPqj7V+0/QGKiKw5mVVuOVeM+Pg0bBF9YNhh4/GAU0OwBhDyGf1x27v9Cg4g2ufhMCaa4zx34pE0HAQqR79dwg9b0sZPxMFXGArN5R5aOqOiS2Kx4Fxrx0m50PQpSR+wKaZjIH+ObKvckQ1W/KdrS1Pm3FCTsr6QhE3EmFfMVVnkw5qaXLDKYwiasnsBxZsRZR+Cch0Ig0O5UxRX/4=; rr_rcs=eF5jYSlN9jBJSzY3szRL1k00TEzVNUkxTdY1M0pO0jUwMDMwMUo0tDAwNuXKLSvJTOEzMjDVNdQ1BACPGg3r; dtSa=-; bluecoreNV=false; SC_LINKS=%5B%5BB%5D%5D; s_sq=%5B%5BB%5D%5D; s_ppvl=CDW%2520Homepage%2C57%2C57%2C714%2C1536%2C354%2C1536%2C864%2C1.25%2CL; dtLatC=686; s_ppv=CDW%2520Homepage%2C9%2C57%2C714%2C1536%2C354%2C1536%2C864%2C1.25%2CL; dtPC=15$291316408_786h-vAMMQUUQJLLMUJCFKMFRMAIHKWDTFILMK-0e0; rxvt=1653896867857|1653895067857; AKA_A2=A; s_dfa=cdwglobalstaging; s_visit=1; s_dl=1; s_dl1=1; s_dl2=1; s_dl3=1; gpv_pn=CDW%20Homepage; _uetsid=8354f7e0dfdc11ec8dd14fb2cbdc4d64; _uetvid=9fbbaf70de7e11ecb54e2308d7373ceb; CartKey=6a56525343ca407d9eb734f935715acd; mp_cdw_mixpanel=%7B%22distinct_id%22%3A%20%221810a8fa8dbe4-0a6e07fab6ae61-14333270-144000-1810a8fa8dc1a5%22%2C%22bc_persist_updated%22%3A%201653739595999%7D; bc_invalidateUrlCache_targeting=1653895482918; _hjIncludedInSessionSample=1; _hjSession_68143=eyJpZCI6ImI5MDg4YWRjLTQxYzMtNDRjYS04MjcxLTgyNWRjZWQ3ZTE2YyIsImNyZWF0ZWQiOjE2NTM4OTU0ODM5MzIsImluU2FtcGxlIjp0cnVlfQ==; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=0; utag_main=v_id:01810a8fa59700a674975e312c2003073001406b00978$_sn:4$_se:1$_ss:1$_st:1653897258549$vapi_domain:cdw.com$dc_visit:4$ses_id:1653895458549%3Bexp-session$_pn:1%3Bexp-session$dcsyncran:1%3Bexp-session$dc_event:1%3Bexp-session$dc_region:ap-east-1%3Bexp-session; needleopt=Saant0-usOnly; needlepin=N190d1653739603993000140081649313b81649313b00000000000000000000000000000000; s_ptc=0.49%5E%5E0.68%5E%5E0.00%5E%5E0.27%5E%5E0.38%5E%5E0.36%5E%5E34.65%5E%5E0.12%5E%5E38.06; bm_sv=186ECED1B83F86B149FF0714DB0D2205~YAAQ9IfTFzfoe/CAAQAAt33aEw9CWXxfkmvOD5S12CE8RCBFR9kPyY2R3A1OxhQk7I+y/g2/9U578/CAlV/tXLiescMRMr+ApZ7vV+NOzfUvt+hN2oo3f4T3yFcdtcn7w4uHkFhE+ZH2AMyF+RNY91W/5v2NRW1j+/onTgoNidiLSHLOyFHJIpRS1EhY5/LpiQxmhiFDNeAmIAurZNmVUMNYUTjzO2X+eI/qgTeblOuzcFwlljf6aczRWNmolQ==~1; RT="z=1&dm=cdw.com&si=f287cf9b-c330-42e9-a5c6-edc2b7f1ede8&ss=l3sendyy&sl=0&tt=0&bcn=%2F%2F684d0d43.akstat.io%2F&ld=2ek6o&ul=1uxh"',
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36"}
        response = requests.get(url=link, headers=self.header)
        response = HtmlResponse(url=link, body=response.content)

        Link = response.xpath(f'//li//a[contains(text(),"{self.category}")]//@href').get(default='')
        if Link != '':
            Link = 'https://www.cdw.com' + Link
        yield scrapy.FormRequest(url=Link, callback=self.parse, headers=self.header)

    def parse(self, response):
        Link = response.xpath(f'//a[contains(text(),"{self.subcategory}")]//@href').get(default='')
        if Link != '':
            Link = 'https://www.cdw.com' + Link
        yield scrapy.FormRequest(url=Link, callback=self.productList, headers=self.header, meta={'productPage': Link})

    def productList(self, response):
        productPage = response.meta['productPage']

        if self.currentPage != 0:
            self.currentPage += 1
            getLinks = response.xpath('//div[@class="search-results"]/div//h2//a//@href').getall()
            if getLinks != []:
                for getLink in getLinks:
                    getLink = 'https://www.cdw.com' + getLink
                    yield scrapy.FormRequest(url=getLink, callback=self.product, headers=self.header, dont_filter=True)
                Link = f'{productPage}&pcurrent={self.currentPage}'
                yield scrapy.FormRequest(url=Link, callback=self.productList, headers=self.header,
                                         meta={'productPage': productPage}, dont_filter=True)

    def product(self, response):
        item = dict()
        item['categories'] = '/'.join(response.xpath('//nav[@class="product-breadcrumb"]//li//span//text()').getall())
        item['name'] = response.xpath('//h1//text()').get(default='')
        item['sku'] = response.xpath('//span[@itemprop="mpn"]//text()').get(default='')
        item['cdw_part_number'] = response.xpath('//span[@itemprop="sku"]//text()').get(default='')
        item['description'] = ''.join(response.xpath('//div[@class="product-overview-container"]//text()').getall())
        item['short_description'] = ''.join(response.xpath('//div[@class="quick-tech-spec"]//ul').getall())
        item['price'] = response.xpath('//span[@class="price"]//span//text()').get(default='')
        images = response.xpath('//li[@class="slide-thumb"]//@data-imageurl').getall()
        if images == []:
            images = response.xpath('//div[@class="main-image"]//img//@src').getall()
        if images != []:
            image_count = 1
            for image in images:
                if 'http' not in image:
                    item[f'image_{image_count}'] = f'https{image}'
                else:
                    item[f'image_{image_count}'] = image
                image_count += 1
        tech_specs = f"https://www.cdw.com/api/product/1/data/technicalspecifications/{item['cdw_part_number']}"
        yield scrapy.FormRequest(url=tech_specs, callback=self.getSpec, headers=self.header, dont_filter=True,
                                 meta={'item': item})

    def getSpec(self, response):
        data = json.loads(response.text)
        item = response.meta['item']
        item['tech_specs'] = data
        self.dataStore.append(item)

    def close(spider, reason):
        frame = pd.DataFrame(DataExtractor.dataStore)
        frame.to_excel('temp.xlsx', index=False)

# execute('scrapy runspider DataExtractor.py'.split())
