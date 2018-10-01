import requests
from lxml import html
from lxml import etree
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import operator
import time

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
    
def read_page_content(page_link,  page_number):
    content = None
    if page_number > 0:
        url = page_link % (page_number)
    else:
        url = page_link
    try:
        response = requests_retry_session().get(url, timeout=(3.05, 15))
        if response.status_code == 200:
            content = response.text
        else:
            print(response.status_code)
            print(url)
    except Exception as e:
        print('It failed :(', e.__class__.__name__)
        print(str(e))
    return content

def find_last_page_lxml(content):
    tree = html.fromstring(content)
    pages_div = tree.xpath('//div[@class = "pages"]')
    last_page = 0
    for page_href in  pages_div[0].xpath('.//a'):
        try:
            page_num = int(page_href.text_content())
            if page_num > last_page:
                last_page = page_num
        except:
            pass
    return last_page

start_page_link = 'http://iportal.rada.gov.ua/news/Rstr_nd/page/%d'
start_page=1

start_page_content = read_page_content(start_page_link, start_page)
last_page = find_last_page_lxml(start_page_content)

# Створюємо списки дат та посилань
hrefs = []  
dates = []

for page in range(1, last_page+1):
    page_content = read_page_content(start_page_link, page)
    pl_sessions_tree = html.fromstring(page_content)
    pl_sessions_div = pl_sessions_tree.xpath('//div[@class = "information_block archieve_block"]')[0]
    date = pl_sessions_div.xpath('.//span[@class = "date"]')
    href = pl_sessions_div.xpath('.//span[@class = "details"]/a')
    for _date in date:
        dates.append(_date.text_content())
    for _href in href:
        hrefs.append(_href.attrib['href'])

# Створюємо список посилань на списки у plain html (time consuming)
bad_urls = []
p0 = []
p1 = []
p2 = []
res_table = []

# Сортування: 0 - по н. депктатах; 1 - по фракціях
vid_pr = 0

for plsession in range(0, len(hrefs)):
    page_link = hrefs[plsession]
    if requests_retry_session().get(page_link).status_code == 200:
        temp_content = read_page_content(page_link, 0)
        temp_content_tree = html.fromstring(temp_content)
        plain_href = temp_content_tree.xpath('//div[@class = "vid_pr"]/a[re:match(text(), "друку$")]',
                                            namespaces={"re": "http://exslt.org/regular-expressions"})[vid_pr].get('href')
        visits = pd.read_html(plain_href)[-1]
        p1 = pd.DataFrame({'ПІБ': visits[0], 
                            dates[plsession]: visits[1]})
        p2 = pd.DataFrame({'ПІБ': visits[2], 
                            dates[plsession]: visits[3]})
        p0 = pd.concat([p1, p2], ignore_index=True).dropna()
        # У ВР присутні дві сутності "Тимошенко Ю.В.", перейменовуємо ту, що чоловічого роду
        p0.loc[operator.or_(operator.and_(p0['ПІБ'] == 'Тимошенко Ю.В.',  p0[dates[plsession]] == 'Присутній'), operator.and_(p0['ПІБ'] == 'Тимошенко Ю.В.',  p0[dates[plsession]] == 'Відсутній')), 'ПІБ'] = 'Тимошенко Ю.Вл.'
        if plsession == 0:
            res_table = p0.copy().set_index('ПІБ')
        else:
            p0 = p0.set_index('ПІБ')
            res_table = res_table.join(p0)
    else:
        bad_urls.append(page_link)
    
# Генеруємо префікс для вихідного файлу
timestr = time.strftime("%y%m%d-%H%M")

res_table.to_csv(timestr+'-res_table_lxml.csv')
if not bad_urls:
    print("Всі посилання на пленарні засідання було оброблено")
else:
    print("Наступні посилання не вдалось обробити:")
    for bad_url in bad_urls:
        print('\t', bad_url, '\n')
