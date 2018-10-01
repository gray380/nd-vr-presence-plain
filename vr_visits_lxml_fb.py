import requests
from lxml import html
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import operator
import time
import os

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
    
def read_file(filename):
    with open(filename) as input_file:
        text = input_file.read()
    return text
    
path = os.getcwd()
temp_data = path + "/data"

try:  
    os.mkdir(temp_data)
except OSError:  
    print ("Creation of the directory %s failed" % temp_data)
    
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

# Створюємо файловий буфер зі сторінок
bad_urls = []

# Сортування: 0 - по н. депутатах; 1 - по фракціях
vid_pr = 0

for plsession in range(0, len(hrefs)):
    page_link = hrefs[plsession]
    if requests_retry_session().get(page_link).status_code == 200:
        temp_content = read_page_content(page_link, 0)
        with open(temp_data + '/%03d_vr_page.html' % (plsession),  'wb') as output_file:
            output_file.write(temp_content.encode('utf-8'))
    else:
        bad_urls.append(page_link)
    
for filename in os.listdir(temp_data):
    text = read_file(temp_data + "/" + filename)
    tree = html.fromstring(text)
    plain_href = tree.xpath('//div[@class = "vid_pr"]/a[re:match(text(), "друку$")]',
                                            namespaces={"re": "http://exslt.org/regular-expressions"})[vid_pr].get('href')
    if requests_retry_session().get(plain_href).status_code == 200:
        plain_content = read_page_content(plain_href,  0)
        with open(temp_data + "/" + filename.rsplit( ".", 1 )[ 0 ] + "_pl" + ".html",  'wb') as output_file:
            output_file.write(plain_content.encode('cp1251'))
        os.remove(temp_data + "/" + filename)
    else:
        bad_urls.append(page_link)
        
p0 = []
p1 = []
p2 = []
res_table = []
for filename in sorted(os.listdir(temp_data)):
    index = int(filename.rsplit( "_", 3 )[ 0 ])
    visits = pd.read_html(temp_data + "/" + filename)[-1]
    p1 = pd.DataFrame({'ПІБ': visits[0], 
                    dates[index]: visits[1]})
    p2 = pd.DataFrame({'ПІБ': visits[2], 
                    dates[index]: visits[3]})
    p0 = pd.concat([p1, p2], ignore_index=True).dropna()
    # У ВР присутні дві сутності "Тимошенко Ю.В.", перейменовуємо ту, що чоловічого роду
    p0.loc[operator.or_(operator.and_(p0['ПІБ'] == 'Тимошенко Ю.В.',  p0[dates[index]] == 'Присутній'), operator.and_(p0['ПІБ'] == 'Тимошенко Ю.В.',  p0[dates[index]] == 'Відсутній')), 'ПІБ'] = 'Тимошенко Ю.Вл.'
    if index == 0:
        res_table = p0.copy().set_index('ПІБ')
    else:
        p0 = p0.set_index('ПІБ')
        res_table = res_table.join(p0)
    os.remove(temp_data + "/" + filename)

# Генеруємо префікс для вихідного файлу
timestr = time.strftime("%y%m%d-%H%M")

res_table.to_csv(timestr+'-res_table_lxml.csv')
try:  
    os.rmdir(temp_data)
except OSError:  
    print ("Creation of the directory %s failed" % temp_data)
    
if not bad_urls:
    print("Всі посилання на пленарні засідання було оброблено")
else:
    print("Наступні посилання не вдалось обробити:")
    for bad_url in bad_urls:
        print('\t', bad_url, '\n')
