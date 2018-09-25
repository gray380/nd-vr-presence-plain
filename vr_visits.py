from bs4 import BeautifulSoup
import requests
import pandas as pd
import operator
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
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
    #else:
    #    print('It eventually worked', response.status_code)
    return content

# Генеруємо префікс для вихідного файлу
timestr = time.strftime("%y%m%d-%H%M")

# Визначаємо загальну кількість сторінок
page=1
page_link = 'http://iportal.rada.gov.ua/news/Rstr_nd/page/%d'
page_content = read_page_content(page_link, page)
page_content_soup = BeautifulSoup(page_content, 'lxml')
pages = page_content_soup.find(class_='pages') 
last_page = 0
for link in pages.find_all('a'):
        page_ref = link.get('href').split('/')
        page_num = int(page_ref[-1])
        if page_num > last_page:
            last_page = page_num

# Створюємо списки дат та посилань
hrefs = []  
dates = []

for page in range(14, 17):
# for page in range(1, last_page+1):
    page_content = read_page_content(page_link, page)
    pl_sessions_soup = BeautifulSoup(page_content, 'lxml')
    pl_sessions = pl_sessions_soup.find_all('div',  {'id': 'list_archive'})
    #Видаляємо зайвий div зі сторінками
    for pl_session in pl_sessions:
        pl_session.find('div',  {'class': 'pages'}).decompose()
        span_dates = pl_session.find_all('span',  {'class': 'date'})
        for _span_dates in span_dates:
            dates.append(_span_dates.text)
        span_hrefs = pl_session.find_all('span',  {'class': 'details'})
        for _span_hrefs in span_hrefs:
            hrefs.append(_span_hrefs.find('a').get('href'))

# Створюємо список посилань на списки у plain html (time consuming)
bad_urls = []
p0 = []
p1 = []
p2 = []
res_table = []

for plsession in range(0, len(hrefs)):
    page_link = hrefs[plsession]
    if requests_retry_session().get(page_link).status_code == 200:
        temp_content = read_page_content(page_link, 0)
        temp_content_soup = BeautifulSoup(temp_content, 'lxml')
        plain_href = temp_content_soup.find('a',  href=lambda href: href and 'ns_reg_print' in href  and 'vid=0' in href).get('href')
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
    

res_table.to_csv(timestr+'-res_table.csv')
if not bad_urls:
    print("Всі посилання на пленарні засідання було оброблено")
else:
    print("Наступні посилання не вдалось обробити:")
    for bad_url in bad_urls:
        print('\t', bad_url, '\n')
