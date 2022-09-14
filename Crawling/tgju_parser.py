from bs4 import BeautifulSoup
import jdatetime
import os


def parse_html(html_text):

    soup = BeautifulSoup(html_text, features="html.parser")
    title = soup.find('h1', {'class': 'news-article-title'}).a['href'].split('/')[-1].replace('-', ' ')
    text = soup.find('div', {'class': 'news-article-single-content'}).get_text()
    category = soup.find('a', {'class': 'news-article-tag'}).text.replace('\n', '')
    summary = soup.find('div', {'class': 'summary'})
    summary = '' if not summary else summary.get_text().replace('\n', '')
    writer = soup.find('div', {'class': 'news-article-source'}).a
    writer = '' if not writer else writer.text
    time = soup.find('div', {'class': 'article-time'})
    time = '' if not time else time.text
    tags = soup.find_all('a', {'class': 'news-article-tag'})
    tags = '-'.join([tag.text.replace('\n', '') for tag in tags])
    link = soup.find('div', {'class': 'article-short-link-item'})['data-clipboard-text']
    code = link.split('/')[-1]

    return code, title, text, category, summary, writer, time, tags, link


def extract_htmls_write_in_csv(directory_path):

    time = jdatetime.datetime.now()
    with open(f'/opt/airflow/logs/{str(time).replace(" ", "_").replace(":", "-")}.csv', 'a+') as f:
        f.write('code,title,text,category,summary,writer,time,tags,link\n')

    tree = list(os.walk(directory_path))
    for record in tree[1:]:
        try:
            with open(f'{record[0]}/{record[2][0]}', 'r') as f:
                webpage = f.read()
                data = parse_html(webpage)
                data = [col.replace(',', '') for col in data]
                data = ','.join(data).replace('\n', ' ')
            with open(f'/opt/airflow/logs/{str(time).replace(" ", "_").replace(":", "-")}.csv', 'a+') as f:
                f.write(f'{data}\n')
        except:
            print('failed')


if __name__ == "__main__":
    extract_htmls_write_in_csv('/opt/airflow/logs/www.tgju.org/news/')

