import requests
from bs4 import BeautifulSoup
import re
import time
from multiprocessing import Pool, cpu_count


def fetch_page(link):
    time.sleep(0.3)
    return requests.get(link)


def parse_page(raw_html):
    time.sleep(0.2)
    return BeautifulSoup(raw_html.content, 'lxml')


def fetch_movie_info(raw_info):
    parsed_data = raw_info.find_all('h3', class_=re.compile('usetags'))
    movie_names = [movie_name.text for movie_name in parsed_data]
    movie_links = [movie_link.a.get('href') for movie_link in parsed_data]
    movie_id_number = re.compile(r'[0-9]+')
    movies_id_list = [movie_id_number.search(link).group(0) for link in movie_links]
    movie_links = ['https://www.afisha.ru/msk/schedule_cinema_product/{}'.format(movie_id)
                   for movie_id in movies_id_list]
    number_of_movie_theaters = parallel_page_parsing(movie_links)
    return zip(movie_names, number_of_movie_theaters)


def parallel_page_parsing(movie_links):
    num_of_parallel_processes = cpu_count() * 2
    pool = Pool(num_of_parallel_processes)
    return pool.map(get_number_of_movie_theaters, movie_links)


def get_number_of_movie_theaters(movie_link):
    raw_data_page = fetch_page(movie_link)
    parsed_movie_info_page = parse_page(raw_data_page)
    return len(parsed_movie_info_page.find_all('td', class_=re.compile('b-td-item')))


def output_movies_to_console(movies):
    pass


if __name__ == '__main__':
    raw_movies_page = fetch_page('https://www.afisha.ru/msk/schedule_cinema/')
    parsed_page_w_movies_list = parse_page(raw_movies_page)
    [print('{} -- > {}'.format(movie, number_of_theaters)) for movie, number_of_theaters in fetch_movie_info(parsed_page_w_movies_list)]
    # output_movies_to_console(movie_names)
