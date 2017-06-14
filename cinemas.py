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


def fetch_afisha_movie_info(raw_info):
    parsed_data = raw_info.find_all('h3', class_=re.compile('usetags'))
    movie_names = [movie_name.text for movie_name in parsed_data]
    movie_links = [movie_link.a.get('href') for movie_link in parsed_data]
    movie_id_number = re.compile(r'[0-9]+')
    movies_id_list = [movie_id_number.search(link).group(0) for link in movie_links]
    afisha_movie_url = 'https://www.afisha.ru/msk/schedule_cinema_product/'
    movie_links = ['{}{}'.format(afisha_movie_url, movie_id) for movie_id in movies_id_list]
    number_of_movie_theaters = parallel_page_parsing(get_number_of_movie_theaters, movie_links)
    return movie_names, number_of_movie_theaters


def parallel_page_parsing(func, movie_links):
    num_of_parallel_processes = cpu_count() * 2
    pool = Pool(num_of_parallel_processes)
    return pool.map(func, movie_links)


def get_number_of_movie_theaters(movie_link):
    raw_data_page = fetch_page(movie_link)
    parsed_movie_info_page = parse_page(raw_data_page)
    return len(parsed_movie_info_page.find_all('td', class_=re.compile('b-td-item')))


def get_mean_rating(movie_link):
    raw_data_page = fetch_page(movie_link)
    parsed_movie_info_page = parse_page(raw_data_page)
    rating = parsed_movie_info_page.find('span', class_=re.compile('rating_ball'))
    return float(rating.text) if rating else None


def retrieve_kinopoisk_movie_info(movie_names):
    kinopoisk_movie_url = 'https://www.kinopoisk.ru/index.php?first=yes&what=&kp_query='
    movie_links = ['{}{}'.format(kinopoisk_movie_url, movie_name) for movie_name in movie_names]
    return parallel_page_parsing(get_mean_rating, movie_links)


def output_movies_to_console(parsed_page):
    movie_names, number_of_movie_theaters = fetch_afisha_movie_info(parsed_page)
    movie_ratings = retrieve_kinopoisk_movie_info(movie_names)
    return zip(movie_names, number_of_movie_theaters, movie_ratings)


if __name__ == '__main__':
    raw_movies_page = fetch_page('https://www.afisha.ru/msk/schedule_cinema/')
    parsed_page_w_movies_list = parse_page(raw_movies_page)
    print('| Фильм | Кол-во кинотеатров | Рейтинг |')
    zipped = output_movies_to_console(parsed_page_w_movies_list)
    for n, m, r in sorted(zipped, key=lambda x: x[2] if isinstance(x[2], str) else ""):
        print(' {} {} {} '.format(n, m, r))
    # for names, movies, ratings in sorted(zipped_movies_info, key=lambda x: x[2] if isinstance(x[2], str) else ""):
    #     print('| {} | {} | {} |'.format(names, movies, ratings))
