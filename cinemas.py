import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool, cpu_count
import _pickle as pickle
from os.path import isfile
import random

PROXIES_LIST = pickle.load(open('prx.pkl', 'rb'))
USER_AGENTS_LIST = pickle.load(open('user_agents.pkl', 'rb'))


def get_page(link, number_of_retries=10):
    user_agent = random.choice(USER_AGENTS_LIST)
    headers = {"Connection": "close", "User-Agent": user_agent}
    received_data = None
    for item in range(number_of_retries):
        try:
            proxie = random.choice(PROXIES_LIST)
            new_proxie = {'http': '{}'.format(proxie)}
            received_data = requests.get(link, headers=headers, proxies=new_proxie, timeout=6)
        except requests.exceptions.Timeout:
            continue
        except requests.exceptions.RequestException:
            continue
        if received_data:
            break
    return received_data if received_data else None


def fetch_page(raw_html):
    return BeautifulSoup(raw_html.content, 'lxml')


def parallel_page_parsing(func, movie_links):
    num_of_parallel_processes = cpu_count() * 2
    pool = Pool(num_of_parallel_processes)
    return pool.map(func, movie_links)


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


def get_number_of_movie_theaters(movie_link):
    raw_movie_page = get_page(movie_link)
    ready_movie_page = fetch_page(raw_movie_page) if raw_movie_page else None
    return len(ready_movie_page.find_all('td', class_=re.compile('b-td-item')))


def get_mean_rating(movie_link):
    raw_movie_page = get_page(movie_link)
    ready_movie_page = fetch_page(raw_movie_page) if raw_movie_page else None
    rating = ready_movie_page.find('span', class_=re.compile('rating_ball')) if ready_movie_page else None
    if rating is None:
        try:
            rating = ready_movie_page.find('div', class_=re.compile('div1')).span['title'] if ready_movie_page else None
        except AttributeError:
            return None
    if rating == 'Рейтинг скрыт (недостаточно оценок)':
        return rating
    else:
        return float(rating.text) if rating else None


def retrieve_kinopoisk_movie_info(movie_names):
    kinopoisk_movie_url = 'https://www.kinopoisk.ru/index.php?first=yes&what=&kp_query='
    movie_links = ['{}{}'.format(kinopoisk_movie_url, movie_name) for movie_name in movie_names]
    return parallel_page_parsing(get_mean_rating, movie_links)


def output_movies_to_console(parsed_page):
    movie_names, number_of_movie_theaters = fetch_afisha_movie_info(parsed_page)
    movie_ratings = retrieve_kinopoisk_movie_info(movie_names)
    return zip(movie_names, number_of_movie_theaters, movie_ratings)


def write_pickled_file(prepared_data, filename='zipped_data.pkl'):
    with open(filename, 'wb') as file:
        pickle.dump(prepared_data, file)


def load_pickled_data(filename):
    return pickle.load(open(filename, 'rb'))


if __name__ == '__main__':
    if not isfile('zipped_data.pkl'):
        raw_movies_page = get_page('https://www.afisha.ru/msk/schedule_cinema/')
        parsed_page_w_movies_list = fetch_page(raw_movies_page)
        zipped = output_movies_to_console(parsed_page_w_movies_list)
        write_pickled_file(zipped)
    else:
        print('Loaded cached result.\n')
        print('| Фильм | Кол-во кинотеатров | Рейтинг |')
        zipped = load_pickled_data('zipped_data.pkl')
        num_entr_for_return = 10
    for names, movies, ratings in sorted([(names, movies, ratings) for names, movies, ratings in sorted(zipped,
                                           key=lambda x: x[2] if isinstance(x[2], float) else 0, reverse=True)],
                                           key=lambda x: x[1] if isinstance(x[2], float) else 0, reverse=True):
        print('| {} | {} | {} |'.format(names, movies, ratings))
