import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool, cpu_count
import _pickle as pickle
from os.path import isfile
import random
import json

# PROXIES_LIST = pickle.load(open('prx.pkl', 'rb'))
# USER_AGENTS_LIST = pickle.load(open('user_agents.pkl', 'rb'))


def get_page(link, number_of_retries=10):
    user_agent = random.choice(USER_AGENTS_LIST) if USER_AGENTS_LIST else None
    headers = {"Connection": "close", "User-Agent": user_agent} if user_agent else None
    received_data = None
    for item in range(number_of_retries):
        try:
            proxie = random.choice(PROXIES_LIST) if PROXIES_LIST else None
            new_proxie = {'http': '{}'.format(proxie)} if proxie else None
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


def load_proxies_and_user_agents(proxies_filename, user_agents_filename):
    if (isfile(proxies_filename)) and (isfile(user_agents_filename)):
        return load_json_file(proxies_filename), load_json_file(user_agents_filename)
    else:
        return None, None


def write_pickled_file(prepared_data, filename='movies_data_zipped.pkl'):
    with open(filename, 'wb') as file:
        pickle.dump(prepared_data, file)


def load_pickled_data(filename):
    return pickle.load(open(filename, 'rb'))


def write_json_file(prepared_data, filename='movies_data_zipped.json'):
    with open(filename, 'w') as file:
        json.dump(dict(enumerate(list(prepared_data))), file)


def load_json_file(filename='movies_data_zipped.json'):
    return json.load(open(filename, 'r'))


def sort_by_rating(list_item):
    return list_item[2] if isinstance(list_item[2], float) else 0


def sort_by_theaters(list_item):
    return list_item[1] if isinstance(list_item[2], float) else 0


PROXIES_LIST, USER_AGENTS_LIST = load_json_file('good_proxie.json'), load_json_file('user_agents.json')

if __name__ == '__main__':
    # if not isfile('movies_data_zipped.pkl'):
    if not isfile('movies_data_zipped.json'):
        raw_movies_page = get_page('https://www.afisha.ru/msk/schedule_cinema/')
        parsed_page_w_movies_list = fetch_page(raw_movies_page)
        zipped = output_movies_to_console(parsed_page_w_movies_list)
        print('| Фильм | Кол-во кинотеатров | Рейтинг |')
        # write_pickled_file(zipped)
        try:
            write_json_file(zipped)
        except TypeError:
            pass
    else:
        # zipped = load_pickled_data('movies_data_zipped.pkl')
        zipped = load_json_file()
        print('| Фильм | Кол-во кинотеатров | Рейтинг |')
    num_entr_for_return = 10
    sorted_by_rating = sorted(zipped, key=sort_by_rating)
    sorted_by_theaters_and_rating = sorted(sorted_by_rating, key=sort_by_theaters, reverse=True)
    for names, movies, ratings in sorted_by_theaters_and_rating[:num_entr_for_return]:
        print('| {} | {} | {} |'.format(names, movies, ratings))
