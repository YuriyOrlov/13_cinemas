from args_parser import ConsoleArgsParser
from proxie_test import ProxieList
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool, cpu_count
import _pickle as pickle
from os.path import isfile
import datetime
import random
from functools import partial


def get_page(link, proxies_list=None, user_agent_list=None, number_of_retries=10):
    user_agent = random.choice(user_agent_list) if user_agent_list else None
    headers = {"Connection": "close", "User-Agent": user_agent} if user_agent else None
    received_data = None
    for item in range(number_of_retries):
        try:
            proxie = random.choice(proxies_list) if proxies_list else None
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


def parallel_page_parsing(func, movie_links, proxies_list, user_agents_list):
    num_of_parallel_processes = cpu_count() * 2
    pool = Pool(num_of_parallel_processes)
    return pool.map(partial(func, proxies_list=proxies_list, user_agents_list=user_agents_list), movie_links)


def fetch_afisha_movie_info(raw_info, proxies_list, user_agents_list):
    parsed_data = raw_info.find_all('h3', class_=re.compile('usetags'))
    movie_names = [movie_name.text for movie_name in parsed_data]
    movie_links = [movie_link.a.get('href') for movie_link in parsed_data]
    movie_id_number = re.compile(r'[0-9]+')
    movies_id_list = [movie_id_number.search(link).group(0) for link in movie_links]
    afisha_movie_url = 'https://www.afisha.ru/msk/schedule_cinema_product/'
    movie_links = ['{}{}'.format(afisha_movie_url, movie_id) for movie_id in movies_id_list]
    number_of_movie_theaters = parallel_page_parsing(get_number_of_movie_theaters,
                                                     movie_links,
                                                     proxies_list,
                                                     user_agents_list)
    return movie_names, number_of_movie_theaters


def get_number_of_movie_theaters(movie_link, proxies_list, user_agents_list):
    raw_movie_page = get_page(movie_link, proxies_list, user_agents_list)
    ready_movie_page = fetch_page(raw_movie_page) if raw_movie_page else None
    return len(ready_movie_page.find_all('td', class_=re.compile('b-td-item')))


def get_mean_rating(movie_link, proxies_list, user_agents_list):
    raw_movie_page = get_page(movie_link, proxies_list, user_agents_list)
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


def retrieve_kinopoisk_movie_info(movie_names, proxies_list, user_agents_list):
    kinopoisk_movie_url = 'https://www.kinopoisk.ru/index.php?first=yes&what=&kp_query='
    movie_links = ['{}{}'.format(kinopoisk_movie_url, movie_name) for movie_name in movie_names]
    return parallel_page_parsing(get_mean_rating, movie_links, proxies_list, user_agents_list)


def output_movies_to_console(parsed_page, proxies_list, user_agents_list):
    movie_names, number_of_movie_theaters = fetch_afisha_movie_info(parsed_page, proxies_list, user_agents_list)
    movie_ratings = retrieve_kinopoisk_movie_info(movie_names, proxies_list, user_agents_list)
    return zip(movie_names, number_of_movie_theaters, movie_ratings)


def determine_action_and_make_output_to_console(*args, **kwargs):
    header = '| Фильм | Кол-во кинотеатров | Рейтинг |'
    if (not isfile('movies_data_zipped.pkl')) and (chek_update_date()):
        write_pickled_file(datetime.date.today(), 'last_updated.pkl')
        proxies_list = what_kind_of_proxies_to_use(get_anonymous_proxies=kwargs.get('get_anonymous_proxies'),
                                                   get_common_proxies=kwargs.get('get_common_proxies'))
        user_agents_list = define_where_to_take_user_agents_list(get_user_agents=kwargs.get('get_user_agents'))
        raw_movies_page = get_page('https://www.afisha.ru/msk/schedule_cinema/', proxies_list, user_agents_list)
        parsed_page_w_movies_list = fetch_page(raw_movies_page)
        zipped = output_movies_to_console(parsed_page_w_movies_list, proxies_list, user_agents_list)
        write_pickled_file(zipped)
        return zipped, header
    else:
        zipped = load_pickled_data('movies_data_zipped.pkl')
        return zipped, header


def compare_dates(date):
    return False if (date - datetime.date.today()).days < -2 else True


def chek_update_date():
    if isfile('last_updated.pkl'):
        return compare_dates(load_pickled_data('last_updated.pkl'))
    else:
        write_pickled_file(datetime.date.today(), 'last_updated.pkl')
        return True


def define_where_to_take_anon_proxies_list(**kwargs):
    if isfile('anon_prx.pkl') and kwargs.get('get_anonymous_proxies'):
        return load_pickled_data('anon_prx.pkl')
    else:
        return create_anonymous_proxie_list()


def define_where_to_take_common_proxies_list(**kwargs):
    if isfile('untested_prx.pkl') and kwargs.get('get_common_proxies'):
        return load_pickled_data('untested_prx.pkl')
    else:
        return create_common_proxie_list()


def what_kind_of_proxies_to_use(**kwargs):
    if kwargs.get('get_anonymous_proxies') and not kwargs.get('get_common_proxies'):
        return define_where_to_take_anon_proxies_list(get_anonymous_proxies=kwargs.get('get_anonymous_proxies'))
    elif kwargs.get('get_common_proxies') and not kwargs.get('get_anonymous_proxies'):
        return define_where_to_take_common_proxies_list(get_common_proxies=kwargs.get('get_common_proxies'))
    elif not kwargs.get('get_common_proxies') and not kwargs.get('get_anonymous_proxies'):
        return None
    else:
        raise ValueError('Too many keys for proxies. Choose the kind of proxies to use.')


def define_where_to_take_user_agents_list(**kwargs):
    if isfile('user_agents.pkl') and (not kwargs.get('get_user_agents')):
        return load_pickled_data('user_agents.pkl')
    else:
        user_agents_list = create_user_agents_list()
        return user_agents_list


def write_pickled_file(prepared_data, filename='movies_data_zipped.pkl'):
    with open(filename, 'wb') as file:
        pickle.dump(prepared_data, file)


def load_pickled_data(filename):
    return pickle.load(open(filename, 'rb')) if isfile(filename) else None


def sort_by_rating(list_item):
    return list_item[2] if isinstance(list_item[2], float) else 0


def sort_by_theaters(list_item):
    return list_item[1] if isinstance(list_item[2], float) else 0


class Movie(object):

    def __init__(self, name=None, cinemas=None, rating=None):
        self.name = name
        self.cinemas = cinemas
        self.rating = rating

    def __repr__(self):
        return repr((self.name, self.cinemas, self.rating))


def create_user_agents_list():
    user_agents = ProxieList()
    user_agents.load_user_agents_from_any_source()
    user_agents.save_user_agents()
    return user_agents.user_agents


def create_common_proxie_list():
    proxies = ProxieList()
    proxies.get_proxie_list()
    proxies.save_untested_proxies()
    return proxies.proxie_list


def create_anonymous_proxie_list():
    proxies = ProxieList()
    proxies.get_proxie_list()
    proxies.parallel_proxie_test()
    proxies.save_good_proxies()
    return proxies.proxie_list


if __name__ == '__main__':
    parser = ConsoleArgsParser()
    args = parser.parse_args()
    last_update = chek_update_date()
    zipped_cinema_data, console_output_header = determine_action_and_make_output_to_console(last_update,
                                                                get_common_proxies=args.get_common_proxies,
                                                                get_anonymous_proxies=args.get_anonymous_proxies,
                                                                get_user_agents=args.get_user_agents)
    num__of_entr_to_return = args.ret
    sorted_by_rating = sorted(zipped_cinema_data, key=sort_by_rating)
    sorted_by_theaters_and_rating = sorted(sorted_by_rating, key=sort_by_theaters, reverse=True)
    if num__of_entr_to_return > 0:
        print(console_output_header)
        for names, movies, ratings in sorted_by_theaters_and_rating[:num__of_entr_to_return]:
            print('| {} | {} | {} |'.format(names, movies, ratings))
    else:
        print(console_output_header)
        for names, movies, ratings in sorted_by_theaters_and_rating:
            print('| {} | {} | {} |'.format(names, movies, ratings))
