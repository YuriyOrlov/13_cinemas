import requests
from bs4 import BeautifulSoup
import re
# import time
from multiprocessing import Pool, cpu_count
import pickle
# from os.path import isfile
import random

PROXIES_LIST = pickle.load(open('good_prx_2.pickle', 'rb'))


def fetch_page(link):  # , proxie_file=open('/home/yo_n/Projects/env35/Simple_scripts/prx_http.txt', 'r')):
    # time.sleep(1)
    user_agents_list = LoadUserAgents()
    user_agent = random.choice(user_agents_list)
    headers = {"Connection": "close", "User-Agent": user_agent}
    received_data = None
    # proxies_list = [line.rstrip('\n') for line in proxie_file]
    for item in range(5):
        try:
            proxie = random.choice(PROXIES_LIST)
            new_proxie = {'http': '{}'.format(proxie)}
            # print(new_proxie)
            # received_data = requests.get(link, headers=headers)
            received_data = requests.get(link, headers=headers, proxies=new_proxie, timeout=5)
        except requests.exceptions.Timeout:
            # print("Timeout occurred")
            continue
        except requests.exceptions.RequestException:
            # print("Refused connection")
            continue
        if received_data:
            break
    return received_data if received_data else None


def parse_page(raw_html):
    # time.sleep(0.2)
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
    # print('Got number of theaters.')
    return len(parsed_movie_info_page.find_all('td', class_=re.compile('b-td-item')))


def get_mean_rating(movie_link):
    # while True:
    raw_data_page = fetch_page(movie_link)
    parsed_movie_info_page = parse_page(raw_data_page) if raw_data_page else None
    rating = parsed_movie_info_page.find('span', class_=re.compile('rating_ball')) if parsed_movie_info_page else None
    if rating is None:
        try:
            rating = parsed_movie_info_page.find('div', class_=re.compile('div1')).span['title'] if parsed_movie_info_page else None
        except AttributeError:
            rating = None
        # if rating:
        #     break
    print(rating)
    if rating == 'Рейтинг скрыт (недостаточно оценок)':
        return rating
    else:
        return float(rating.text) if rating else None


def retrieve_kinopoisk_movie_info(movie_names):
    kinopoisk_movie_url = 'https://www.kinopoisk.ru/index.php?first=yes&what=&kp_query='
    movie_links = ['{}{}'.format(kinopoisk_movie_url, movie_name) for movie_name in movie_names]
    # print('Got rating.')
    return parallel_page_parsing(get_mean_rating, movie_links)


def output_movies_to_console(parsed_page):
    movie_names, number_of_movie_theaters = fetch_afisha_movie_info(parsed_page)
    # return zip(movie_names, number_of_movie_theaters)
    movie_ratings = retrieve_kinopoisk_movie_info(movie_names)
    return zip(movie_names, number_of_movie_theaters, movie_ratings)


def LoadUserAgents(user_agents_file='/home/yo_n/Projects/env35/Simple_scripts/user_agents.txt'):
    user_agents_list = []
    with open(user_agents_file, 'rb') as file:
        for user_agent in file.readlines():
            if user_agent:
                user_agents_list.append(user_agent.strip()[1: -1 - 1])
    random.shuffle(user_agents_list)
    return user_agents_list


if __name__ == '__main__':
    raw_movies_page = fetch_page('https://www.afisha.ru/msk/schedule_cinema/')
    parsed_page_w_movies_list = parse_page(raw_movies_page)
    print('| Фильм | Кол-во кинотеатров | Рейтинг |')
    zipped = output_movies_to_console(parsed_page_w_movies_list)
    # for n, m, r in sorted(zipped, key=lambda x: x[2] if isinstance(x[2], str) else ""):
    #     print(' {} {} {} '.format(n, m, r))
    for names, movies, ratings in sorted([(n, m, r) for n, m, r in sorted(zipped, key=lambda x: x[2] if isinstance(x[2], float) else 0, reverse=True)], key=lambda x: x[1] if isinstance(x[2], float) else 0, reverse=True):
        print('| {} | {} | {} |'.format(names, movies, ratings))

# data = pickle.load(open('data.pickle', 'rb'))
# for n, m,r in sorted([(n, m, r) for n, m, r in sorted(data, key=lambda x: x[2] if isinstance(x[2], float) else 0, reverse=True)], key=lambda x: x[1] if isinstance(x[2], float) else 0, reverse = True):
#     print(n,m,r)