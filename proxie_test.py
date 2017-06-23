import requests
import re
import _pickle as pickle
from multiprocessing import Pool, cpu_count
from bs4 import BeautifulSoup
import random
from os.path import isfile
import json


class IpAndProxieLinks(object):

    def __init__(self, detect_ip_link='http://icanhazip.com',
                 link='http://proxyserverlist-24.blogspot.ru/'):
        self.link = link
        self.detect_ip_link = detect_ip_link

    def __repr__(self):
        return '{}, {}'.format(self.detect_ip_link, self.link)


class ProxieList(IpAndProxieLinks):

    def __init__(self, link=None, detect_ip_link=None,
                 own_ip=None,
                 user_agents=None,
                 original_ip_adress=None,
                 proxie_list=None,
                 anonymous_proxies=None):
        if detect_ip_link is None and link is None:
            super().__init__()
        else:
            super().__init__(detect_ip_link, link)
        self.own_ip = own_ip
        self.user_agents = user_agents
        self.original_ip_adress = original_ip_adress
        self.proxie_list = proxie_list
        self.anonymous_proxies = anonymous_proxies

    def get_own_ip(self):
        self.own_ip = (requests.get(self.detect_ip_link)).text.strip()

    def get_and_fetch_page(self, link):
        raw_page = requests.get(link)
        return BeautifulSoup(raw_page.content, 'lxml')

    def get_proxie_list(self):
        parsed_main_page_link = self.get_and_fetch_page(self.link)
        blog_posts = parsed_main_page_link.find('div', class_=re.compile('blog-posts hfeed'))
        latest_proxies_link = blog_posts('h3', class_=re.compile('post-title entry-title'))[0].a['href']
        latest_proxies_page = self.get_and_fetch_page(latest_proxies_link)
        proxie_list = latest_proxies_page.find('pre', class_=re.compile('alt2')).span.text.split('\n')
        if proxie_list:
            self.proxie_list = proxie_list
        else:
            return None

    def load_user_agents_from_any_source(self):
        if (self.user_agents is None) and (not isfile('user_agents.json')):
            self.load_user_agents_from_web()
        elif (self.user_agents is None) and (isfile('user_agents.json')):
            self.load_user_agents_from_JSON_file(self, 'user_agents.json')
        elif (self.user_agents is None) and (isfile('user_agents.pkl')):
            self.load_user_agents_from_PKL_file(self, 'user_agents.pkl')
        else:
            return None

    def load_user_agents_from_web(self, url='https://github.com/cvandeplas/pystemon/blob/master/user-agents.txt'):
        if url == 'https://github.com/cvandeplas/pystemon/blob/master/user-agents.txt':
            raw_page = requests.get(url)
            user_agents_parsed_page = BeautifulSoup(raw_page.content, 'lxml')
            user_agents_elements = user_agents_parsed_page.find_all('td',
                                                                    class_='blob-code blob-code-inner js-file-line')
            user_agents_list = [user_agent.text for user_agent in user_agents_elements]
            self.user_agents = user_agents_list
        else:
            return None

    def load_user_agents_from_JSON_file(self, user_agents_file):
        self.user_agents = json.load(open(user_agents_file, 'r'))

    def load_user_agents_from_TXT_file(self, user_agents_file):
        if user_agents_file:
            user_agents_list = []
            with open(user_agents_file, 'rb') as file:
                for user_agent in file.readlines():
                    if user_agent:
                        user_agents_list.append(user_agent.strip()[1: -1 - 1])
            random.shuffle(user_agents_list)
            self.user_agents = user_agents_list
        else:
            return None

    def load_user_agents_from_PKL_file(self, user_agents_file):
        return pickle.load(open(user_agents_file, 'rb'))

    def test_ip(self, proxie, number_of_retries=10, default_timeout=5):
        user_agent = random.choice(self.user_agents) if self.user_agents else None
        headers = {"Connection": "close", "User-Agent": user_agent} if user_agent else None
        received_data = None
        for item in range(number_of_retries):
            try:
                new_proxie = {'http': '{}'.format(proxie)}
                received_data = requests.get(self.detect_ip_link,
                                             headers=headers,
                                             proxies=new_proxie,
                                             timeout=default_timeout)
            except requests.exceptions.Timeout:
                received_data = None
                break
            except requests.exceptions.RequestException:
                received_data = None
                break
            if received_data:
                break
        if received_data:
            visible_ip = received_data.text.strip()
            if (visible_ip != self.original_ip_adress) and (visible_ip != 'HTTP/1.1 400 Bad Request'):
                return proxie
        else:
            return None

    def parallel_proxie_test(self):
        if self.proxie_list:
            num_of_parallel_processes = cpu_count() * 2
            pool = Pool(num_of_parallel_processes)
            anonymous_proxies = pool.map(self.test_ip, self.proxie_list)
            self.anonymous_proxies = [proxie for proxie in anonymous_proxies if proxie is not None]
        else:
            return None

    def save_untested_proxies(self):
        if self.proxie_list:
            with open('untested_prx.pkl', 'wb') as file:
                pickle.dump(self.proxie_list, file)
        else:
            return None

    def save_good_proxies(self):
        if self.anonymous_proxies:
            with open('anon_prx.pkl', 'wb') as file:
                pickle.dump(self.anonymous_proxies, file)
        else:
            return None

    def save_user_agents(self):
        if self.user_agents:
            with open('user_agents.pkl', 'wb') as file:
                pickle.dump(self.anonymous_proxies, file)
        else:
            return None

    def load_good_proxies(self):
        if isfile('anon_prx.pkl'):
            with open('anon_prx.pkl', 'rb') as file:
                return pickle.load(file)
        else:
            return None


if __name__ == '__main__':
    proxies = ProxieList()
    proxies.get_proxie_list()
    proxies.load_user_agents_from_any_source()
    proxies.parallel_proxie_test()
    proxies.save_good_proxies()
