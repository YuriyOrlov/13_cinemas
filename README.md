# Cinemas

This script parses two internet sites. 
From https://www.afisha.ru/msk/schedule_cinema/ program takes movies and then it loads average rating for every movie from kinopoisk.ru.
Because of the policy of the second site it often bans your IP adress and you would not receive any more rating from it. So it's recommended to use all functionality of this script.

When you run program for the first time it will create a file with date. It's needed for the script to understand how often to renew the list. Normally, the cinema list will be renewed once in two days. After the first run the program will create a file with the whole list of movies, ratings and the number of theaters. It's cache, so if you want, you can run the script as many times as you want, it would't use web. At least for the next two days. The console output will show only first 10 string, but if you will use additional key, you will be able to see all results.

### What to do if you was blocked and cannot see ratings.
In this case you could use additional keys (see help) to download proxies and user agents for this script. They will be saved in files and used as cache, when it will be needed. Moreover, you could use testing ability of this script to find only L3 proxies (non transparent) to level up your privicy. In this case pay attantion for the time needed for program run, because proxie testing could take 40 - 60 minutes. Furthermore, you can renew all this lists manually. Hope you'll like my simple script!


```#!bash
$ python cinemas.py
| Фильм | Кол-во кинотеатров | Рейтинг |
| Трансформеры: Последний рыцарь | 172 | 5.808 |
| Тачки-3 | 152 | 7.049 |
| Мумия | 140 | 5.832 |
| Очень плохие девчонки | 133 | 5.806 |
| Весь этот мир | 133 | 6.191 |
| Пираты Карибского моря: Мертвецы не рассказывают сказки | 95 | 6.674 |
| Чудо-женщина | 71 | 7.011 |
| Вечно молодой | 60 | 7.443 |
| Нелюбовь | 40 | 7.663 |
| Меч короля Артура | 36 | 7.412 |

```

# Project Goals

The code is written for educational purposes. Training course for web-developers - [DEVMAN.org](https://devman.org)
