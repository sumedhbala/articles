#!/usr/bin/env python3
#
# Code for Udacity Log Analysis Project
#

import psycopg2

DB = 'news'
POPULAR_ARTICLES_QUERY = ("select articles.title, count(articles.title) "
                          "as views from articles, log where "
                          "articles.slug = regexp_replace(log.path, "
                          "'^/article/', '') and log.path ~*  '^/article' "
                          "group by articles.title order "
                          "by views desc limit 3;")
POPULAR_AUTHORS_QUERY = ("select authors.name, count(authors.name) as views "
                         "from authors,articles, log where "
                         "authors.id = articles.author and "
                         "articles.slug = regexp_replace(log.path, "
                         "'^/article/', '') and log.path ~*  '^/article' "
                         "group by authors.name  order by views desc;")
ERRORS_QUERY = ("select a.date,ROUND((a.count*100)::numeric/(b.count)::numeric"
                ",2) from (select status, date(time), count(*) from log "
                "group by status,date) a, (select status, date(time), "
                "count(*) from log group by status,date) b where a.date=b.date"
                " and a.status = '404 NOT FOUND' and b.status = '200 OK' "
                "and a.count > b.count/100;")


def print_popular_artiles():
    """ Prints the 3 most popular articles"""
    try:
        db = psycopg2.connect(database=DB)
        cur = db.cursor()
        cur.execute(POPULAR_ARTICLES_QUERY)
        results = cur.fetchall()
        print("Most popular Articles:")
        for result in results:
            print("{} - {} views".format(result[0], result[1]))
    finally:
        db.close()


def print_popular_authors():
    """ Prints the authors according to their popularity"""
    try:
        db = psycopg2.connect(database=DB)
        cur = db.cursor()
        cur.execute(POPULAR_AUTHORS_QUERY)
        results = cur.fetchall()
        print("Most popular Authors:")
        for result in results:
            print("{} - {} views".format(result[0], result[1]))
    finally:
        db.close()


def print_bad_access_days():
    """ Prints the days on which the number of errors
    from the website exceeded 1%"""
    try:
        db = psycopg2.connect(database=DB)
        cur = db.cursor()
        cur.execute(ERRORS_QUERY)
        results = cur.fetchall()
        print("Bad access days:")
        for result in results:
            print("{} - {}% errors".format(result[0], result[1]))
    finally:
        db.close()


if __name__ == '__main__':
    print_popular_artiles()
    print("")
    print_popular_authors()
    print("")
    print_bad_access_days()