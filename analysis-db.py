#!/usr/bin/env python
import psycopg2

question1 = '''1. What are the most popular three articles of all time? '''
query1 = ('''
    select articles.title, count(*) as views
    from articles
    join log
    on log.path = concat('/article/', articles.slug)
    where log.status like '%200%'
    group by articles.title, log.path
    order by views desc
    limit 3;
    ''')

question2 = '''2. Who are the most popular article authors of all time? '''
query2 = ('''
    select authors.name, count(*) as views
    from authors
    join articles
    on authors.id = articles.author
    join log
    on log.path = concat('/article/', articles.slug)
    where log.status like '%200%'
    group by authors.name
    order by views desc;
    ''')

question3 = '''3.On which days did more than 1% of requests lead to errors?'''
query3 = ("""
    SELECT to_char(date, 'Mon DD, YYYY') as date, rate
    FROM (SELECT date(log.time) as date,
          round(cast(100 * sum(CASE WHEN log.status like '%404%'
          THEN 1 ELSE 0 END)::float /
          count(*) AS numeric), 2) as rate
          FROM log
          GROUP BY date
          ) AS err
          WHERE rate>1
    limit 10;
    """)


def get_results(query):
    pg = psycopg2.connect("dbname=news")
    c = pg.cursor()
    c.execute(query)
    results = c.fetchall()
    pg.close()
    return results


def print_result(query_results):
    # print question
    print query_results[0]
    for result in query_results[1]:
        # output format: article -- 1000 views
        print '  ' + result[0] + ' -- ' + str(result[1]) + ' views'
    print '\n'


def print_result_err(query_results):
    # print question
    print query_results[0]
    for result in query_results[1]:
        # output format: article -- 1000 views
        print '  ' + result[0] + ' -- ' + str(result[1]) + ' %'

if __name__ == '__main__':
    article_results = question1, get_results(query1)
    author_results = question2, get_results(query2)
    error_results = question3, get_results(query3)

    print_result(article_results)
    print_result(author_results)
    print_result_err(error_results)
