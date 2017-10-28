import psycopg2

question1 = ''' 1. What are the most popular three articles of all time? '''
query1 = ('''
    select articles.title, count(*) as views
    from articles
    join log
    on log.path like concat('%', articles.slug, '%')
    where log.status like '%200%'
    group by articles.title, log.path
    order by views desc
    limit 3; 
    ''')

question2 ='''2. Who are the most popular article authors of all time? '''
query2 = ('''
    select authors.name, count(*) as views
    from authors
    join articles
    on authors.id = articles.author
    join log
    on log.path like concat('%', articles.slug, '%')
    where log.status like '%200%'
    group by authors.name
    order by views desc
    limit 3;
    ''')

question3 =''' On which days did more than 1% of requests lead to errors?'''
query3 = ("""
    SELECT date, rate
    FROM (SELECT to_char(log.time, 'Mon DD, YYYY') as date, round(cast(100 * sum(CASE WHEN log.status like '%404%' THEN 1 ELSE 0 END)::float /
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

    results =  c.fetchall()
    for item in results:
        print item
    pg.close()

if __name__ == '__main__':
    # make question list associate with according query
    querys = [query1, query2, query3]
    questions = [question1, question2, question3]
    for question, query in zip(questions, querys):
        print question
        get_results(query)
        print '\n'

    
