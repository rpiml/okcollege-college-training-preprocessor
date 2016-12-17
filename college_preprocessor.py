import numpy as np
import pandas as pd
import io, csv, os
import StringIO
import redis
import pika
import helpers

def SAT_begin(score):
    '''
    Splits the SAT score range string into two real valued numbers
    '''
    if isinstance(score, basestring):
        if '-' not in score:
            begin = score
        else:
            begin = score.split('-')[0]
        if int(begin) > 100:
            return begin

    return 0

def SAT_end(score):
    '''
    Splits the SAT score range string into two real valued numbers
    '''
    if isinstance(score, basestring):
        if '-' not in score:
            end = score
        else:
            end = score.split('-')[1]
        if int(end) > 100:
            return end

    return 0

def ACT_begin(score):
    '''
    Splits the ACT score range string into two real valued numbers
    '''
    if isinstance(score, basestring):
        if '-' not in score:
            begin = score
        else:
            begin = score.split('-')[0]
        if int(begin) < 100:
            return begin

    return 0

def ACT_end(score):
    '''
    Splits the ACT score range string into two real valued numbers
    '''
    if isinstance(score, basestring):
        if '-' not in score:
            end = score
        else:
            end = score.split('-')[1]
        if int(end) < 100:
            return end

    return 0

def ratio(rat):
    '''
    Converts the student-faculty ratio string into a real valued number
    '''
    if isinstance(rat, basestring):
        student = rat.split(':')[0]
        faculty = rat.split(':')[1]
        return float(student)/float(faculty)
    return 0

def strip_chars(value):
    '''
    Strips all cell values of ,$% for easy parsing
    '''
    strvalue = value
    if isinstance(strvalue, basestring):
        strvalue = strvalue.replace('$', '')
        strvalue = strvalue.replace(',', '')
        if '%' in strvalue:
            strvalue = strvalue.replace('%', '')
            strvalue = str(float(strvalue)/100)
        return strvalue
    else:
        return value

def separate_parens(value):
    '''
    Some cells have an unecessary description within parenthesis,
    so just remove it from the cell
    '''
    strvalue = value
    if isinstance(strvalue, basestring):
        if '(' in strvalue:
            strvalue = strvalue.split('(')
            return strvalue[0]
    
    return value

def parselabels(cols_file):
    '''
    Parses the features text file into an array and returns it
    '''
    columns = []
    with open(cols_file) as f:
        for item in f:
            columns = item.split(',')

    return columns

def parsecolleges(college_file, columns):
    '''
    Parses the colleges csv file into a pandas dataframe and
    adjusts SAT/ACT & student/faculty ratio to contain
    real valued numbers for ML algorithms
    '''
    df = pd.read_csv(college_file, header=None)
    df.columns = columns
    df['SAT1'] = df['SAT/ACT 25th-75th percentile'].apply(SAT_begin)
    df['SAT2'] = df['SAT/ACT 25th-75th percentile'].apply(SAT_end)
    df['ACT1'] = df['SAT/ACT 25th-75th percentile'].apply(ACT_begin)
    df['ACT2'] = df['SAT/ACT 25th-75th percentile'].apply(ACT_end)
    df['Student-faculty ratio'] = df['Student-faculty ratio'].apply(ratio)
    df.drop('SAT/ACT 25th-75th percentile', axis=1, inplace=True)
    df = df.sort_index(axis=1)

    csv_file_obj = StringIO.StringIO()

    df.to_csv(csv_file_obj, sep='\t', index=False)

    csv_file_obj.seek(0)
    return csv_file_obj

def getfeaturestring(columns):
    '''
    Converts the columns txt file into a string for redis
    '''
    columns.sort()
    column_string = ""
    for i in columns:
        column_string += i + "\tnumerical\n"

    return column_string

def setredis(column_string, college_string):
    '''
    Adds the colleges & features string into the redis db
    '''
    r = redis.StrictRedis(host=(os.environ['REDIS_HOST'] or "localhost"))
    r.set('learning:college_training.csv', college_string)
    r.set('learning:college_features.csv', column_string)


def rabbitmq_callback(ch, method, properties, body):
    print('Message received: %s' % body.decode('utf-8'))
    try:
        college_file = 'assets/colleges.csv'
        cols_file = 'assets/column_labels.csv'

        columns = parselabels(cols_file)
        csv_file_obj = parsecolleges(college_file, columns)
        college_string = csv_file_obj.read()
        column_string = getfeaturestring(columns)
        setredis(column_string, college_string)

    except Exception as e:
        print e
        return

    print('Message processed: %s' % body.decode('utf-8'))

def main():
    '''
    Preprocesses the colleges data and ultimately adds it into redis
    '''

    # Must wait for redis before connecting to RabbitMQ or else the RabbitMQ
    # heartbeats will time out!
    helpers.wait_for_redis()

    conn = helpers.rabbitmq_connect()
    channel = conn.channel()

    channel.exchange_declare(exchange='preprocessor', exchange_type='direct', auto_delete=True)
    channel.queue_declare(queue='college-training-preprocessor')

    channel.queue_bind(
        exchange='preprocessor',
        queue='college-training-preprocessor'
    )

    channel.basic_publish(
        exchange='preprocessor',
        routing_key='college-training-preprocessor',
        body='set-colleges-to-redis'
    )

    channel.basic_consume(
        rabbitmq_callback,
        queue='college-training-preprocessor',
        no_ack=True
    )


    print('Consuming...')
    channel.start_consuming()

if __name__ == '__main__':
    main()
