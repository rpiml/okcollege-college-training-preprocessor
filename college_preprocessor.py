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

def category_yn(value):
	'''
	Cells with yes/no values are changed to 1/0
	yes = 1
	no = 0
	'''
	if isinstance(value, basestring):
		if value == 'Yes':
			return 1
		if value == 'No':
			return 0
	return value

def category_enlist(value):
	'''
	Category parsing for army/navy/airforce
	not offered = 0
	offered on campus = 1
	offered at other institute = 2
	'''
	if isinstance(value, basestring):
		if 'Offered on campus' in value:
			return 1
		if 'Offered at cooperating institution' in value:
			return 2
	return 0

def category_tests(value):
	'''
	Category parsing for standardized tests
	No tests required = 0
	SAT only = 1
	ACT only = 2
	EIther SAT or ACT = 3
	'''
	if isinstance(value, basestring):
		if value == 'Neither SAT nor ACT':
			return 0
		if value == 'SAT':
			return 1
		if value == 'ACT':
			return 2
		if value == 'Either SAT or ACT':
			return 3
	return 0

def category_selectivity(value):
	'''
	Category parsing for selectivity
	Least selective = 0
	Less selective = 1
	Selective = 2
	More selective = 3
	Most selective = 4
	'''
	if isinstance(value, basestring):
		if value == 'Least selective':
			return 0
		if value == 'Less selective':
			return 1
		if value == 'Selective':
			return 2
		if value == 'More selective':
			return 3
		if value == 'Most selective':
			return 4
	return 0

def parselabels(cols_file):
    '''
    Parses the features csv file into an array and returns it
    '''
    columns = []
    with open(cols_file, 'a+') as f:
    	reader = csv.reader(f)
    	for row in reader:
    		columns.append(row[0])
    
    columns.sort()
    
    return columns

def parsecolleges(college_file, columns):
    '''
    Parses the colleges csv file into a pandas dataframe and
    adjusts SAT/ACT & student/faculty ratio to contain
    real valued numbers for ML algorithms
    '''

    # read the csv
    df = pd.read_csv(college_file, header=None)
    df.columns = columns

    # change all 'N/A' values to 0
    df.fillna(0, inplace=True)

    # category for yes/no
    df = df.applymap(category_yn)

    # fix the small issue where extra unecessary info is in cells
    df = df.applymap(separate_parens)

    # strip cells of $,% to include only numbers or categories
    df = df.applymap(strip_chars)

    # split the SAT/ACT range into two cells with lower/upper values
    df['SAT-lower-percentile'] = df['SAT/ACT 25th-75th percentile'].apply(SAT_begin)
    df['SAT-upper-percentile'] = df['SAT/ACT 25th-75th percentile'].apply(SAT_end)
    df['ACT-lower-percentile'] = df['SAT/ACT 25th-75th percentile'].apply(ACT_begin)
    df['ACT-upper-percentile'] = df['SAT/ACT 25th-75th percentile'].apply(ACT_end)
    df['Student-faculty-ratio'] = df['Student-faculty ratio'].apply(ratio)
    df['Air-Force-ROTC'] = df['Air-Force-ROTC'].apply(category_enlist)
    df['Army-ROTC'] = df['Army-ROTC'].apply(category_enlist)
    df['Navy-ROTC'] = df['Nacy-ROTC'].apply(category_enlist)
    df['Required-standardized-tests'] = df['Required-standardized-tests'].apply(category_tests)
    df['Selectivity'] = df['Selectivity'].apply(category_selectivity)
    df.drop('SAT/ACT 25th-75th percentile', axis=1, inplace=True)
    df = df.sort_index(axis=1)

    csv_file_obj = StringIO.StringIO()

    df.to_csv(csv_file_obj, index=False)

    csv_file_obj.seek(0)
    return csv_file_obj

def getfeaturestring(cols_file):
    '''
    Converts the columns txt file into a string for redis
    '''
    feature_list = []
    with open(cols_file, 'a+') as f:
    	reader = csv.reader(f)
    	for row in reader:
    		feature_list.append(row)

    # add the new columns we created from SAT/ACT range
    feature_list.append(['SAT-lower-percentile', 'numerical'])
    feature_list.append(['SAT-upper-percentile', 'numerical'])
    feature_list.append(['ACT-lower-percentile', 'numerical'])
    feature_list.append(['ACT-upper-percentile', 'numerical'])
    feature_list.remove(['SAT/ACT-25th-75th-percentile', 'ranking'])
    feature_list.sort()
    column_string = "attribute_name,type,categories\n"

    for feature in feature_list:
    	for j in feature:
        	column_string += j + ','
        column_string = column_string.strip(',')
        column_string += '\n'
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
        csv_file_obj = parsecolleges(college_file, columns, id_to_name)
        college_string = csv_file_obj.read()
        column_string = getfeaturestring(cols_file)
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
