import numpy as np
import pandas as pd
import io, csv, os
import redis
import pika

def SAT1(score):
	'''
	Splits the SAT score range string into two real valued numbers
	'''
	if score != 'None':
		if '-' not in score:
			begin = score
		else:
			begin = score.split('-')[0]
		if int(begin) > 100:
			return begin
	
	return 'None'

def SAT2(score):
	'''
	Splits the SAT score range string into two real valued numbers
	'''
	if score != 'None':
		if '-' not in score:
			end = score
		else:
			end = score.split('-')[1]
		if int(end) > 100:
			return end
	
	return 'None'

def ACT1(score):
	'''
	Splits the ACT score range string into two real valued numbers
	'''
	if score != 'None':
		if '-' not in score:
			begin = score
		else:
			begin = score.split('-')[0]
		if int(begin) < 100:
			return begin

	return 'None'

def ACT2(score):
	'''
	Splits the ACT score range string into two real valued numbers
	'''
	if score != 'None':
		if '-' not in score:
			end = score
		else:
			end = score.split('-')[1]
		if int(end) < 100:
			return end

	return 'None'

def ratio(rat):
	'''
	Converts the student-faculty ratio string into a real valued number
	'''
	if rat != 'None':
		student = rat.split(':')[0]
		faculty = rat.split(':')[1]
		return float(student)/float(faculty)
	return 'None'

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
	df = pd.read_csv(college_file, sep='\t', header=None)
	df.columns = columns
	df['SAT1'] = df['SAT/ACT 25th-75th percentile'].apply(SAT1)
	df['SAT2'] = df['SAT/ACT 25th-75th percentile'].apply(SAT2)
	df['ACT1'] = df['SAT/ACT 25th-75th percentile'].apply(ACT1)
	df['ACT2'] = df['SAT/ACT 25th-75th percentile'].apply(ACT2)
	df['Student-faculty ratio'] = df['Student-faculty ratio'].apply(ratio)
	df.drop('SAT/ACT 25th-75th percentile', axis=1, inplace=True)
	df = df.sort_index(axis=1)
	df.to_csv('assets/out.csv', sep='\t', index=False)

def getcollegestring():
	'''
	Converts the colleges csv file into a string for redis
	'''
	college_string = ""
	with open('assets/out.csv') as csvfile:
		reader = csv.reader(csvfile)
		next(reader, None)
		for row in reader:
			college_string += row[0] + "\n"

	return college_string

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
	r = redis.StrictRedis(host=os.environ['PG_HOST'])
	r.set('learning:college_training.csv', college_string)
	r.set('learning:college_features.csv', column_string)

def main():
	'''
	Preprocesses the colleges data and ultimately adds it into redis
	'''
	college_file = 'assets/colleges.csv'
	cols_file = 'assets/Column Labels.txt'
	
	columns = parselabels(cols_file)
	parsecolleges(college_file, columns)
	college_string = getcollegestring()
	column_string = getfeaturestring(columns)
	setredis(column_string, college_string)



if __name__ == '__main__':
	main()