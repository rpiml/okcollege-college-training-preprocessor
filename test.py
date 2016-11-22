import numpy as np
import pandas as pd
import io, csv, os
import redis
import pika

def initialize(college_file, cols_file):
	columnString = ""
	collegeString = ""
	with open(college_file) as csvfile:
		reader = csv.reader(csvfile)
		next(reader, None)
		for row in reader:
			collegeString += row[0] + "\n"

	with open(cols_file) as txtfile:
		line = txtfile.readline().strip()
		line = line.split(',')
		line.sort()
		for i in line:
			columnString += i + "\tnumerical\n"

	return collegeString, columnString

def testFeaturesCSV(column_string, r):
	output_features = r.get('learning:college_features.csv')
	#output_features_text = output_features.decode('utf-8').rstrip()
	#print output_features_text
	#print column_string
	assert output_features == column_string, 'Error with features.txt' 


def testCollegesCSV(college_string, r):
	output_colleges = r.get('learning:college_training.csv')
	#output_colleges_text = output_colleges.decode('utf-8').rstrip()

	assert output_colleges == college_string, 'Error with colleges.csv'


def Main():
	college_file = 'assets/out.csv'
	cols_file = 'assets/Column Labels.txt'
	r = redis.StrictRedis(host=os.environ['PG_HOST'])
	college_string, column_string = initialize(college_file, cols_file)

	testFeaturesCSV(column_string, r)
	testCollegesCSV(college_string, r)


if __name__ == '__main__':
	Main()