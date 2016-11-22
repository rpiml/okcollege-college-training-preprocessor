import numpy as np
import pandas as pd
import io, csv, os
import redis
import pika

def initialize(college_file, cols_file):
	'''
	Converts the txt and csv files into strings in order to compare
	with the strings in the redis
	'''
	column_string = ""
	college_string = ""
	with open(college_file) as csvfile:
		reader = csv.reader(csvfile)
		next(reader, None)
		for row in reader:
			college_string += row[0] + "\n"

	with open(cols_file) as txtfile:
		line = txtfile.readline().strip()
		line = line.split(',')
		line.sort()
		for i in line:
			column_string += i + "\tnumerical\n"

	return college_string, column_string

def testfeaturescsv(column_string, r):
	'''
	Compares the created features string with the one in redis
	'''
	output_features = r.get('learning:college_features.csv')
	assert output_features == column_string, 'Error with features.txt' 


def testcollegescsv(college_string, r):
	'''
	Compares the created colleges string with the one in redis
	'''
	output_colleges = r.get('learning:college_training.csv')
	assert output_colleges == college_string, 'Error with colleges.csv'


def main():
	'''
	Test file that ensures the strings that were added into redis
	are complete/correct
	'''
	college_file = 'assets/out.csv'
	cols_file = 'assets/Column Labels.txt'
	r = redis.StrictRedis(host=os.environ['PG_HOST'])
	college_string, column_string = initialize(college_file, cols_file)

	testfeaturescsv(column_string, r)
	testcollegescsv(college_string, r)


if __name__ == '__main__':
	main()