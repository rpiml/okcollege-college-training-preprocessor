import numpy as np
import pandas as pd
import io, csv, os
import redis
import pika

def SAT1(score):
	if score != 'None':
		if '-' not in score:
			begin = score
		else:
			begin = score.split('-')[0]
		if int(begin) > 100:
			return begin
	
	return 'None'

def SAT2(score):
	if score != 'None':
		if '-' not in score:
			end = score
		else:
			end = score.split('-')[1]
		if int(end) > 100:
			return end
	
	return 'None'

def ACT1(score):
	if score != 'None':
		if '-' not in score:
			begin = score
		else:
			begin = score.split('-')[0]
		if int(begin) < 100:
			return begin

	return 'None'

def ACT2(score):
	if score != 'None':
		if '-' not in score:
			end = score
		else:
			end = score.split('-')[1]
		if int(end) < 100:
			return end

	return 'None'

def ratio(rat):
	if rat != 'None':
		student = rat.split(':')[0]
		faculty = rat.split(':')[1]
		return float(student)/float(faculty)
	return 'None'

# csv file is located on the desktop
csv_file = 'assets/colleges.csv'
col_file = 'assets/Column Labels.txt'

columns = []
with open(col_file) as f:
	for item in f:
		columns = item.split(',')
df = pd.read_csv(csv_file, sep='\t', header=None)
df.columns = columns
#print df['SAT/ACT 25th-75th percentile']
df['SAT1'] = df['SAT/ACT 25th-75th percentile'].apply(SAT1)
df['SAT2'] = df['SAT/ACT 25th-75th percentile'].apply(SAT2)
df['ACT1'] = df['SAT/ACT 25th-75th percentile'].apply(ACT1)
df['ACT2'] = df['SAT/ACT 25th-75th percentile'].apply(ACT2)
df['Student-faculty ratio'] = df['Student-faculty ratio'].apply(ratio)
df.drop('SAT/ACT 25th-75th percentile', axis=1, inplace=True)
df = df.sort_index(axis=1)
columns.sort()
df.to_csv('out.csv', sep='\t', index=False)

columnString = ""
collegeString = ""
with open('out.csv') as csvfile:
	reader = csv.reader(csvfile)
	for row in reader:
		collegeString += row[0] + "\n"
#print collegeString

for i in columns:
	columnString += i + "\tnumerical\n"
#print columnString

r = redis.StrictRedis(host=os.environ['PG_HOST'])
r.set('learning:college_training.csv', collegeString)
r.set('learning:college_features.csv', columnString)
#print df2