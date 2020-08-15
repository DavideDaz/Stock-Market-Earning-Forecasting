from datetime import datetime
from concurrent import futures

import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web
import os

def download_stock(stock):
	try:
		print(stock)
		stock_df = web.DataReader(stock,'yahoo', start_time, now_time)
		stock_df['Name'] = stock
		output_name = stock + '_prices.csv'
		stock_df.to_csv("./Stocks/"+output_name)
	except:
		bad_names.append(stock)
		print('bad: %s' % (stock))

if __name__ == '__main__':
	errorFix = False
	symbolsFile = '/Users/davideconcu/Documents/Stock Analysis/DataPipeline/WebScrapingZacks/docs/Symbols.csv'
	stockFolder = '/Stocks'
	wd = os.getcwd()

	if not os.path.exists(wd + stockFolder):
		os.mkdir(wd + stockFolder)

	""" set the download window """
	now_time = datetime.now()
	start_time = datetime(1995, 1 , 1)

	""" list of s_anp_p companies """
	symbols = pd.read_csv(symbolsFile)
	s_and_p = list(symbols['Symbol'])
	s_and_p = ['A']

	bad_names =[] #to keep track of failed queries

	"""here we use the concurrent.futures module's ThreadPoolExecutor
		to speed up the downloads buy doing them in parallel 
		as opposed to sequentially """

	#set the maximum thread number
	max_workers = 50
	if not errorFix:
		workers = min(max_workers, len(s_and_p)) #in case a smaller number of stocks than threads was passed in
		with futures.ThreadPoolExecutor(workers) as executor:
			res = executor.map(download_stock, s_and_p)

		""" Save failed queries to a text file to retry """
		if len(bad_names) > 0:
			with open('failed_queries.txt','w') as outfile:
				for name in bad_names:
					outfile.write(name+'\n')
	else:
		errorfile = './failed_queries.txt'
		errorFile = open(errorfile, "r")
		s_and_p = errorFile.readlines()
		for s in s_and_p:
			s = s.strip()
			download_stock(s)

			p = wd + '/Stocks/' + '{}_prices.csv'.format(s)
			if os.path.exists(p):
				with open(errorfile, "r") as f:
					lines = f.readlines()
				with open(errorfile, "w") as f:
					for line in lines:
						if line.strip("\n") != s:
							f.write(line) 

	