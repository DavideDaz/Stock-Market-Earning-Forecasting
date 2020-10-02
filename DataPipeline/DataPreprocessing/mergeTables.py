import pandas as pd
from datetime import datetime
from math import nan
from functools import reduce


class tabMerge():
    def __init__(self,truncatDateB = '2020-05-01' ):
        self.fundamnetalsPath = 'DataPipeline/WebScrapingZacks/FundamentalsHistorical/'
        self.mergedTablesPath = 'DataPipeline/DataPreprocessing/MergedTables/'
        self.epsPath = 'DataPipeline/WebScrapingZacks/epsHistorical/'
        self.stockPricesPath = 'DataPipeline/SPpricesYahooFinance/Stocks/'
        self.truncationDateBottom = truncatDateB

    def mergeTab(self,symbolsTab,fundamentalsTab):
        symbols = symbolsTab['Symbol']
        fundamentalsToMerge = fundamentalsTab['Mark']
        for s in symbols:
            # get quarter subdivision
            quarterMark = self.getQuarterSubdivision(s)

            # merge all fundamentals in one table 
            dataFrames = self.mergeFundamentals(s,fundamentalsToMerge,quarterMark)
            dfMerged = reduce(lambda  left,right: pd.merge(left,right,left_index=True,right_index=True,how='outer'), dataFrames)
            
            # aggregate eps surprise 
            epsSurprisedMerged = self.mergeEPS(dfMerged,s)
            
            # clean Prices table and calculate averages
            prices = self.pricesSMAandEMA(s)

            # create final table
            finalTable = self.mergePricesFundamentals(epsSurprisedMerged,prices,s)
            
            #export final table
            finalTable.to_csv(self.mergedTablesPath + '{}_merged.csv'.format(s))


    def getQuarterSubdivision(self,s):
        hashQuarterSub = {'[3,6,9,12]':'Q-DEC','[1,4,7,10]':'Q-JAN','[2,5,8,11]':'Q-FEB'}
        name = self.fundamnetalsPath + s + '/{}_eps-diluted-quarterly.csv'.format(s)
        df = pd.read_csv(name,index_col=[0])
        df['Date'] = df['Date'].astype('datetime64[ns]')
        quarterSubdivision = str(sorted(df['Date'].dt.month.unique().tolist())).replace(' ','')
        assert quarterSubdivision in hashQuarterSub.keys(), 'Anomalous Quarter subdivision: {}'.format(quarterSubdivision)
        quarterMark = hashQuarterSub[quarterSubdivision]
        return quarterMark

    def mergeFundamentals(self,s,fundamentalsToMerge,quarterMark):
        dataFrames = []
        for i in fundamentalsToMerge:
                name = self.fundamnetalsPath + s + '/' + '{}_{}.csv'.format(s,i)
                df = pd.read_csv(name,index_col=[0])
                df['Date'] = df['Date'].astype('datetime64[ns]')
                dfr = df.resample(quarterMark, convention='end',on='Date').agg('mean')
                dataFrames.append(dfr)
        return dataFrames


    def mergeEPS(self,dfMerged,s):
        name = self.epsPath + '/' + '{}_eps_surprise.csv'.format(s)
        epsData = pd.read_csv(name,index_col=[0])
        epsData['Date'] = epsData['Date'].astype('datetime64[ns]')
        epsData.sort_values(by='Date', inplace=True)
        epsData.set_index('Date', inplace=True)
        epsData = epsData[['Symbol','EPS Surprise']]
        epsData.drop(labels='Symbol',axis=1,inplace=True)
        dfMer = pd.merge_asof(dfMerged,epsData, left_index=True,right_index=True,direction='forward')
        return dfMer


    def pricesSMAandEMA(self,symbol):
        df = pd.read_csv(self.stockPricesPath + '{}_prices.csv'.format(symbol))
        df['ma90_Close'] = df['Close'].rolling(window=90).mean()
        df['ema_Close'] = df['Close'].ewm(com = 5,min_periods =90).mean()
        df['ma90_Volume'] = df['Volume'].rolling(window=90).mean()
        df['ema_Volume'] = df['Volume'].ewm(com = 5,min_periods =90).mean()
        df = df[['Date','Close','ma90_Close','ema_Close','ma90_Volume','ema_Volume']]
        return df

    def mergePricesFundamentals(self,fundamTable,pricesTable,s):
        assert len(pricesTable)>=len(fundamTable), 'Prices table {} has missing data'.format(s)
        pricesTable['Date'] = pricesTable['Date'].astype('datetime64[ns]')
        pricesTable.set_index('Date',inplace=True)
        mergedFundPrices = pd.merge_asof(fundamTable,pricesTable, left_index=True,right_index=True,direction='forward')
        mergedFundPrices['percent_return'] = mergedFundPrices['Close'].pct_change(periods=1)
        mergedFundPrices.drop('Close', axis = 1, inplace = True)
        firstIndex = self.findMaxNaNRow(mergedFundPrices,10) #Look for the first index with max 3 NaN in the row
        mergedFundPrices = mergedFundPrices[firstIndex:].interpolate(limit_direction = 'both').fillna(method='bfill')
        mergedFundPrices = mergedFundPrices.truncate(after = self.truncationDateBottom)
        return mergedFundPrices

    def findMaxNaNRow(self,df,n):
        i = 0
        while df.iloc[i].isnull().sum() > n:
            i += 1
        return i






        
        
