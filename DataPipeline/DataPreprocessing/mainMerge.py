import pandas as pd
import os
import sys
import mergeTables

if __name__ == "__main__":

    ROOT_DIR = '/Users/davideconcu/Documents/Stock Analysis/DataPipeline/DataPreprocessing'

    if not os.path.exists(ROOT_DIR + '/MergedTables'):
        os.mkdir(ROOT_DIR + '/MergedTables')

    tickersData = pd.read_csv(ROOT_DIR+'/../WebScrapingZacks/docs/Symbols.csv')
    fundamentals = pd.read_csv(ROOT_DIR+'/../WebScrapingZacks/docs/FundamentalsList.csv')
    mergeT = mergeTables.tabMerge()
    mergeT.mergeTab(tickersData,fundamentals)
