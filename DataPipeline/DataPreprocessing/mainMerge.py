import pandas as pd
import os
import sys
from sklearn.preprocessing import LabelEncoder
import mergeTables

if __name__ == "__main__":

    wd = os.getcwd()

    if not os.path.exists(wd + '/mergedTables'):
        os.mkdir(wd + '/mergedTables')

    tickersData = pd.read_csv(wd+'/../WebScrapingZacks/docs/Symbols.csv')
    fundamentals = pd.read_csv(wd+'/../WebScrapingZacks/docs/FundamentalsList.csv')
    mergeT = mergeTables.tabMerge()
    mergeT.mergeTab(wd,tickersData,fundamentals)
