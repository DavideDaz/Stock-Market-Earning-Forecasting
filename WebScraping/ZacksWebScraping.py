import selenium
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
import os
import pandas as pd
from datetime import datetime
import time 
from math import nan

class tabScrap():
    def getTable(self,title,driver,ticker,f,frequency):

        #Scroll web page
        if not title:
            scrollTab = self.scrollTabFundamentals(driver,frequency)
        else:
            scrollTab = self.scrollTabEps(driver,title)

        #Get last Page
        nPages = self.getNPages(scrollTab)

        #Get table head
        head = self.getHead(scrollTab,f)

        data = pd.DataFrame(columns = head)

        #Create dataFrame 
        date,value = [],[]

        for p in range(1,nPages+1):
            date,value = self.createColumns(scrollTab,date,value,p)

        lDate,lValue = len(date),len(value)

        assert lDate == lValue, 'The Date and Value columns have different length.'

        data[head[0]] = [ticker for x in range(lDate)]
        data[head[1]] = date
        data[head[2]] = value

        return data

    def scrollTabEps(self,element,title):
        scrollTab = element.find_element_by_id('chart_table_container')
        epsScroll = element.find_element_by_id('chart_ad_container')
        epsButton = scrollTab.find_element_by_id('ui-id-6')
        epsScroll.location_once_scrolled_into_view
        time.sleep(1)
        epsButton.click()
        time.sleep(1)
        cont = element.find_element_by_id(title)
        return cont

    def scrollTabFundamentals(self,element,frequency):
        if frequency =='other':
            adScroll = element.find_element_by_id('chart_ad_container')
            adScroll.location_once_scrolled_into_view
            monthTab = element.find_element_by_css_selector('a.ui-tabs-anchor[id="ui-id-5"]')
            monthTab.click()
            scrollToTable = element.find_element_by_id('DataTables_Table_2_wrapper')
        else:
            scrollToTable = element.find_element_by_id("chart_table_container")
        scrollToTable.location_once_scrolled_into_view
        return scrollToTable

    def getNPages(self,element):
        lastPage = element.find_element_by_tag_name('span')
        nPages = int(lastPage.find_elements_by_tag_name('a')[-1].get_attribute('innerHTML'))
        return nPages

    def getHead(self,element,f):
        headEl = element.find_element_by_tag_name('thead').find_elements_by_tag_name('th')
        hNames = headEl[0].get_attribute('innerHTML')
        hValues = headEl[1].get_attribute('innerHTML')
        head = ['Symbol','Date',f]
        return head

    def turnPage(self,element,page):
        page = str(page)
        pageButtons = element.find_elements_by_css_selector('a.paginate_button')
        pageButtonL = [x for x in pageButtons if x.get_attribute('innerHTML') == page]
        pageButton = pageButtonL[0]
        if pageButton.is_displayed():
            pageButton.click()
            #time.sleep(0.001)

    def createColumns(self,element,date,value,p):
        self.turnPage(element,p)
        rows = element.find_element_by_tag_name('tbody')
        for r in rows.find_elements_by_tag_name('tr'):
            entries = r.find_elements_by_tag_name('td')
            date.append(datetime.strptime(entries[0].get_attribute('innerHTML'), '%m/%d/%Y'))
            val = entries[1].get_attribute('innerHTML')
            if val == 'N/A':
                value.append(nan)
            else:
                value.append(float(entries[1].get_attribute('innerHTML').replace('$','').replace('%','').replace(',','')))
        return (date,value)

    def epsScraping(self,tickers,wd):
        tickerErrorEPS = []
        for t in tickers:
            try:
                opts = Options()
                opts.headless = True
                driver = webdriver.Firefox(options=opts)
                driver.get("https://www.zacks.com/stock/chart/{}/price-consensus-eps-surprise-chart".format(t))

                title  = 'chart_wrapper_datatable_eps_surprise'
                
                data = self.getTable(title,driver,t,'EPS Surprise',None)

                data.to_csv(wd + '/epsHistorical/{}_eps_surprise.csv'.format(t), na_rep='NaN')

                driver.close()

            except selenium.common.exceptions.NoSuchElementException:
                tickerErrorEPS.append(t)
                print('Ticker Error {}: EPS Table not available'.format(t))
                driver.close()
            except Exception as e:
                tickerErrorEPS.append(t)
                print('Error during {tk} EPS Scraping! Code: {c}, Message, {m}'.format(tk = t, c = type(e).__name__, m = str(e)))
                tickerErrorEPS.append(str(e))
                driver.close()

        #Save failed queries to a text file to retry
        if len(tickerErrorEPS) > 0:
            writepath = wd+'/docs/failed_queries_EPS.txt'
            mode = 'a' if os.path.exists(writepath) else 'w'
            with open(writepath, mode) as outfile:
                for name in tickerErrorEPS:
                    outfile.write(name + '\n')
    
    def fundamentalsScraping(self,tickers,fundamentalsList,wd,fixError):
        writepathFund = wd+'/docs/failed_queries_Fundamentals.txt'
        for t in tickers:
                for f,freq in fundamentalsList:
                    try:
                        opts = Options()
                        opts.headless = True
                        driver = webdriver.Firefox(options=opts)
                        driver.get("https://www.zacks.com/stock/chart/{}/fundamental/{}".format(t,f))
                        
                        data = self.getTable(None,driver,t,f,freq)

                        if not os.path.isdir(wd + '/FundamentalsHistorical/' + t):
                            os.mkdir(wd + '/FundamentalsHistorical/' + t)
                            
                        data.to_csv(wd + '/FundamentalsHistorical/' + t + '/{}_{}.csv'.format(t,f), na_rep='NaN')
                        driver.close()

                    except selenium.common.exceptions.NoSuchElementException:
                        self.writeError(fixError,writepathFund,t,f)
                        print('Ticker Error {}: Fundamentals Table {} not available'.format(t,f))
                        driver.close()
                    except Exception as e:
                        self.writeError(fixError,writepathFund,t,f)
                        print('Error during {tk} Fundamental Scraping of {ft}! Code: {c}, Message, {m}'.format(tk = t,ft = f, c = type(e).__name__, m = str(e)))
                        driver.close()

        #Save failed queries to a text file to retry
    def writeError(self,fixErr,writepath,name,mark):
            if not fixErr:
                mode = 'a' if os.path.exists(writepath) else 'w'
                with open(writepath, mode) as outfile:
                    outfile.write(name + ' ' + mark + '\n')

    def fixErrorTickers(self,file,df,wd):
        errorFile = open(file, "r")
        lines = errorFile.readlines()
        errorFile.close()

        for l in lines:
            symbol,feature = l.split()
            frequency = df.loc[df['Mark'] == feature, 'TimeFrequency'].iloc[0]
            symbolList,featureList = [symbol],[(feature,frequency)]
            self.fundamentalsScraping(symbolList,featureList,wd,True)            
            p = wd + '/FundamentalsHistorical/' + '{}/{}_{}.csv'.format(symbol,symbol,feature)
            if os.path.exists(p):
                with open(file, "r") as f:
                    lines = f.readlines()
                with open(file, "w") as f:
                    for line in lines:
                        if line.strip("\n") != l.strip("\n"):
                            f.write(line)   


        

        
    

