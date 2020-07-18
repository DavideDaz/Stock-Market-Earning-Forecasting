from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd
from datetime import datetime
import time 
from math import nan

class tabScrap():
    def getTable(self,title,driver,ticker,f,unit):

        #Scroll and push EPS button
        if not title:
            scrollTab = self.scrollTabFundamentals(driver)
        else:
            scrollTab = self.scrollTabEps(driver,title)

        #Last Page
        nPages = self.getNPages(scrollTab)

        #find table head
        head = self.getHead(scrollTab,f)

        data = pd.DataFrame(columns = head)

        # create dataFrame 
        date,value = [],[]

        for p in range(1,nPages+1):
            date,value = self.createColumns(scrollTab,date,value,p,unit)

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

    def scrollTabFundamentals(self,element):
        #Scroll to Tab
        scrollToTable = element.find_element_by_id("chart_table_container")
        scrollToTable.location_once_scrolled_into_view
        time.sleep(1)
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
        #pageButton = element.find_element_by_css_selector('.paginate_button[data-dt-idx="{}"]'.format(page))
        pageButtons = element.find_elements_by_css_selector('a.paginate_button')
        pageButtonL = [x for x in pageButtons if x.get_attribute('innerHTML') == page]
        pageButton = pageButtonL[0]
        if pageButton.is_displayed():
            pageButton.click()
            time.sleep(0.001)

    def createColumns(self,element,date,value,p,unit):
        self.turnPage(element,p)
        rows = element.find_element_by_tag_name('tbody')
        for r in rows.find_elements_by_tag_name('tr'):
            entries = r.find_elements_by_tag_name('td')
            date.append(datetime.strptime(entries[0].get_attribute('innerHTML'), '%m/%d/%Y'))
            val = entries[1].get_attribute('innerHTML')
            if val == 'N/A':
                value.append(nan)
            else:
                value.append(float(entries[1].get_attribute('innerHTML').replace(unit,'').replace(',','')))
        return (date,value)
    




        

        
    

