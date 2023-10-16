import pandas as pd 
import numpy as np 
import re
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

def main(): 
    funds = pd.read_csv("../../data/FundTick.csv")
    # funds = funds.iloc[1:10,]
    funds["link"] = funds.apply(lambda row: generate_link(row.ticker.lower(), row.not_index), axis = 1)

    model_dict = {"sustainability_rating": None, 
                    "global_category_count": None, 
                    "sustainable_investment": None, 
                    "hist_sustainability_score": None, "current_sus_score": None, "hist_avg": None,
                    "environmental_rating": None, 
                    "social_rating": None, 
                    "governance_rating": None, 
                    "unallocated_rating": None, 
                    "carbon_current": None, "carbon_low": None, "carbon_high": None, "carbon_average": None, 
                    "fossil_current": None, "fossil_low": None, "fossil_high": None, "fossil_avg": None}

    for key in model_dict.keys(): 
        funds[key] = None

    parser = ESG_Parser()
    success_message = "Successfully parsed ticker: {}, index: {}"
    failure_message = "Failed to parse ticker: {}, index: {}"

    for i in range(1, len(funds) + 1):
        if i % 20 == 0:
            # save progress every 20 iterations 
            save_slice = funds.iloc[:i - 1,]
            save_slice.to_csv("./backups/backup_fund_tick.csv")

        link = funds.loc[i].link
        try: 
            data = parser.get_data_from_webpage(weblink = link)
            structured_data = parser.parse_data(data)
            for key in structured_data.keys(): 
                funds.loc[i, key] = structured_data[key]
            
            print(success_message.format(funds.loc[i, "ticker"], i))
        except KeyboardInterrupt: 
            sys.exit()
            pass
        except: 
            print(failure_message.format(funds.loc[i, "ticker"], i))
            continue
    
    parser.close_parser()
    funds.to_csv("./FundTick.csv")

def generate_link (fund_name, is_not_index): 
    """
    Generates the link for the fund page on morningstar.com fromt the two links 
    depending on whether the fund is an index or not.
    """
    index_link = "https://www.morningstar.com/etfs/arcx/{}/portfolio"
    not_index_link = "https://www.morningstar.com/funds/xnas/{}/portfolio"

    if is_not_index: 
        return not_index_link.format(fund_name)
    
    return index_link.format(fund_name)


class ESG_Parser: 
    def __init__(self): 
        self.driver = self.start_selenium()
        
    def start_selenium(self): 
        """
        Starts new selenium session, allowing for multiple web requests in a single session 
        saving startup time when parsing new website. 
        """
        driver = webdriver.Chrome("./chromedriver_mac_arm64/chromedriver")
        return driver 
    
    def link_is_broken(self, weblink): 
        """
        Morningstar still accepts non-findable webpages without an error which means we must 
        manually check to see if a funds website exists. 
        """
        try: 
            self.driver.find_element(By.CLASS_NAME, "error")
            return True 
        except NoSuchElementException: 
            return False
        
    def get_data_from_webpage(self, weblink): 
        """
        Takes in a weblink for morningstar and extracts the ESG data from it. 
        Two methods are used finding all the "sr-only" tags which contain data for 
        this portion of the website, and loking at the sustainbility dp-value (only for
        number of funds in category). 
        """
        driver = self.driver

        driver.get(weblink)
        sleep(2) 

        if self.link_is_broken(weblink): 
            return None 

        info1 = []
        for item in driver.find_elements(By.CLASS_NAME, "sal-sustainability__dp-value"): 
            info1.append(item.text)

        info2 = []
        for item in driver.find_elements(By.CLASS_NAME,"sr-only"): 
            info2.append(item.text)

        return [info1, info2]
    
    def parse_data(self, data): 
        """
        Pass in data from the get_data_from_webpage() function, and if the 
        data exists a dictionary with the relevant information will be returned. 
        """
        esg_dict = {"sustainability_rating": None, 
                    "global_category_count": None, 
                    "sustainable_investment": None, 
                    "hist_sustainability_score": None, "current_sus_score": None, "hist_avg": None,
                    "environmental_rating": None, 
                    "social_rating": None, 
                    "governance_rating": None, 
                    "unallocated_rating": None, 
                    "carbon_current": None, "carbon_low": None, "carbon_high": None, "carbon_average": None, 
                    "fossil_current": None, "fossil_low": None, "fossil_high": None, "fossil_avg": None}
        
        #make sure data exists (morningstar doesn't list every fund)
        if not data: 
            return esg_dict 
        
        def extract_data_from_series(series, regex, dtype = "float"): 
            """
            Extracts a single data point from a series of strings. Assumes only one match 
            will exist, so only first observation will be returned. 
            """
            return (series.loc[series.str.match(regex, case = False)]
                    .str.extract(regex)
                    .values[0].astype(dtype))
        
        global_score = pd.Series(data[0])
        other_scores = pd.Series(data[1])
        #make sure sustainbility data exists (not every fund has it)
        
        
        globe_rating_regex = r"Rating ([0-5]) Out of 5"
        try: 
            esg_dict["sustainability_rating"] = extract_data_from_series(other_scores, globe_rating_regex, "int")[0]
        except IndexError: 
            return esg_dict 
        
        #first series conatains information about global_category count and sustainable investment indicator (yes/no)
        yes_no_regex = r"no|yes"
        sustainable_investment_loc = global_score.loc[global_score.str.match(yes_no_regex, case = False)]
        esg_dict["sustainable_investment"] = sustainable_investment_loc.values[0]
        
        #gobal_count is always one before the sustainble_investment (y/n) category
        esg_dict["global_category_count"] = int(global_score.loc[sustainable_investment_loc.index - 1].values[0])

        #second series contains all other information
        
        environmental_rating_regex = r"Environmental ([0-9]{1,2}\.[0-9]{1,2})"
        social_rating_regex = r"Social ([0-9]{1,2}\.[0-9]{2})"
        governance_rating_regex = r"Governance ([0-9]{1,2}\.[0-9]{1,2})"
        unallocated_rating_regex = r"Unallocated ([0-9]{1,2}\.[0-9]{1,2})"

        sustainibility_score_regex = r"Historical score ([0-9]{1,2}\.[0-9]{1,2}) Out of Fifty, Current Score ([0-9]{1,2}\.[0-9]{1,2}) Out of Fifty, Historical Average ([0-9]{2}\.[0-9]{1,2}) Out of Fifty"
        carbon_risk_score_regex = r"Carbon Risk Score, ([0-9]{1,2}\.[0-9]{1,2})? ?Out Of Hundred\. Carbon Risk Score Category Low, ([0-9]{1,2}\.[0-9]{1,2}) Out Of Hundred\. Carbon Risk Score Category High, ([0-9]{1,2}\.[0-9]{1,2}) Out Of Hundred\. Carbon Risk Score Category Average, ([0-9]{1,2}\.[0-9]{1,2}) Out Of Hundred\."
        fossil_fuel_involvement_regex = r"Fossil Fuel Involvement %, ([0-9]{1,2}\.[0-9]{1,2})? ?Out Of Hundred\. Fossil Fuel Involvement % Category Low, ([0-9]{1,2}\.[0-9]{1,2}) Out Of Hundred\. Fossil Fuel Involvement % Category High, ([0-9]{1,3}\.[0-9]{1,2}) Out Of Hundred. Fossil Fuel Involvement % Category Average, ([0-9]{1,2}\.[0-9]{1,2}) Out Of Hundred."


        #extract ratings from series using regex's defined above
        try: 
            esg_dict["sustainability_rating"] = extract_data_from_series(other_scores, globe_rating_regex, "int")[0]
        except IndexError: 
            return esg_dict 
        
        esg_dict["hist_sustainability_score"], esg_dict["current_sus_score"], esg_dict["hist_avg"] = extract_data_from_series(other_scores, sustainibility_score_regex)
        esg_dict["environmental_rating"] = extract_data_from_series(other_scores, environmental_rating_regex)[0]
        esg_dict["social_rating"] = extract_data_from_series(other_scores, social_rating_regex)[0]
        esg_dict["governance_rating"] = extract_data_from_series(other_scores, governance_rating_regex)[0]
        esg_dict["unallocated_rating"] = extract_data_from_series(other_scores, unallocated_rating_regex)[0]
        esg_dict["carbon_current"], esg_dict["carbon_low"], esg_dict["carbon_high"], esg_dict["carbon_average"] = extract_data_from_series(other_scores, carbon_risk_score_regex)
        esg_dict["fossil_current"], esg_dict["fossil_low"], esg_dict["fossil_high"], esg_dict["fossil_avg"] = extract_data_from_series(other_scores, fossil_fuel_involvement_regex)

        return esg_dict
    
    def close_parser(self): 
        """
        Ends Selenium session, and shuts down the parser. 
        Parser will not function properly after a call to close_parser, please 
        start a new parser, and don't call this method until parsing is complete. 
        """
        self.driver.close()
#End of class

if __name__ == "__main__":
    main()