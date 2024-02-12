# Databricks notebook source
# MAGIC %pip install lxml

# COMMAND ----------

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import lxml
import time

# COMMAND ----------

# MAGIC %md
# MAGIC #Test the page extraction function

# COMMAND ----------

# search for searchterm and salary to get the number of pages there are for each search
# url = 'https://www.seek.co.nz/jobs-in-'+job_category_list[0]+'/in-All-Auckland'
# res = requests.get(url)
# soup = BeautifulSoup(res.content, features='html.parser')
# number_of_jobs = int(soup.find('span', {'data-automation':'totalJobsCount'}).text.replace(',',''))
# number_of_pages = number_of_jobs // 22 + (number_of_jobs % 22 > 0)


# COMMAND ----------

# MAGIC %md
# MAGIC #Scrape based on job categories

# COMMAND ----------

job_category_list=['human-resources-recruitment','manufacturing-transport-logistics','engineering','construction','accounting','administration-office-support']

job_title = []
job_link = []
company_name = []
short_description = []
job_category = []
job_location = []

# Initialize job_counter if it's not already defined
job_counter = 0
for category in job_category_list:
    # search for searchterm and salary to get the number of pages there are for each search
    url = 'https://www.seek.co.nz/jobs-in-'+category+'/in-All-Auckland'
    res = requests.get(url)
    soup = BeautifulSoup(res.content, features='html.parser')
    number_of_jobs = int(soup.find('span', {'data-automation':'totalJobsCount'}).text.replace(',',''))
    number_of_pages = number_of_jobs // 22 + (number_of_jobs % 22 > 0)

    for number in np.arange(1,number_of_pages):
        url = 'https://www.seek.co.nz/jobs-in-'+category+'/in-All-Auckland?page='+ str(number)
        print(url)
        res = requests.get(url)
        soup = BeautifulSoup(res.content, features='html.parser')
        jobs_on_page = soup.find_all('article')
        for job in jobs_on_page:
            
            job_counter += 1
            print('Jobs scraped: %d' % job_counter, end='\r')

            # Extracting job title from the 'aria-label' attribute of the article tag
            job_title.append(job['aria-label'])

            # Extracting job link. The link is within an <a> tag inside an <h3> tag with data-automation attribute 'jobTitle'
            job_link_element = job.find('a', {'data-automation': 'jobTitle'})
            if job_link_element:
                job_link.append(job_link_element['href'])
            else:
                job_link.append(None)  # Append None if no link is found

            # Extracting company name. The company name is within an <a> tag with data-automation attribute 'jobCompany'
            company_name_element = job.find('a', {'data-automation': 'jobCompany'})
            if company_name_element:
                company_name.append(company_name_element.text)
            else:
                company_name.append(None)  # Append None if no company name is found

            # Extracting short description from a <span> tag with data-automation attribute 'jobShortDescription'
            short_description_element = job.find('span', {'data-automation': 'jobShortDescription'})
            if short_description_element:
                short_description.append(short_description_element.text)
            else:
                short_description.append(None)  # Append None if no short description is found

            # Extracting job location from an <a> tag with data-automation attribute 'jobLocation'
            job_location_element = job.find('a', {'data-automation': 'jobLocation'})
            if job_location_element:
                job_location.append(job_location_element.text)
            else:
                job_location.append(None)  # Append None if no location is found
            job_category.append(category)



# COMMAND ----------

pre_data = {'job_title': job_title,
            'job_link': job_link, 
            'company_name': company_name,
            'job_category':job_category,
            'short_description': short_description,
            'job_location': job_location,
            }
pre_data_df = pd.DataFrame(pre_data)
pre_data_df

# COMMAND ----------

# Save pandas dataframe to dbfs in the form of csv
pre_data_df.to_csv('/dbfs/FileStore/auckland_job_selected_categories.csv')


# COMMAND ----------

# DBTITLE 1,Display dataset for downloading
display(pre_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Scrape based on the salary range

# COMMAND ----------

# https://www.seek.com.au/data-scientist-jobs?page=2&salaryrange=30000-50000&salarytype=annual

# input values for search
salary_ranges = [
                # '0-30000', 
                #  '30000-40000', 
                #  '40000-50000', 
                #  '50000-60000', 
                #  '60000-70000',
                 '70000-80000'
                #  '80000-100000',
                #  '100000-120000',
                # '120000-150000',
                # '150000-200000',
                # '200000-999999'
                ]

search_terms = ['data-scientist', 
                # 'data-engineer', 
                # 'data-analyst', 
                # 'data-warehousing', 
                # 'big-data', 
                # 'business-analyst', 
                # 'data'
                ]


# data i want to scrape
job_title = []
job_link = []
salary_range = []
search_term = []
company_name = []
short_description = []
job_area = []
salary_amount_searchpage=[]
job_location = []

job_counter = 0

# search every search term above for every salary range above
for term in search_terms:
    
    for salary in salary_ranges:
        
        # search for searchterm and salary to get the number of pages there are for each search
        url = 'https://www.seek.com.au/'+ term + '-jobs?salaryrange='+ salary + 'salarytype=annual'
        print(url)
        res = requests.get(url)
        soup = BeautifulSoup(res.content, features='html.parser')

        # try and except in case of NoneType error when there are zero search results
        try:
            number_of_jobs = int(soup.find('span', {'data-automation':'totalJobsCount'}).text)
        except:
            number_of_jobs = 0
            
        # find number of pages by dividing number of jobs by 22 (jobs per page)
        number_of_pages = number_of_jobs // 22 + (number_of_jobs % 22 > 0)
        
        # for each page in search, find job titles     
        for number in np.arange(1,number_of_pages):
            url = 'https://www.seek.com.au/'+ term + '-jobs?page='+ str(number)+ '&salaryrange='+ salary + '&salarytype=annual'
            res = requests.get(url)
            soup = BeautifulSoup(res.content, features='html.parser')
            
            jobs_on_page = soup('article')
            
            # for each job on the page, find job title, link, and attach salary range and search term info to the job
            for i in range(len(jobs_on_page)):
                
                job_counter += 1
                
                print('Jobs scraped: %d' % job_counter, end='\r')
                
                job_title.append(soup('article')[i]['aria-label'])
                job_link.append(soup('article')[i].h1.a['href'])
                company_name.append(soup('article')[i]('a', {'target': '_self'})[1].text)
                short_description.append(soup('article')[i].find('span', {'data-automation':'jobShortDescription'}).text)
                job_location.append(soup('article')[i].find('a', {'data-automation':'jobLocation'}).text)
                salary_range.append(salary)
                search_term.append(term)
                
                try:
                    salary_amount_searchpage.append(soup('article')[i].find_all('span', {'data-automation':'jobSalary'})[0].text)
                except:
                    print('nothing found')
                    salary_amount_searchpage.append(np.nan)
                try:
                    job_area.append(soup('article')[i].find('a', {'data-automation': 'jobArea'}).text)
                except:
                    print('nothing found')
                    job_area.append(np.nan)
                
pre_data = {'job_title': job_title,
            'job_link': job_link, 
            'salary_range': salary_range, 
            'search_term': search_term,
            'company_name': company_name,
            'short_description': short_description,
            'job_location': job_location,
            'job_area': job_area,
            'salary_amount_searchpage': salary_amount_searchpage}



# COMMAND ----------

pre_data_df = pd.DataFrame(pre_data)
pre_data_df
