"""
Mehran Ali Banka - Sep 2023
----------------------------
This is a main class to create a sampling of the web using a crawler.
Various parameters have been provided to tune this code. 
Run crawler_main.py -help to get sample execution commands

"""
import heapq
import requests
from urllib import robotparser
import time
import Parameters as params
import seed_loader
from bs4 import BeautifulSoup
import os
import socket
from langdetect import detect
import Logger
from urllib.parse import urlparse
import math
import random
import re
import atexit
import logging
import sys

# Record the start of the crawler
time_start = time.time()

# To keep track of visited nodes in memory
visited_nodes = set()

# To keep track of languages already seen
seen_languages = {}

# To keep track of geographies already seen
seen_geographies = {}

# To keep a track of visited domains, and their count, in memory
domain_frequency = {}

# To keep track of sites that may have been affected by crawling
do_not_visit_list = set()

# The index of the subfolder where the current file is downloading.
# Max no of files under a subfolder is controlled by the 
# max_files_downloaded_in_same_path parameter
current_folder_idx = -1
current_folder_count = 0

# Keep track of pages sampled
pages_sampled = 0

# Keep track of pages explored, useful for
# stats as well as PQ weight function
pages_explored = 0

def dump_summary_stats():     
    time_end = time.time()
    elapsed_time = time_end - time_start
    logger.info("Stopping Crawler ...")
    logger.info(f"Crawling Runtime: {elapsed_time:.6f} seconds")
    logger.end_section()
 
    logger.info("---------------- Crawler Statistics: -----------------")
    logger.info("Pages Visited : {} , Pages Sampled : {}.".format(pages_explored, pages_sampled))
    logger.info("Unique Languages : {} , Unique Countries: {} , Unique Domains: {}.".format(len(seen_languages), len(seen_geographies), len(domain_frequency)))
    logger.info("Language Details: ")
    logger.info("-----------------")
    for key, value in seen_languages.items():
        logger.info(f"Language: {key}, Count: {value}")
    logger.info("Country Details: ")
    logger.info("-----------------")
    for key, value in seen_geographies.items():
        logger.info(f"Country: {key}, Count: {value}") 

# register the stats summary method, so information
# isnt lost on any program termination
atexit.register(dump_summary_stats)

# Get initial list of seeds based on the seeding strategy
seed_list = seed_loader.get_seeder_list(params.seed_file_path,params.seeding_strategy)


# check if we want to log all files explored
sec_logger_name = 'Python-Web-Crawl-all-explored-urls.log'
sec_log_path = params.log_file_path + "/" + sec_logger_name


# Initialize the logger 
# This logger will only log files that get sampled 
logger_name = 'Python-Web-Crawl-1.log'
log_path = params.log_file_path + "/" + logger_name
logger = Logger.Logger(logger_name,log_path)



# Define a method which will return back in a reasonable time
def submit_http_request(url):
    time_out = 3 # 3 seconds is the timeout I have set
    try:
        response = requests.get(url, timeout=time_out)
        # Check if the request was successful
        if response.status_code >= 400:
            do_not_visit_list.add(url)   
            return None 
    except Exception as e: 
        do_not_visit_list.add(url)
        return None
    return response


def not_supported_or_responsive_type(url):
   # Check if the content is of one of the supported types
    try:
      if(request_not_allowed(url)): return True
      response = requests.head(url,timeout=1)
      # The server is not responsive
      if(is_bad_response(response,url)): return True
      content_type = response.headers.get('content-type').split(";")[0]
      # gold ---- print(url)
      # Check if the Content-Type header indicates HTML
      if not(content_type and content_type.lower() in params.supported_crawl_types): return True
    except Exception as e: 
        print(e)
        return True
    return False 


def not_allowed_to_crawl(url):
    # Check the robot status:
    # create the robot url
    
    # scheme, domain, and path
    base_url = '/'.join(url.split('/')[:3])  
    robots_url = f"{base_url}/robots.txt"

    try:
       # Send a request to fetch the robots.txt file
       rp = robotparser.RobotFileParser()
       rp.set_url(robots_url)
       rp.read()
       # Check if the URL can be crawled
       if not(rp.can_fetch("*", url)): return True
    except Exception as e: return True    
    return False


def is_sensitive(url):
    ls = url.split(".",10)
    for part in ls:
        if(part in params.urls_to_avoid): return True
    return False    

def domain_frequency_exceeded(url):
    try:
        domain_name = urlparse(url).netloc
        domain_name_split = domain_name.split(".")
        domain_name = domain_name_split[len(domain_name_split)-2] + "."+  domain_name_split[len(domain_name_split) - 1]
        if(domain_name in domain_frequency and domain_frequency[domain_name] >= params.max_pages_per_domain): return True
    except Exception as e: return True
    return False

# Check against the following conditions:
# <TODO> list the conditions
def is_parsible(node):
    try:
        
        if(params.log_all_explored_files):
            logger.info(" visited ========>" + node)    
        global pages_explored
        pages_explored = pages_explored + 1
    
        # if in no visit list, dont visit
        # maybe moved there due to rate limit voilations
        if(signature(node) in do_not_visit_list): return False

        # if already visited
        # if(signature(node) in visited_nodes): return False

        # if the length of the url is greater than 100
        if(len(node) > 100): return False

        # Check if it is a query string, or a useless link
        if("?" in node or "#" in node or "%" in node or "@" in node): return False

        # The url is either not one of the supported types (like .html) 
        # or is not responsive, or is password protected
        if(not_supported_or_responsive_type(node)): return False
    
        # The robot.txt file does not allow to crawl
        # In the current code, I have not cached the robot.txt file
        # and since the scope of this run is small, I would not revisit these nodes
        if(not_allowed_to_crawl(node)): return False
    
        # If the url is password protected, do not crawl, catch it only the fly ig
        
        if(is_sensitive(node)): return False

        if(domain_frequency_exceeded(node)): return False
    except Exception as e:
        print(e)
    return True
    

# Save and log the file 
def download_file(soup,response,url):
  try:  
    # determine if we need to create a new subfolder
    make_new_folder = False
    global current_folder_idx
    global current_folder_count
    if((current_folder_count >= params.max_files_downloaded_in_same_path) or (current_folder_idx == -1)):
         current_folder_idx = current_folder_idx + 1
         make_new_folder = True
         current_folder_count = 0
   
    folder_path = params.file_download_root + str(current_folder_idx)
   
    if(make_new_folder):
        try:
           os.makedirs(folder_path, exist_ok=True)  # Create the folder, and don't raise an error if it already exists
           # Log the new folder that has been created
        except OSError as e:
            # Exit the code as there is no point continuing
            logger.error("Failed to create the dir to save the crawled files --- exiting ")
            exit

    file_name = str(soup.find('title').text)
    # remove all whitespaces for windows safety
    file_name = "".join(file_name.split())

    # check it contains illegal windows filename chars
    illegal_file_pattern = r'[<>:"/\\|?*]'
    
    # if its illegal, store it with a temp name
    if re.search(illegal_file_pattern, file_name): file_name = "file-" + str(pages_explored) + ".html"
    file_path = folder_path + "/" + str(file_name)
    
    with open(file_path, mode='wb') as localfile: localfile.write(response.content)
    global pages_sampled
    pages_sampled = pages_sampled + 1
    current_folder_count = current_folder_count + 1
    # Log the url entry
    logger.info("URL Sampled: {}, Size: {}".format(url, len(response.content)))

  except Exception as e:
    print(e)


# Update the stats for the crawled URL  
def work_statistics(response,soup,url,log_entry):
    
    country = 'NA'

    # Language
    try:
      language = detect(soup.find('title').text)
    except Exception as e:
       print(e)
       language = 'NA'

    # Domain     
    try:
       domain_name = urlparse(url).netloc
       domain_name_split = domain_name.split(".")
       domain_name_cleaned = domain_name_split[len(domain_name_split)-2] + "."+  domain_name_split[len(domain_name_split) - 1]
    except Exception as e:
       print(e)
       domain_name = 'NA'  
       domain_name_cleaned = domain_name

    # Geography
    try:
        # Try to get the geography of the node
        ip = socket.gethostbyname(domain_name)
        # use ipinfo data to get locations
        geolocation_url = f"https://ipinfo.io/{ip}/json"
        location_response = requests.get(geolocation_url,timeout=1)
        if location_response.status_code == 200:
           country = location_response.json().get('country')
    except Exception as e:
        country = 'NA'
        print(e)

    domain_name = domain_name_cleaned    
    
    # Log all this information,only when actually parsing the URL
    if log_entry: 
        if(not(country) in seen_geographies): seen_geographies[country] = 1
        else: seen_geographies[country] = seen_geographies[country] + 1
    
        if(not(language) in seen_languages): seen_languages[language] = 1
        else: seen_languages[language] = seen_languages[language] + 1

        if(not(domain_name) in domain_frequency): domain_frequency[domain_name] = 1
        else: domain_frequency[domain_name] = domain_frequency[domain_name] + 1
    
        # Log the country and lang for the Url
        logger.info("Country: {}, Language: {}".format(country, language)) 
    
    # return these for the helper methods
    return [country,language,domain_name]


# Return a unique signature for a url, for hashing purposes
# Returns a basic hash of the URL
# It can be extended to include more input to the 'key', like
# a substring of the response content, to generate same hash values
# for pages with diff URLs, but same content
def signature(url):
   return hash(url)



# Save the page in the output folder given in params
# Find and return the child urls
# Add to the Logger
# Update languages, domains, and geography maps
def parse_node(url):
    # all child nodes to be returned
    child_nodes = set()
    # update pages explored
    try:
        # Send a request to fetch the robots.txt file
        if(request_not_allowed(url)): return
        response = submit_http_request(url)
        # Check if the request was successful 
        # and not password protected
        if(not(is_bad_response(response,url))):
            soup = BeautifulSoup(response.content, 'html.parser')
            # download and save the file
            download_file(soup,response,url)
            # update geo,language etc
            work_statistics(response,soup,url,True)
            
            # Pick the next N children using random sampling
            all_child_list = soup.find_all('a', href=True)
            child_node_count = len(all_child_list)
            if(params.max_child_per_page <= child_node_count):
                random_indexes = random.sample(range(child_node_count),params.max_child_per_page)
                random_elements = [all_child_list[i] for i in random_indexes]
            else:     
                random_elements =  all_child_list

            for child_link in random_elements:
                # Get the unique signature for the node
                next_node = child_link['href']
                # See if the url is relative,also avoid self loops
                if(next_node == '/'): continue
                if('http' not in next_node): next_node = url + next_node # it was a relative URL
                if(signature(next_node) not in visited_nodes): child_nodes.add(next_node) 
              
    except Exception as e: 
        print(e) 
        return child_nodes                     
    return child_nodes 
      
# Returns true if the code has blacklisted a url
def request_not_allowed(url):
    if(url in do_not_visit_list): return True
    return False

# Return true if the response was not polite.
# Also blacklists aggressive sites
def is_bad_response(response,url):
    if(response is None): return True
    status_code = response.status_code
    # Have sent too many requests to this URL
    if(status_code == 429): 
        do_not_visit_list.add(url)
        return True
    if(status_code >= 400): return True
    return False


# We need to read the URL, even when adding to the PQ
# because the PQ priority function is heavily influenced
# by the language of the document
def add_node_to_pq(priority_queue, url):
    
    # Skip completely if the node is not parsible
    if(not(is_parsible(url))): return
    # weight of the URL for the priority queue
    # The more unique the URL, as compared to prev ones
    # the more weight we try to give it
    weight = 0
    
    try: 
        if(request_not_allowed(url)): return
        response = submit_http_request(url)
        if(is_bad_response(response,url)): return

    except Exception as e: return
    
    soup = BeautifulSoup(response.content, 'html.parser')

    node_metadata = work_statistics(response,soup,url,False)
    node_country = node_metadata[0]
    node_language = node_metadata[1]
    node_domain = node_metadata[2]
   
    geographic_representation = 0
    lang_representation = 0
    domain_representation = 0

    if(pages_explored != 0 and node_country in seen_geographies):
        geographic_representation = (seen_geographies[node_country]/pages_explored)*100
    
    if(pages_explored != 0 and node_language in seen_languages):
        lang_representation = (seen_languages[node_language]/pages_explored)*100
   
    if(pages_explored != 0 and node_domain in domain_frequency):
        domain_representation = (domain_frequency[node_domain]/pages_explored)*100

    # Give very high weights to potentially fresh links
    if(node_country not in seen_geographies): weight = weight + 10*(100 - geographic_representation)
    if(node_language not in seen_languages): weight = weight + 5*(100 - lang_representation)
    if(node_domain not in domain_frequency): weight = weight + 2.5*(100 - domain_representation)
    
    # weight it by the size of the page, if weight is zero
    if(weight == 0): weight = weight + math.log(len(response.content)) 
    # mult by -1 to implement a max heap
    entry = (-1*weight,url)
    # Push it to the queue
    heapq.heappush(priority_queue,entry)

    # implement a basic rate limiting to prevent DDos  
    time.sleep(0.5*(1/params.max_requests_per_second))


def start_crawling():
 
 try:
    # Log that the following seeds are loaded:
    logger.info("Starting Crawler ...")
    logger.info("Loaded the following seeds: ")
    logger.info(seed_list)
    logger.end_section()

    for next_seed in seed_list:

        # To keep a count of number of samples collected, starting from this seed
        child_count = 0
    
        # Build a priority queue, starting from this seed
        priority_queue = []
    
        # If a previous seed crawled over this seed too
        if(signature(next_seed) in visited_nodes): continue
  
        # Add the seed to the pq and start the traversal
        # Need a separate function for this as determining the 
        # priority is based on a few criterias
        add_node_to_pq(priority_queue,next_seed)

        # update visited list
        visited_nodes.add(signature(next_seed))
    
        # Iterate the pq till its empty or we have exceeded  
        # the max pages we can crawl per seed
        
        time_seed_st = time.time()

        while ((len(priority_queue) > 0) and (child_count <= params.max_pages_per_seed)) and (time.time() - time_seed_st <= 1800):
                                     
             # deque next node
             next_node = heapq.heappop(priority_queue)

             # parses the node and returns all its child nodes
             # also add it to the visited nodes
             # log the entry as well
             child_nodes = parse_node(next_node[1])
              
             # implement a basic rate limiting to prevent DDos  
             time.sleep(0.5*(1/params.max_requests_per_second))

             child_count = child_count + 1
             for child_node in child_nodes:
                # Add to visited
                visited_nodes.add(signature(child_node))
                add_node_to_pq(priority_queue,child_node)
            
 except Exception as e: print(e)          
 finally:
     dump_summary_stats()


start_crawling()