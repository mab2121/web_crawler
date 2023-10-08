"""
Mehran Ali Banka - Sep 2023
----------------------------
This is a class to store the default value for all parameters 
used by the crawler

"""

# List of types of sites to crawl, by default is set to only html
ls1 = ["text/html"]
supported_crawl_types = set(ls1)

# Log all the files explored by the crawler or not
log_all_explored_files = True

# No of seeds to consider
max_number_of_seeds = 20

# Seeding strategy (number assigned to different strategies)
seeding_strategy = "READ_FROM_PRE_CREATED_LIST"

# Seed file path
seed_file_path = r"C:/Search_Engines/Crawler/seed_files/seeds.txt.txt"

# Max pages to sample
max_number_of_pages_to_sample = 15000

# Max no of pages to sample per seed
max_pages_per_seed = 2000 # was 3k

# Root folder to save the downloaded files
file_download_root = r"C:/Search_Engines/Crawler/crawled_files/"

# Max files under a subfolder
max_files_downloaded_in_same_path = 400

# log file path
log_file_path = r"C:/Search_Engines/Crawler/log_files"

# max pages per domain
max_pages_per_domain = 20

# sensitive urls to avoid
ls = [".gov",".uk",".mil",".ca",".eu",".state",".org"]
urls_to_avoid = set(ls)

# crawl max requests per second. To prevent from being marked as a DDoS
max_requests_per_second = 4

# max children that we fetch per page
max_child_per_page = 10