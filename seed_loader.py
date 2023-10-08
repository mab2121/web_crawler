"""
seed_loader.py
Mehran Ali Banka - Sep 2023
----------------------------
This is a class to deal with operations involving a initial seed list

"""

def get_seeder_list(file_path, seeding_strategy):
   
  if(seeding_strategy == "READ_FROM_PRE_CREATED_LIST"): return load_seeds_from_file(file_path)
  elif(seeding_strategy == "GET_FROM_SEARCH_ENGINE"): return load_seeds_from_search_engine()
  

def load_seeds_from_file(file_path):
   # read all the seeds line by line
   # Open the file in read mode
   # Create an empty list to store the lines
   seed_list = []
   with open(file_path, 'r') as file:
        # Iterate over the lines of the file
        for line in file:
          # Remove the newline character at the end of the line
          line = line.strip()
          # Append the line to the list
          seed_list.append(line)
   
   return seed_list


def load_seeds_from_search_engine():
   return None