# Imports
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import os
import time


# Logging
def log_progress(message):
    """ This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./program_log.txt", "a") as g:
        g.write(timestamp + ' : ' + message + '\n')
        g.close()


# Webscraping
'''
Need to generate the URLs for the different maps
URL to generate list(towns and filds): https://ratemyserver.net/worldmap.php
URL to generate list(dungeons): https://ratemyserver.net/dungeonmap.php
URL for maps: https://ratemyserver.net/[map list]
URL for mobs: https://ratemyserver.net/index.php?page=mob_db&mob_id=[mob's id]
'''
list_of_maps = []
img_path = "images\\maps"

# This is used, so you don't have to make the same list every time you run the program
with open(r"list_of_maps.txt", 'r') as f:
    lines = f.readlines()
    for l in lines:
        if len(l) > 1:
            list_of_maps.append(l[:-1])
    f.close()

# This creates a folder for the images we will be extracting and enables us to create references to them in our data
if not os.path.exists(img_path):
    os.mkdir(img_path)
    print("Folder %s created!" % img_path)
else:
    pass


def make_list():
    """ This function creates a list to be used by the extract function
     to go through all the pages with the maps information. """
    global list_of_maps
    tnf_maps = "https://ratemyserver.net/worldmap.php"
    dun_maps = "https://ratemyserver.net/dungeonmap.php"
    tnf_page = requests.get(tnf_maps)
    dun_page = requests.get(dun_maps)

    if tnf_page.status_code == 200:
        tnf = tnf_page.text
        bs1 = BeautifulSoup(tnf, 'html.parser')
        one = bs1.find_all('a')
        two = one[11:-7]
        for t in two:
            href = t.get('href')
            list_of_maps.append(href)
    else:
        print("Could not get worldmap")

    if dun_page.status_code == 200:
        dun = dun_page.content
        bs2 = BeautifulSoup(dun, 'html.parser')
        three = bs2.find_all('td', class_="bborder")
        for t in three:
            more = t.find_all('a')
            for m in more:
                href2 = m.get('href')
                list_of_maps.append(href2)
    else:
        print("Could not get dungeons")

    # Writes a file, so we don't have to get the list every time
    with open("list_of_maps.txt", 'w') as h:
        for m in list_of_maps:
            h.writelines(f"{m}\n")

        print("File written successfully")
    f.close()


def extract_map_data():
    """ This function extracts the data we want from all URLs in our list of maps """
    global list_of_maps
    global img_path
    maps_columns = ["image_reference", "map_id", "map_area"]
    maps_df = pd.DataFrame(columns=maps_columns)
    list_of_maps = list(dict.fromkeys(list_of_maps))
    all_mobs_ids = []  # Need this for extract mob data function

    for m in list_of_maps:
        map_page = requests.get(f"https://ratemyserver.net/{m}")
        if map_page.status_code == 200:
            bs = BeautifulSoup(map_page.text, 'html.parser')


            # Downloading the images
            image_src = bs.find('img').get('src')
            final_path = f"{img_path}\\{image_src[37:]}"

            '''
            img_data = requests.get(image_src).content
            with open(final_path, 'wb') as h:
                h.write(img_data)
            '''
            
            # Creating the data frame for the Maps table we will have in our data base.
            # Data was extracted to csv so we do not have to repeat this process
            text_box = bs.find_all('div')[1]
            box = text_box.find_all('a')
            data_dict = {"image_reference": final_path, "map_id": box[0].text, "map_area": box[1].text}
            df1 = pd.DataFrame(data_dict, index=[0])
            maps_df = pd.concat([maps_df, df1], ignore_index=True)

            '''
            find_mob_id = bs.find_all('a', class_='gen_small')
            mob_id = []
            for find in find_mob_id:
                var = find.get('href')
                var = var[-4:]
                mob_id.append(var)
                all_mobs_ids.append(var)

            # Repeated so we can keep the logic intact for now
            text_box = bs.find_all('div')[1]
            box = text_box.find_all('a')

            # From the 6th element we need every other until we run out.
            find_mob = bs.find('body').get_text(strip=True, separator='|')

            # If the map has an MVP the structure of the page changes.
            if "MVP" in find_mob:
                find_mob = find_mob.replace("|[|MVP|]", "")

            mob_qt = find_mob.split('|')
            mob_qt = mob_qt[5::2]
            mob = dict(zip(mob_id, mob_qt[1::2]))
            relation_dict = {box[0].text: mob}

            # Writes a file, so we don't have to get the same data every time
            with open("relationship_dict.txt", 'a') as h:
                h.writelines(f"{relation_dict}\n")

            f.close()
            '''

            time.sleep(2)
        else:
            timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
            now = datetime.now()
            timestamp = now.strftime(timestamp_format)
            message = f"Could not get map {m[16:-9]}: {map_page.status_code}"
            with open("./error_log.txt", "a") as file:
                file.write(timestamp + ' : ' + message + '\n')
                file.close()
    '''
    all_mobs_ids = list(dict.fromkeys(all_mobs_ids))  # This removes all duplicates
    all_mobs_ids = sorted(all_mobs_ids)
    # Writes a file, so we don't have to get the list every time
    with open("list_of_mobs.txt", 'w') as h:
        for m in all_mobs_ids:
            h.writelines(f"{m}\n")
        print("File written successfully")
    f.close()
    '''
    load_to_csv(maps_df, 'maps_data.csv')
    return


def load_to_csv(df, output_path):
    """ This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""
    df.to_csv(output_path)


# make_list()

extract_map_data()

# load_to_csv(maps_data, 'maps_data.csv')

# Data transformation
# Database connection
# Loading
# Querying and plotting
