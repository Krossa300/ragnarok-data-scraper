# Imports
from bs4 import BeautifulSoup
import requests
import pandas as pd
from mysql import connector
from sqlalchemy import create_engine
from datetime import datetime
import os
import time

img_paths = ["images", "images\\maps", "images\\mobs"]  # Where to save the images


# Logging
def error_log(message):
    """ This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./error_log.txt", "a") as g:
        g.write(timestamp + ' : ' + message + '\n')
        g.close()


# Webscraping
'''
Need to generate the URLs for the different maps
URL to generate list(towns and fields): https://ratemyserver.net/worldmap.php
URL to generate list(dungeons): https://ratemyserver.net/dungeonmap.php
URL for maps: https://ratemyserver.net/[list of maps]
URL for mobs: p[mob's id]
'''


def load_to_csv(file_to_process):
    file_to_process.to_csv("maps_data.csv")


def make_list():
    """ This function creates a list to be used by the extract function
     to go through all the pages with the maps information. """
    list_of_maps = []
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
        error_log("Could not get worldmap")

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
        error_log("Could not get dungeons")

    list_of_maps = list(dict.fromkeys(list_of_maps))  # This removes some duplicates
    return list_of_maps


def extract_map_data(list_of_maps):
    """ This function extracts the data we want from all URLs in our list of maps """
    global img_paths
    maps_columns = ["image_reference", "map_id", "map_area"]
    maps_df = pd.DataFrame(columns=maps_columns)
    relation_dict = {}
    all_mobs_ids = []  # Need this for extract mob data function

    for m in list_of_maps:
        map_page = requests.get(f"https://ratemyserver.net/{m}")

        if map_page.status_code == 200:
            bs = BeautifulSoup(map_page.text, 'html.parser')
            text_box = bs.find_all('div')[1]
            box = text_box.find_all('a')

            # Downloads the images
            image_src = bs.find('img').get('src')
            final_path = f"{img_paths[1]}\\{image_src[37:]}"
            '''
            img_data = requests.get(image_src).content
            with open(final_path, 'wb') as fp:
                fp.write(img_data)
                fp.close()
            '''

            # Creating a data frame for the Maps table we will have in our database.
            data_dict = {"image_reference": final_path, "map_id": box[0].text, "map_area": box[1].text}
            df1 = pd.DataFrame(data_dict, index=[0])
            maps_df = pd.concat([maps_df, df1], ignore_index=True)

            # Now we get the information for our relationship between maps and monsters
            find_mob_id = bs.find_all('a', class_='gen_small')
            mob_id = []
            for find in find_mob_id:
                var = find.get('href')
                var = var[-4:]
                mob_id.append(var)
                all_mobs_ids.append(var)

            # From the 6th element we need every other until we run out.
            find_mob_qt = bs.find('body').get_text(strip=True, separator='|')

            # If the map has an MVP the structure of the page changes.
            if "MVP" in find_mob_qt:
                find_mob_qt = find_mob_qt.replace("|[|MVP|]", "")

            mob_qt = find_mob_qt.split('|')
            mob_qt = mob_qt[5::2]
            mob = dict(zip(mob_id, mob_qt[1::2]))
            relation_dict.update({box[0].text: mob})

            # When I let ir run wild I got blocked for a few minutes
            time.sleep(2)
        else:
            message = f"Could not get map {m[16:-9]}: {map_page.status_code}"
            error_log(message)

    all_mobs_ids = list(dict.fromkeys(all_mobs_ids))  # This removes all duplicates

    return maps_df, relation_dict, all_mobs_ids


def extract_mob_data(all_mobs_ids):
    """ This function extracts the data we want from all URLs in our list of monsters IDs """
    global img_paths
    mobs_columns = ["image_reference", "mob_id", "name", "level", "race", "property", "size"]
    mobs_df = pd.DataFrame(columns=mobs_columns)

    for mob_id in all_mobs_ids:
        mob_page = requests.get(f"https://ratemyserver.net/index.php?page=mob_db&mob_id={mob_id}")
        if mob_page.status_code == 200:
            bs3 = BeautifulSoup(mob_page.text, 'html.parser')
            head = bs3.find('tr', class_='filled_header_m')
            mob_name = head.find('span').get_text().split('(')  # We do this so we can get compound names right
            table = bs3.find('div', class_='mob_stat')
            t_data = table.find_all('td')

            # Download the image
            image_src = table.find('img', class_='mob_img').get('src')
            final_path = f"{img_paths[2]}\\{image_src[37:]}"
            '''
            img_data = requests.get(image_src).content
            with open(final_path, 'wb') as fp:
                fp.write(img_data)
                fp.close()
            '''

            # Creating a data frame for the Monsters table we will have in our database.
            data_dict = {"image_reference": final_path, "mob_id": mob_id,
                         "name": mob_name[0][:-3], "level": t_data[2].text, "race": t_data[3].text,
                         "property": t_data[4].text, "size": t_data[5].text}
            df2 = pd.DataFrame(data_dict, index=[0])
            mobs_df = pd.concat([mobs_df, df2], ignore_index=True)

            # Dont get blocked
            time.sleep(2)
        else:
            message = f"Could not get monster {mob_id}: {mob_page.status_code}"
            error_log(message)

    return mobs_df


def create_relation_df(relationship_dict):
    table_columns = ["map_id", "mob_id", "quantity"]
    relation_df = pd.DataFrame(columns=table_columns)
    for key, value in relationship_dict.items():
        for mob, qt in value.items():
            data_dict = {"map_id": key, "mob_id": mob, "quantity": qt}
            df = pd.DataFrame(data_dict, index=[0])
            relation_df = pd.concat([relation_df, df], ignore_index=True)

    return relation_df


def create_database():
    credentials = []
    with open("database_user.txt", 'r') as file:
        li = file.readlines()
        for line in li:
            split = line.split('=')
            credentials.append(split[1])

        file.close()

    try:
        with connector.connect(host="localhost", user=credentials[0], password=credentials[1]) as conn:
            create_db = "CREATE DATABASE IF NOT EXISTS ragnarok_online"
            with conn.cursor() as cursor:
                cursor.execute(create_db)
        conn.close()

    except connector.Error as e:
        print(e)

    try:
        with connector.connect(host="localhost", user=credentials[0],
                               password=credentials[1], database='ragnarok_online') as conn2:
            create_table_game_maps = """
                            CREATE TABLE IF NOT EXISTS game_maps (
                                image_reference VARCHAR(31) NOT NULL,
                                map_id VARCHAR(11) PRIMARY KEY NOT NULL,
                                map_area VARCHAR(47) NOT NULL
                            );
                            """
            create_table_game_monsters = """
                            CREATE TABLE IF NOT EXISTS game_monsters(
                                image_reference CHAR(21) NOT NULL,
                                mob_id CHAR(4) NOT NULL PRIMARY KEY,
                                name VARCHAR(40) NOT NULL,
                                level INT NOT NULL,
                                race VARCHAR(11) NOT NULL,
                                property VARCHAR(11) NOT NULL,
                                size VARCHAR(7) NOT NULL
                            );
                            """
            create_relationship_table = """
                            CREATE TABLE IF NOT EXISTS monsters_in_maps(
                            map_id VARCHAR(11) NOT NULL,
                            mob_id CHAR(4) NOT NULL ,
                            quantity INT,
                            FOREIGN KEY (map_id) REFERENCES game_maps(map_id),
                            FOREIGN KEY (mob_id) REFERENCES game_monsters(mob_id)
                            );
                            """
            with conn2.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS `monsters_in_maps`;")
                cursor.execute("DROP TABLE IF EXISTS `game_maps`;")
                cursor.execute("DROP TABLE IF EXISTS `game_monsters`;")
                cursor.execute(create_table_game_maps)
                cursor.execute(create_table_game_monsters)
                cursor.execute(create_relationship_table)
        conn2.close()

    except connector.Error as e:
        print(e)


def load_data(maps_df, mobs_df, relationship_df):
    credentials = []
    with open("database_user.txt", 'r') as file:
        li = file.readlines()
        for line in li:
            split = line.split('=')
            credentials.append(split[1])

        file.close()

    engine = create_engine(f"mysql+mysqlconnector://{credentials[0]}:{credentials[1]}@localhost/ragnarok_online")
    maps_df.to_sql("game_maps", engine, if_exists='append', index=False)
    mobs_df.to_sql("game_monsters", engine, if_exists='append', index=False)
    relationship_df.to_sql("monsters_in_maps", engine, if_exists='append', index=False)


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process, index_col=0)
    return dataframe


for path in img_paths:
    if not os.path.exists(path):
        os.mkdir(path)
        print("Folder %s created!" % path)


# Use wile developing
'''
sample_mobs = []
with open("list_of_mobs.txt", 'r') as f:
    lines = f.readlines()
    for l in lines:
        if len(l) > 1:
            sample_mobs.append(l[:-1])
    f.close()
    
list_of_maps = []
with open("list_of_maps.txt", 'r') as f:
    lines = f.readlines()
    for l in lines:
        if len(l) > 1:
            list_of_maps.append(l[:-1])
    f.close()
'''
relationship_dict = {}
with open("relationship_dict.txt", 'r') as f:
    lines = f.readlines()
    for l in lines:
        if len(l) > 1:
            l = eval(l[:-1])
            relationship_dict.update(l)
    f.close()

# need = make_list()
# d_frame, relation, mobs_ids = extract_map_data(need)
create_database()
maps_data = extract_from_csv('maps_data.csv')
mobs_data = extract_from_csv('mobs_data.csv')
relationship_df = create_relation_df(relationship_dict)
load_data(maps_data, mobs_data, relationship_df)
