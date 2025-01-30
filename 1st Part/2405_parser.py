import math
import pandas as pd
import psycopg2
import numpy as np

file1 = "C:/Users/manio/Desktop/data science/data managment/mini project/miniproject/csv_GameofThrones_dataset/characters.csv"
file2 = "C:/Users/manio/Desktop/data science/data managment/mini project/miniproject/csv_GameofThrones_dataset/episodes.csv"
file3 = "C:/Users/manio/Desktop/data science/data managment/mini project/miniproject/csv_GameofThrones_dataset/locations.csv"
file4 = "C:/Users/manio/Desktop/data science/data managment/mini project/miniproject/csv_GameofThrones_dataset/keyValues.csv"

def populate_character_table(characters_df,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO character(characterid, ch_name, housename, nickname, royal) VALUES(%s,%s,%s,%s,%s)"
    for i in range(characters_df.shape[0]):
        charid = int(characters_df.get("charid")[i])
        name = get_or_none(characters_df.get("characterName")[i])
        nickname = get_or_none(characters_df.get("nickname")[i])
        housename = get_or_none(characters_df.get("houseName")[i])
        isroyal = get_or_none(characters_df.get("royal")[i])
        cur.execute(insert_statement,(charid, name, housename, nickname, isroyal))
    conn.commit()
    cur.close()
    return 

def get_or_none(val):
    if type(val) == str and val.lower() == "nan":
        return None
    if type(val) == float and math.isnan(val):
        return None
    return val

def populate_location_table(locations_df,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO locations(locationid, locationname) VALUES(%s,%s)"
    for i in range(locations_df.shape[0]):
        id = int(locations_df.get("locid")[i])
        locationname = get_or_none(locations_df.get("location")[i])
        cur.execute(insert_statement,(id,locationname))
    conn.commit()
    cur.close()

def populate_episodes_table(episodes_keyvalues_df,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO episodes(episodeid, seasonnum, episodenum, episodetitle, episodeairdate, length, episodedescription) VALUES(%s,%s,%s,%s,%s,%s,%s)"
    for i in range(episodes_keyvalues_df[1].shape[0]):
        id = int(episodes_keyvalues_df[1].get('epid')[i])
        episodeNum = int(get_or_none(episodes_keyvalues_df[1].get("episodes/episodeNum")[i]))
        seasonNum = int(get_or_none(episodes_keyvalues_df[1].get("episodes/seasonNum")[i]))
        episodetitle = str(get_or_none(episodes_keyvalues_df[1].get("episodes/episodeTitle")[i]))
        episode_airdate = episodes_keyvalues_df[1].get("episodes/episodeAirDate")[i]
        episode_Description = str(episodes_keyvalues_df[1].get("episodes/episodeDescription")[i])
        length = int(episodes_keyvalues_df[1].get("episode_length")[i])
        cur.execute(insert_statement,(id,seasonNum,episodeNum,episodetitle, episode_airdate, length, episode_Description))
    conn.commit()
    cur.close()

def populate_seasons_table(keyvalues_df ,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO seasons(seasonnum, seasonlength) VALUES(%s,%s)"
    for i in range(keyvalues_df.shape[0]):
        seasonnum = int(get_or_none(keyvalues_df.get("seasonNum")[i]))
        length = int(get_or_none(keyvalues_df.get("length")[i]))
        cur.execute(insert_statement,(seasonnum,length))
    conn.commit()
    cur.close()

def actor_df(characters_df):
    actor_df = characters_df.melt(
            id_vars=["charid"],
            value_vars=["actorName0", "actorName1"],
            var_name="actor_column",
            value_name="actorname"
        ).dropna(subset=["actorname"])
    actor_df = actor_df.dropna(subset=['actorname']).drop(columns=['actor_column']).reset_index(drop=True)
    actor_data = actor_df.values.tolist()
    return actor_data

def populate_actorname_table(actor_data,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO actorname(characterid, actorname) VALUES (%s, %s)"
    cur.executemany(insert_statement, actor_data)
    conn.commit()
    cur.close()

def sublocation_df(locations_df):
    subloc_df = locations_df.melt(
        id_vars=["locid"],
        value_vars=[col for col in locations_df.columns if "subLocation" in col],
        var_name="subLocation_column",  
        value_name="sublocation"  
    ).dropna(subset=["sublocation"])
    subloc_df = subloc_df.drop(columns=["subLocation_column"]).reset_index(drop=True)
    subloc_data = subloc_df.values.tolist()
    return subloc_data,subloc_df

def populate_sublocation_table(subloc_df,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO sublocation(locationid, sublocationname) VALUES(%s,%s)"
    for i in range(subloc_df.shape[0]):
        locationid = int(get_or_none(subloc_df.get("locid")[i]))
        subname = get_or_none(subloc_df.get("sublocation")[i])
        cur.execute(insert_statement,(locationid,subname))
    conn.commit()
    cur.close()

def melt_keyvalues_df(keyvalues_df,episodes_df):
    melted_keyvalues_df = keyvalues_df.melt(
        id_vars=["seasonNum"],
        value_vars=[col for col in keyvalues_df.columns if "/length" in col],
        var_name="episodes/episodeNum",
        value_name="episode_length"
    ).dropna(subset=["episode_length"])
    melted_keyvalues_df = melted_keyvalues_df.dropna(subset=["episode_length"]).reset_index(drop=True)
    melted_keyvalues_df['episodes/episodeNum'] = 1 + melted_keyvalues_df['episodes/episodeNum'].str.extract(r'episodes/(\d+)/length').astype(int)
    melted_keyvalues_df=melted_keyvalues_df.rename(columns={"seasonNum": "episodes/seasonNum"})
    melted_keyvalues_df.sort_values(by=['episodes/seasonNum'])
    result = pd.merge(episodes_df, melted_keyvalues_df, how="outer", on=["episodes/seasonNum", "episodes/episodeNum"])
    return melted_keyvalues_df, result

def opening_seq_df(episode_df):
    melt_episode_df =episode_df.melt(
            id_vars=["epid"],
            value_vars=[col for col in episode_df.columns if "/opening" in col],
            var_name="order",
            value_name="openingsequencelocation"
        ).dropna(subset=["openingsequencelocation"])
    melt_episode_df['order'] = 1 + melt_episode_df['order'].str.extract(r'episodes/openingSequenceLocations/(\d+)').astype(int)
    return melt_episode_df

def populate_opseqloc_table(op_seq_lox_df,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO openingsequencelocation(episodeid, orderofsequence, openingsequencelocation) VALUES(%s,%s,%s)"
    op_seq_lox_df['openingsequencelocation'] = op_seq_lox_df['openingsequencelocation'].astype(str)
    for i in range(op_seq_lox_df.shape[0]):
        epid = int(get_or_none(op_seq_lox_df.get("epid")[i]))
        order = int(get_or_none(op_seq_lox_df.get("order")[i]))
        opseqloc = get_or_none(op_seq_lox_df.get("openingsequencelocation")[i])
        cur.execute(insert_statement,(epid, order, opseqloc))
    conn.commit()
    cur.close()

def relationships_df(character_df):
    character_df = character_df.drop(columns=['actorName0','actorName1','houseName','nickname','royal'])
    melt_character_df =character_df.melt(
            id_vars=["charid","characterName"],
            value_vars=[col for col in character_df.columns if "parent"or"sibling"or"serve"or"kill"or"guardian"or"married" in col],
            var_name="relation",
            value_name="relatedto"
        ).dropna(subset=["relatedto"])
    melt_character_df['relation'] = melt_character_df['relation'].str.replace(r'\d+', '', regex=True)
    melt_character_df=melt_character_df.drop(columns=['characterName'])
    melt_character_df['relation'] = melt_character_df['relation'].astype(str)
    melt_character_df['relatedto'] = melt_character_df['relatedto'].astype(str)
    melt_character_df['relationid'] = range(1000, 1000 + len(melt_character_df))
    melt_character_df["relationid"] = melt_character_df["relationid"].astype(int)
    return melt_character_df

def populate_relationships_table(relationship_df,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO relationships(characterid, relation, relatedto, relationid) VALUES(%s,%s,%s,%s)"
    data = [
        (
            row["charid"],
            row["relation"],
            row["relatedto"],
            row["relationid"]
        )
        for _, row in relationship_df.iterrows()
    ]
    cur.executemany(insert_statement, data)
    conn.commit()
    cur.close()

def melt_scenes_df(episodes_df, subloc_df, locations_df):
    scenes = []
    for index, row in episodes_df.iterrows():
        season = row["episodes/seasonNum"]
        episode = row["episodes/episodeNum"]
        for scene_num in range(10):  # Assuming up to 10 scenes per episode
            prefix = f"episodes/scenes/{scene_num}/"
            if f"{prefix}sceneStart" in row and not pd.isna(row[f"{prefix}sceneStart"]):
                scene_data = {
                    "seasonNum": season,
                    "episodeNum": episode,
                    "sceneNumber": 1 + scene_num,
                    "sceneStart": row.get(f"{prefix}sceneStart"),
                    "sceneEnd": row.get(f"{prefix}sceneEnd"),
                    "location": row.get(f"{prefix}location"),
                    "subLocation": row.get(f"{prefix}subLocation"),
                    "episodeid": row.get("epid")
                }
            scenes.append(scene_data)
    scenes_df = pd.DataFrame(scenes)
    scenes_df['sceneid'] = range(2000, 2000 + len(scenes_df))
    subloc_df=subloc_df.rename(columns={"sublocation": "subLocation"})
    merged_scenes1_df = scenes_df.merge(locations_df,how='left', on='location')
    merged_scenes1_df = merged_scenes1_df.loc[:, ~merged_scenes1_df.columns.str.match(r'subLocation\d+')]
    subloc_df = subloc_df.drop(columns=['locid'])
    merged_scenes2_df = merged_scenes1_df.merge(subloc_df,how='left', on='subLocation')
    merged_scenes2_df = merged_scenes2_df.drop(columns=['seasonNum','episodeNum','location'])
    merged_scenes2_df = merged_scenes2_df.drop_duplicates(subset=['sceneid'])
    return merged_scenes2_df

def populate_scenes_table(scenes_df,conn):
    cur =conn.cursor()
    scenes_df['sceneStart'] = pd.to_datetime(scenes_df['sceneStart'], format='%H:%M:%S').dt.time
    scenes_df['sceneEnd'] = pd.to_datetime(scenes_df['sceneEnd'], format='%H:%M:%S').dt.time
    scenes_df['subLocation'] = scenes_df['subLocation'].where(pd.notna(scenes_df['subLocation']), None)
    insert_statement = "INSERT INTO scenes(scenenumber, scenestart, sceneend, sublocationname, episodeid, sceneid, locationid) VALUES(%s,%s,%s,%s,%s,%s,%s)"
    data_to_insert = list(
        scenes_df[['sceneNumber', 'sceneStart', 'sceneEnd', 'subLocation', 'episodeid', 'sceneid', 'locid']].itertuples(index=False, name=None)
    )
    cur.executemany(insert_statement, data_to_insert)
    conn.commit()
    cur.close()

def appearsin_df(character_df,episodes_df):
    scenes = []
    for index, row in episodes_df.iterrows():
        season = row["episodes/seasonNum"]
        episode = row["episodes/episodeNum"]
        for scene_num in range(10):
            prefix = f"episodes/scenes/{scene_num}/"
            if (f"{prefix}sceneStart" in episodes_df.columns and 
                not pd.isna(row[f"{prefix}sceneStart"])):
                characters = []
                char_num = 0
            while f"{prefix}characters/{char_num}/name" in episodes_df.columns:
                char_name = row.get(f"{prefix}characters/{char_num}/name")
                if not pd.isna(char_name):
                    characters.append(char_name)
                char_num += 1
            scene_data = {
                "seasonNum": season,
                "episodeNum": episode,
                "sceneNumber": 1 + scene_num,
                "sceneStart": row[f"{prefix}sceneStart"],
                "sceneEnd": row.get(f"{prefix}sceneEnd"),
                "characters": characters,
                "location": row.get(f"{prefix}location"),
                "subLocation": row.get(f"{prefix}subLocation"),
                "episodeid": row.get("epid")
            }
            scenes.append(scene_data)
    scenes_df = pd.DataFrame(scenes)
    scenes_df['sceneid'] = range(2000, 2000 + len(scenes_df))
    scenes_df_exploded = scenes_df.explode("characters").reset_index(drop=True)
    scenes_df_exploded = scenes_df_exploded.drop(columns=['seasonNum','episodeNum','location','subLocation','sceneStart','sceneNumber','sceneEnd'])
    character_df=character_df.rename(columns={"characterName": "characters"})
    filtered_df = character_df[['characters', 'charid']]
    merged_df = scenes_df_exploded.merge(filtered_df, how='left', on='characters')
    merged_df = merged_df.drop(columns=['characters'])
    merged_df_cleaned = merged_df.dropna()
    return merged_df_cleaned

def populate_appearsin_table(appears_df,conn):
    cur = conn.cursor()
    insert_statement = "INSERT INTO appearsin(characterid, sceneid, episodeid) VALUES(%s,%s,%s)"
    appears_df['charid'] = appears_df['charid'].astype('Int64')
    data = [
        (
            row["charid"],
            row["sceneid"],
            row["episodeid"]
        )
        for _, row in appears_df.iterrows()
    ]
    cur.executemany(insert_statement, data)
    conn.commit()
    cur.close()

def main():
    #reading data
    characters_df = pd.read_csv(file1,delimiter=';')
    episodes_df = pd.read_csv(file2,delimiter=';')
    locations_df = pd.read_csv(file3,delimiter=';')
    keyvalues_df = pd.read_csv(file4,delimiter=';')
    #generating unique id
    characters_df['charid'] = range(200, 200 + len(characters_df))
    episodes_df['epid'] = range(1, 1 + len(episodes_df))
    locations_df['locid'] = range(100, 100 + len(locations_df))
    #creating nessecary dataFrames
    actor_data = actor_df(characters_df)
    subloc_df = sublocation_df(locations_df)[1]
    episodes_keyvalues_df = melt_keyvalues_df(keyvalues_df,episodes_df)
    op_seq_loc_df = opening_seq_df(episodes_df)
    relationship_df = relationships_df(characters_df)
    scenes_df= melt_scenes_df(episodes_df, subloc_df, locations_df)
    appears_df = appearsin_df(characters_df, episodes_df)
    #connecting to db
    conn = psycopg2.connect(database = "miniproject", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "7244",
                        port = 5432)
    #populating tables
    # #Table insert order
    # 1) Location, Character, season
    # 2) actorname, sublocation, opening seq, characterrelationships, Episode
    # 3) scene
    # 4) Appears in
    populate_character_table(characters_df,conn)
    populate_location_table(locations_df,conn)
    populate_seasons_table(keyvalues_df,conn)
    populate_episodes_table(episodes_keyvalues_df,conn)
    populate_actorname_table(actor_data,conn)
    populate_sublocation_table(subloc_df,conn)
    populate_opseqloc_table(op_seq_loc_df,conn)
    populate_relationships_table(relationship_df,conn)
    populate_scenes_table(scenes_df,conn)
    populate_appearsin_table(appears_df,conn)
    conn.close()


main()
