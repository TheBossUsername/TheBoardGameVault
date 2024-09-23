import traceback
import requests
import pandas as pd
import sqlalchemy
import xml.etree.ElementTree as ET
from sqlalchemy.sql import text
from time import sleep
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the variables
user = os.getenv("USER")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")
server = os.getenv("SERVER")

# Connect to the database
engine = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{server}/{database}')


# Sort the board game cvs file by rank, bayesaverage, and average and remove any rows with 0 values, then strip the list to only include the id column
df = pd.read_csv('boardgames_ranks.csv')
df = df.replace(0, pd.NA)
sorted_list = df.sort_values(by=['rank', 'bayesaverage', 'average'], ascending=[True, False, False], na_position='last')
stripped_list = sorted_list[['id']]
new_games = 0
errored_ids =[]

stripped_list.to_csv('stripped_list.csv', index=False)

def print_progress_bar(current, total, bar_length=50):
    progress = current / total
    block = int(bar_length * progress)
    bar = "#" * block + "-" * (bar_length - block)
    percentage = progress * 100
    print(f"\r[{bar}] {percentage:.2f}%", end='', flush=True)

def fetch_data(url, retries=5, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            return response.content  # or response.json(), response.text() depending on your need
        except requests.RequestException as e:
            if attempt < retries - 1:
                sleep(delay)
            else:
                print(f"Attempt {attempt + 1} failed: {e}")
                return None

game_rank = 1  # Initialize game_rank

def find_text(element, path):
    found = element.find(path)
    return found.text if found is not None else None


connection = engine.connect()
transaction = connection.begin()

try:
    print("Adjusting ranks and adding new games")
    # Get the old ranks from the game_rank table
    old_ranks_query = text("SELECT board_game_id, game_rank FROM game_rank")
    old_ranks_result = connection.execute(old_ranks_query)
    old_ranks = {row[0]: row[1] for row in old_ranks_result}  

    # Delete all rows from the game_rank table
    delete_query = text("DELETE FROM game_rank")
    connection.execute(delete_query)

    #Update the game_rank table, and add any new board games to the board_game table
    total_items = len(stripped_list)
    print(f"Length of list: {total_items}")
    last_printed_progress = 0
    
    # Update the game_rank table, and add any new board games to the board_game table
    for index, row in stripped_list.iterrows():
        # Check if the board_game exists
        select_query = text("SELECT * FROM board_game WHERE id = :val1")
        result = connection.execute(select_query, {"val1": row['id']})
        if result.fetchone() is not None:
            # If the board_game exists, insert the game_rank
            insert_query = text("INSERT INTO game_rank (board_game_id, game_rank) VALUES (:val1, :val2)")
            connection.execute(insert_query, {"val1": row['id'], "val2": game_rank})

            # If the old rank is different from the new rank, update the old_rank column in the board_game table
            if old_ranks.get(row['id']) != game_rank:
                update_query = text("UPDATE board_game SET old_rank = :val1 WHERE id = :val2")
                connection.execute(update_query, {"val1": old_ranks.get(row['id']), "val2": row['id']})

            print_progress_bar(game_rank, total_items)

            game_rank += 1  # Increment game_rank
    

        else:

            url = f"https://api.geekdo.com/xmlapi/boardgame/{row['id']}?&stats=1"
            response = fetch_data(url)
        
            if response is None:
                errored_ids.append(row['id'])
                continue
            
            else:
                # Parse the XML response with ElementTree
                root = ET.fromstring(response)
                boardgame = root.find('boardgame')

                game_id = row['id']
                
                name = find_text(boardgame, 'name[@primary="true"]')
                if name is None:
                    continue
                else:
                    year_published = find_text(boardgame, 'yearpublished')
                    min_players = find_text(boardgame, 'minplayers')
                    max_players = find_text(boardgame, 'maxplayers')
                    age = find_text(boardgame, 'age')
                    average_weight = find_text(boardgame, 'statistics/ratings/averageweight')
                    playing_time = find_text(boardgame, 'playingtime')
                    min_playing_time = find_text(boardgame, 'minplaytime')
                    max_playing_time = find_text(boardgame, 'maxplaytime')
                    full_description = find_text(boardgame, 'description')
                    thumbnail = find_text(boardgame, 'thumbnail')
                    image = find_text(boardgame, 'image')
                    subdomain = find_text(boardgame, 'boardgamesubdomain')
                    average = find_text(boardgame, 'statistics/ratings/average')
                    bayes_average = find_text(boardgame, 'statistics/ratings/bayesaverage')
                    users_rated = find_text(boardgame, 'statistics/ratings/usersrated')
                    
                    # Insert into the board_game table (without description)
                    insert_board_game_query = text("""
                        INSERT INTO board_game 
                        (id, name, year_published, min_players, max_players, age, average_weight, playing_time, min_playing_time, max_playing_time, thumbnail, image, sub_domain, average, bayes_average, users_rated, old_rank) 
                        VALUES 
                        (:game_id, :name, :year_published, :min_players, :max_players, :age, :average_weight, :playing_time, :min_playing_time, :max_playing_time, :thumbnail, :image, :subdomain, :average, :bayes_average, :users_rated, :old_rank)
                    """)
                    connection.execute(insert_board_game_query, {
                        "game_id": row['id'],
                        "name": name,
                        "year_published": year_published,
                        "min_players": min_players,
                        "max_players": max_players,
                        "age": age,
                        "average_weight": average_weight,
                        "playing_time": playing_time,
                        "min_playing_time": min_playing_time,
                        "max_playing_time": max_playing_time,
                        "thumbnail": thumbnail,
                        "image": image,
                        "subdomain": subdomain,
                        "average": average,
                        "bayes_average": bayes_average,
                        "users_rated": users_rated,
                        "old_rank": None
                    })

                    # Insert into the board_game_description table
                    if full_description:
                        # Check if the description exceeds the 8000-character limit
                        if len(full_description) > 8000:
                            full_description = full_description[:8000]  # Truncate the description to 8000 characters

                        insert_description_query = text("""
                            INSERT INTO board_game_description 
                            (id, full_description) 
                            VALUES 
                            (:game_id, :full_description)
                        """)
                        connection.execute(insert_description_query, {
                            "game_id": row['id'],
                            "full_description": full_description
                        })


                    insert_query = text("INSERT INTO game_rank (board_game_id, game_rank) VALUES (:val1, :val2)")
                    connection.execute(insert_query, {"val1": row['id'], "val2": game_rank})

                    # For publishers
                    publishers = [pub.text for pub in boardgame.findall('boardgamepublisher')]
                    for publisher in publishers:
                        select_query = text("SELECT * FROM publishers WHERE name = :val1")
                        result = connection.execute(select_query, {"val1": publisher})
                        publisher_row = result.fetchone()
                        if publisher_row is None:
                            insert_query = text("INSERT INTO publishers (name) VALUES (:val1)")
                            connection.execute(insert_query, {"val1": publisher})
                            publisher_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
                        else:
                            publisher_id = publisher_row[0]

                        # Check if the record already exists in the board_game_has_publishers table
                        select_query = text("SELECT * FROM board_game_has_publishers WHERE board_game_id = :val1 AND publisher_id = :val2")
                        result = connection.execute(select_query, {"val1": row['id'], "val2": publisher_id})
                        if result.rowcount == 0:
                            insert_query = text("INSERT INTO board_game_has_publishers (board_game_id, publisher_id) VALUES (:val1, :val2)")
                            connection.execute(insert_query, {"val1": row['id'], "val2": publisher_id})

                    # For honors
                    honors = [honor.text for honor in boardgame.findall('boardgamehonor')]
                    for honor in honors:
                        select_query = text("SELECT * FROM honors WHERE name = :val1")
                        result = connection.execute(select_query, {"val1": honor})
                        honor_row = result.fetchone()
                        if honor_row is None:
                            insert_query = text("INSERT INTO honors (name) VALUES (:val1)")
                            connection.execute(insert_query, {"val1": honor})
                            honor_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
                        else:
                            honor_id = honor_row[0]

                        # Check if the record already exists in the board_game_has_honors table
                        select_query = text("SELECT * FROM board_game_has_honors WHERE board_game_id = :val1 AND honor_id = :val2")
                        result = connection.execute(select_query, {"val1": row['id'], "val2": honor_id})
                        if result.rowcount == 0:
                            insert_query = text("INSERT INTO board_game_has_honors (board_game_id, honor_id) VALUES (:val1, :val2)")
                            connection.execute(insert_query, {"val1": row['id'], "val2": honor_id})

                    # For mechanics
                    mechanics = [mechanic.text for mechanic in boardgame.findall('boardgamemechanic')]
                    for mechanic in mechanics:
                        select_query = text("SELECT * FROM mechanics WHERE name = :val1")
                        result = connection.execute(select_query, {"val1": mechanic})
                        mechanic_row = result.fetchone()
                        if mechanic_row is None:
                            insert_query = text("INSERT INTO mechanics (name) VALUES (:val1)")
                            connection.execute(insert_query, {"val1": mechanic})
                            mechanic_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
                        else:
                            mechanic_id = mechanic_row[0]

                        # Check if the record already exists in the board_game_has_mechanics table
                        select_query = text("SELECT * FROM board_game_has_mechanics WHERE board_game_id = :val1 AND mechanic_id = :val2")
                        result = connection.execute(select_query, {"val1": row['id'], "val2": mechanic_id})
                        if result.rowcount == 0:
                            insert_query = text("INSERT INTO board_game_has_mechanics (board_game_id, mechanic_id) VALUES (:val1, :val2)")
                            connection.execute(insert_query, {"val1": row['id'], "val2": mechanic_id})

                    # For categories
                    categories = [category.text for category in boardgame.findall('boardgamecategory')]
                    for category in categories:
                        select_query = text("SELECT * FROM categories WHERE name = :val1")
                        result = connection.execute(select_query, {"val1": category})
                        category_row = result.fetchone()
                        if category_row is None:
                            insert_query = text("INSERT INTO categories (name) VALUES (:val1)")
                            connection.execute(insert_query, {"val1": category})
                            category_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
                        else:
                            category_id = category_row[0]

                        # Check if the record already exists in the board_game_has_categories table
                        select_query = text("SELECT * FROM board_game_has_categories WHERE board_game_id = :val1 AND category_id = :val2")
                        result = connection.execute(select_query, {"val1": row['id'], "val2": category_id})
                        if result.rowcount == 0:
                            insert_query = text("INSERT INTO board_game_has_categories (board_game_id, category_id) VALUES (:val1, :val2)")
                            connection.execute(insert_query, {"val1": row['id'], "val2": category_id})

                    

                    new_games += 1

                    print_progress_bar(game_rank, total_items)

                    game_rank += 1  # Increment game_rank

    print()
    print(f"New games that were added {new_games}")
    print(f"Number of games that we could not get info for: {len(errored_ids)}")
    print("---------------------------------------------------------------")
    print("Updating all the boardgames that have moved up in rank")

    select_query = text("""
        SELECT board_game.id
        FROM board_game
        INNER JOIN game_rank
        ON board_game.id = game_rank.board_game_id
        WHERE board_game.old_rank IS NOT NULL
        AND board_game.old_rank < game_rank.game_rank;
    """)
    with engine.connect() as connect:
        result = connect.execute(select_query)
        board_game_ids = [row[0] for row in result]  # Extracting the first element from each tuple

    total_items = len(board_game_ids)
    print(f"Length of list: {total_items}")
    last_printed_progress = 0
    update = 1
    errored_ids = []

    for id in board_game_ids:
        # Assuming board_game is a tuple and the id is the first element
        url = f"https://api.geekdo.com/xmlapi/boardgame/{id}?&stats=1"
        response = fetch_data(url)
        
        if response is None:
            errored_ids.append(row['id'])
            continue
        
        else:
            try:    
                root = ET.fromstring(response)
                boardgame = root.find('boardgame')

                # Get the data from the API
                name = find_text(boardgame, 'name[@primary="true"]')
                year_published = find_text(boardgame, 'yearpublished')
                min_players = find_text(boardgame, 'minplayers')
                max_players = find_text(boardgame, 'maxplayers')
                age = find_text(boardgame, 'age')
                average_weight = find_text(boardgame, 'statistics/ratings/averageweight')
                playing_time = find_text(boardgame, 'playingtime')
                min_playing_time = find_text(boardgame, 'minplaytime')
                max_playing_time = find_text(boardgame, 'maxplaytime')
                full_description = find_text(boardgame, 'description')
                thumbnail = find_text(boardgame, 'thumbnail')
                image = find_text(boardgame, 'image')
                subdomain = find_text(boardgame, 'boardgamesubdomain')
                average = find_text(boardgame, 'statistics/ratings/average')
                bayes_average = find_text(boardgame, 'statistics/ratings/bayesaverage')
                users_rated = find_text(boardgame, 'statistics/ratings/usersrated')
                
                # Update the board_game table
                update_board_game_query = text("""
                UPDATE board_game
                SET name = :name,
                    year_published = :year_published,
                    min_players = :min_players,
                    max_players = :max_players,
                    age = :age,
                    average_weight = :average_weight,
                    playing_time = :playing_time,
                    min_playing_time = :min_playing_time,
                    max_playing_time = :max_playing_time,
                    thumbnail = :thumbnail,
                    image = :image,
                    sub_domain = :subdomain,
                    average = :average,
                    bayes_average = :bayes_average,
                    users_rated = :users_rated
                WHERE id = :id
                """)
                connection.execute(update_board_game_query, {
                    "id": id,
                    "name": name,
                    "year_published": year_published,
                    "min_players": min_players,
                    "max_players": max_players,
                    "age": age,
                    "average_weight": average_weight,
                    "playing_time": playing_time,
                    "min_playing_time": min_playing_time,
                    "max_playing_time": max_playing_time,
                    "thumbnail": thumbnail,
                    "image": image,
                    "subdomain": subdomain,
                    "average": average,
                    "bayes_average": bayes_average,
                    "users_rated": users_rated,
                })

                # Update or insert full_description in board_game_description table
                if full_description:
                    if len(full_description) > 8000:
                            full_description = full_description[:8000]  # Truncate the description to 8000 characters

                    insert_description_query = text("""
                        INSERT INTO board_game_description (id, full_description)
                        VALUES (:id, :full_description)
                        ON DUPLICATE KEY UPDATE full_description = :full_description
                    """)
                    connection.execute(insert_description_query, {
                        "id": id,
                        "full_description": full_description
                    })


                # For honors
                honors = [honor.text for honor in boardgame.findall('boardgamehonor')]
                for honor in honors:
                    select_query = text("SELECT * FROM honors WHERE name = :val1")
                    result = connection.execute(select_query, {"val1": honor})
                    honor_row = result.fetchone()
                    if honor_row is None:
                        insert_query = text("INSERT INTO honors (name) VALUES (:val1)")
                        connection.execute(insert_query, {"val1": honor})
                        honor_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
                    else:
                        honor_id = honor_row[0]

                    # Check if the record already exists in the board_game_has_honors table
                    select_query = text("SELECT * FROM board_game_has_honors WHERE board_game_id = :val1 AND honor_id = :val2")
                    result = connection.execute(select_query, {"val1": id, "val2": honor_id})
                    if result.rowcount == 0:
                        insert_query = text("INSERT INTO board_game_has_honors (board_game_id, honor_id) VALUES (:val1, :val2)")
                        connection.execute(insert_query, {"val1": id, "val2": honor_id})

                # For mechanics
                mechanics = [mechanic.text for mechanic in boardgame.findall('boardgamemechanic')]
                for mechanic in mechanics:
                    select_query = text("SELECT * FROM mechanics WHERE name = :val1")
                    result = connection.execute(select_query, {"val1": mechanic})
                    mechanic_row = result.fetchone()
                    if mechanic_row is None:
                        insert_query = text("INSERT INTO mechanics (name) VALUES (:val1)")
                        connection.execute(insert_query, {"val1": mechanic})
                        mechanic_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
                    else:
                        mechanic_id = mechanic_row[0]

                    # Check if the record already exists in the board_game_has_mechanics table
                    select_query = text("SELECT * FROM board_game_has_mechanics WHERE board_game_id = :val1 AND mechanic_id = :val2")
                    result = connection.execute(select_query, {"val1": id, "val2": mechanic_id})
                    if result.rowcount == 0:
                        insert_query = text("INSERT INTO board_game_has_mechanics (board_game_id, mechanic_id) VALUES (:val1, :val2)")
                        connection.execute(insert_query, {"val1": id, "val2": mechanic_id})

                # For categories
                categories = [category.text for category in boardgame.findall('boardgamecategory')]
                for category in categories:
                    select_query = text("SELECT * FROM categories WHERE name = :val1")
                    result = connection.execute(select_query, {"val1": category})
                    category_row = result.fetchone()
                    if category_row is None:
                        insert_query = text("INSERT INTO categories (name) VALUES (:val1)")
                        connection.execute(insert_query, {"val1": category})
                        category_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
                    else:
                        category_id = category_row[0]

                    # Check if the record already exists in the board_game_has_categories table
                    select_query = text("SELECT * FROM board_game_has_categories WHERE board_game_id = :val1 AND category_id = :val2")
                    result = connection.execute(select_query, {"val1": id, "val2": category_id})
                    if result.rowcount == 0:
                        insert_query = text("INSERT INTO board_game_has_categories (board_game_id, category_id) VALUES (:val1, :val2)")
                        connection.execute(insert_query, {"val1": id, "val2": category_id})
            except ET.ParseError as e:
                # Catch the ParseError, log it, and continue with the next game
                print(f"Error parsing XML for game ID {row['id']}: {e}")
                errored_ids.append(row['id'])
                continue  # Skip to the next game

            print_progress_bar(update, total_items)
            update += 1

    print(f"Number of games that we could not get info for: {len(errored_ids)}")


    print("---------------------------------------------------------------")
    print("Deleting games that are no longer on the database")

    delete_query = text("""
        DELETE FROM 
            board_game
        WHERE 
            id NOT IN (SELECT board_game_id FROM game_rank);
    """)

    connection.execute(delete_query)
    print("Deleted")
    
    transaction.commit()

except Exception as e:
    transaction.rollback()
    print(f"An error occurred: {e}")
    traceback.print_exc()
finally:
    connection.close()  # Ensure the connection is closed
