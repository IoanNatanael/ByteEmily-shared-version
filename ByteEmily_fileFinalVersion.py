import datetime
import asyncio
import tabulate
import csv
import gspread
import discord
import os
import mysql.connector
import logging
import re
import requests
import json
import logging.handlers

from oauth2client.service_account import ServiceAccountCredentials
from typing import List
from io import StringIO
from discord.ext import commands
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
# Specify the path to your main log file
main_log_file_path = 'error.log'
file_handler = logging.handlers.RotatingFileHandler(main_log_file_path, mode='a', maxBytes=5 * 1024 * 1024,
                                                    backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

bot = commands.Bot(command_prefix='#', case_insensitive=True, intents=discord.Intents.all())
active_countdowns_file = 'wb_countdown.json'
list_message_id_wb = None
countdown_links_wb = {}

list_message_id = None
countdown_links = {}
allowed_channel_ids = [1215353680539418686, 1112675424229670995, 1215353688579772417, 1170411419850788936]


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return super().default(obj)


# Load active_countdowns from the JSON file if it exists
try:
    with open('active_countdowns.json', 'r') as json_file:
        loaded_countdowns = json.load(json_file)
        # Convert loaded datetime strings to datetime objects
        active_countdowns = {key: (datetime.datetime.fromisoformat(value[0]), value[1], value[2]) for key, value in
                             loaded_countdowns.items()}
except (FileNotFoundError, json.JSONDecodeError):
    logging.error("Error loading countdowns from file")
    active_countdowns = {}  # Initialize an empty dictionary if the file doesn't exist or is empty

# Load active_countdowns from the JSON file
try:
    with open('wb_countdown.json', 'r') as json_file:
        active_countdowns_wb = json.load(json_file)
except FileNotFoundError:
    print("Error loading countdowns from file")
    active_countdowns_wb = {}

# Load active_countdowns from the JSON file if it exists
try:
    with open('wb_countdown.json', 'r') as json_file:
        loaded_countdowns = json.load(json_file)
        active_countdowns_wb = {}
        for key, value in loaded_countdowns.items():
            if isinstance(value, list) or isinstance(value, tuple):
                if len(value) >= 3:
                    active_countdowns_wb[key] = (datetime.datetime.fromisoformat(value[0]), value[1], value[2])
                else:
                    print(f"Invalid countdown format for key {key}: {value}")
            else:
                print(f"Invalid countdown format for key {key}: {value}")
except (FileNotFoundError, json.JSONDecodeError):
    print("Error loading countdowns from file")
    active_countdowns_wb = {}


# Define the function for splitting messages
def split_message_chunks(message, chunk_size=1980):
    chunks = []
    current_chunk = ""
    for line in message.split('\n'):
        while len(line) > chunk_size:
            # Split the line into smaller chunks
            line_chunk, line = line[:chunk_size], line[chunk_size:]
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            chunks.append(line_chunk)
        if len(current_chunk) + len(line) + 3 > chunk_size:  # Check if adding this line exceeds chunk_size
            chunks.append(current_chunk)
            current_chunk = ""
        current_chunk += line + '\n'
    if current_chunk:  # Add the remaining chunk if not empty
        chunks.append(current_chunk)
    return chunks


# Function to check if a user has a specific role
def member_or_trial(user):
    member_role_name = 'member'
    trial_role_name = 'trial'
    eu_release = 'EU Release'
    lowercase_roles = [role.name.lower() for role in user.roles]
    return any(
        role_name == member_role_name.lower() or role_name == trial_role_name.lower()
        or eu_release == eu_release.lower() for role_name in lowercase_roles)


@bot.event
async def on_message(message):
    # Check if the message is a command and process it
    await bot.process_commands(message)

    # Check if the message is from the bot and not the loot_logger command
    if message.author == bot.user and not message.content.startswith('!loot_logger'):
        # Add the ❌ reaction to the bot message
        await message.add_reaction("❌")


@bot.event
async def on_reaction_add(reaction, user):
    # Check if the reaction is "❌" and the user is not a bot
    if str(reaction.emoji) == "❌" and not user.bot:
        try:
            # Fetch the message to make sure it still exists
            original_message = await reaction.message.channel.fetch_message(reaction.message.id)

            # Check if the message is already deleted or if the bot doesn't have permission to delete messages
            if original_message is not None and original_message.author.bot:
                try:
                    await original_message.delete()
                except discord.NotFound:
                    pass
                except Exception as e:
                    pass

            if original_message is not None and original_message.author.bot:
                try:
                    await original_message.delete()
                except discord.NotFound:
                    pass
                except Exception as e:
                    pass

                # Remove the countdown from the dictionary
                del active_countdowns[original_message.id]

                # Introduce a short delay before updating the list
                await asyncio.sleep(1)

                # Update the list message
                updated_list_message = await update_list_message(original_message.channel)

                # Update the list_message_id variable
                list_message_id = updated_list_message
                # Update the list message
                updated_list_message_wb = await update_list_message_wb(original_message.channel)

                # Update the list_message_id variable
                list_message_id = updated_list_message_wb

        except Exception as e:
            print(e)


@bot.command(name="remove")
@commands.check(lambda ctx: member_or_trial(ctx.author))
async def remove_countdown(ctx, index: str):
    try:
        # Convert the index argument to an integer
        index = int(index)

        # Check if the provided index is valid
        if 1 <= index <= len(active_countdowns):
            # Get the message_id of the countdown to be removed
            message_id_to_remove = list(active_countdowns.keys())[index - 1]

            # Remove the countdown from the dictionary
            del active_countdowns[message_id_to_remove]

            # Update the list message
            await update_list_message(ctx.channel)

        else:
            await ctx.send("Invalid index. Please provide a valid index.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        await ctx.send("An error occurred while trying to remove the countdown.")


@bot.command(name="content_in")
@commands.check(lambda ctx: member_or_trial(ctx.author))
async def content_in(ctx, time_str: str, *, custom_name: str = ""):
    try:
        if ctx.channel.id not in allowed_channel_ids:
            await ctx.send("4. This command is not allowed in this channel.")
            return

        # Load active_countdowns from the JSON file if it hasn't been loaded yet
        if not active_countdowns:
            try:
                with open('active_countdowns.json', 'r') as json_file:
                    loaded_countdowns = json.load(json_file)
                    # Convert loaded datetime strings to datetime objects
                    active_countdowns.update(
                        {key: (datetime.datetime.fromisoformat(value[0]), value[1], value[2]) for key, value in
                         loaded_countdowns.items()})
            except (FileNotFoundError, json.JSONDecodeError):
                logging.error("5. Error loading countdowns from file")

        # Parse input time (hours:minutes)
        hours, minutes = map(int, time_str.split(':'))
        total_seconds = hours * 3600 + minutes * 60

        # Calculate end time in UTC
        current_time = datetime.datetime.now(datetime.UTC)
        end_time = current_time + datetime.timedelta(seconds=total_seconds)

        # Convert end_time to a string before storing it
        end_time_str = end_time.isoformat()

        # Format countdown message using Discord timestamp format
        utc = datetime.timezone.utc
        end_time_utc = end_time.replace(tzinfo=utc)
        discord_timestamp = f'<t:{int(end_time_utc.timestamp())}:R>'
        day_name = end_time.strftime('%A')
        formatted_time = f" `🕦 {end_time.strftime('%H:%M')}`  `📅 {day_name}`"
        countdown_message = f"Countdown will end: {discord_timestamp} {formatted_time} UTC"

        # Reply to the user's message with the countdown message
        response_message = await ctx.message.reply(countdown_message)

        # Add "❌" emoji reaction to the response message
        await response_message.add_reaction("❌")

        def check_reaction(reaction, user):
            return str(reaction.emoji) == "❌" and reaction.message.id == response_message.id and user == ctx.author

        # Add countdown information to the active_countdowns dictionary with custom name
        active_countdowns[response_message.id] = (end_time, ctx.author, custom_name)

        countdown_links[response_message.id] = response_message.jump_url

        # Update the list message and pass the response_message.id
        await update_list_message(ctx)

        # Wait for the countdown to finish or until the user reacts with "❌" emoji
        while end_time > datetime.datetime.now(datetime.UTC):
            await asyncio.sleep(1)

            try:
                reaction, _ = await bot.wait_for("reaction_add", check=check_reaction, timeout=1)
                # User reacted with "❌", stop the countdown
                break
            except asyncio.TimeoutError:
                pass

        # Remove the countdown from the dictionary once it expires
        del active_countdowns[response_message.id]

        # Update the list message
        await update_list_message(ctx)

        # Edit the original countdown message when the countdown is done
        await response_message.edit(content="6. Content ended!")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


async def update_list_message(channel: discord.TextChannel) -> int:
    global list_message_id

    # Check if the list message exists
    if list_message_id:
        try:
            list_message = await channel.fetch_message(list_message_id)
            await list_message.delete()

        except discord.NotFound:
            logging.error("7. Existing list message not found.")

    # Convert datetime objects to ISO format before saving to JSON
    serialized_countdowns = {
        key: (value[0].isoformat(), value[1], value[2]) for key, value in active_countdowns.items()
    }

    # Save active_countdowns to a JSON file
    with open('active_countdowns.json', 'w') as json_file:
        json.dump(serialized_countdowns, json_file, default=str)

    # Create a new list message with an embedded layout
    embed = discord.Embed(
        title="Active Countdowns",
        color=discord.Color.blue(),
    )

    if not active_countdowns:
        embed.description = "No countdowns currently active."
    else:
        embed.description = "_List of ongoing countdowns:_  \n 😵‍💫"
        for idx, (message_id, (end_time, author, custom_name)) in enumerate(
                sorted(active_countdowns.items(), key=lambda x: x[1][0]), start=1):
            formatted_info = f"**{custom_name}**" if custom_name else f"Content: {idx}"
            day_name = end_time.strftime('%A')
            time_str = end_time.strftime('%H:%M')
            formatted_time = f"`📅 {day_name}`  `🕦 {time_str} UTC`"
            final_link = f"[{formatted_info}]({countdown_links.get(message_id, 'Unknown')})" if countdown_links.get(
                message_id) else formatted_info

            embed.add_field(
                name=f"Countdown {idx}",
                value=f"{final_link}\n{formatted_time}",
                inline=False
            )

    # Send the new list message to the same channel
    list_message = await channel.send(embed=embed)
    list_message_id = list_message.id
    return list_message_id


@bot.command(name="remove_wb")
async def remove_countdown_wb(ctx, index: str):
    try:
        # Convert the index argument to an integer
        index = int(index)

        # Check if the provided index is valid
        if 1 <= index <= len(active_countdowns_wb):
            # Get the message_id of the countdown to be removed
            message_id_to_remove = list(active_countdowns_wb.keys())[index - 1]

            # Remove the countdown from the dictionary
            del active_countdowns_wb[message_id_to_remove]

            # Update the list message
            await update_list_message_wb(ctx.channel)

        else:
            await ctx.send("Invalid index. Please provide a valid index.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        await ctx.send("An error occurred while trying to remove the countdown.")


wb_channel = [1215353681059381326, 1112675424229670995, 1170411419850788936]


@bot.command(name="wb")
async def wb_countdown(ctx, date_str: str, time_str: str, *, custom_name: str = ""):
    try:
        if ctx.channel.id not in wb_channel:
            await ctx.send("4. This command is not allowed in this channel.")
            return

        # Load active_countdowns from the JSON file if it hasn't been loaded yet
        if not active_countdowns_wb:
            try:
                with open('wb_countdown.json', 'r') as json_element:
                    loaded_countdowns_json = json.load(json_element)
                    # Convert loaded datetime strings to datetime objects
                    active_countdowns_wb.update(
                        {the_key: (datetime.datetime.fromisoformat(value[0]), value[1], value[2]) for the_key, value in
                         loaded_countdowns_json.items()})
            except (FileNotFoundError, json.JSONDecodeError):
                print("5. Error loading countdowns from file")

        # Parse input date and time
        date_parts = date_str.split('-')
        time_parts = time_str.split(':')
        year, month, day = map(int, date_parts)
        hour, minute = map(int, time_parts)
        user_date_time = datetime.datetime(year, month, day, hour, minute)

        # Calculate end time in UTC (48 hours from user-provided date and time)
        end_time = user_date_time + datetime.timedelta(hours=48)

        # Convert end_time to a string before storing it
        end_time_str = end_time.isoformat()

        # Format countdown message using Discord timestamp format
        utc = datetime.timezone.utc
        end_time_utc = end_time.replace(tzinfo=utc)
        discord_timestamp = f'<t:{int(end_time_utc.timestamp())}:R>'
        formatted_time = end_time_utc.strftime('%Y-%m-%d `%H:%M:%S`')
        countdown_message = f"Countdown will end: {discord_timestamp} ({formatted_time} UTC)"

        # Reply to the user's message with the countdown message
        response_message = await ctx.message.reply(countdown_message)

        # Add "❌" emoji reaction to the response message
        await response_message.add_reaction("❌")

        def check_reaction(reaction, user):
            return str(reaction.emoji) == "❌" and reaction.message.id == response_message.id and user == ctx.author

        # Add countdown information to the active_countdowns dictionary with custom name
        active_countdowns_wb[response_message.id] = (end_time, ctx.author, custom_name)

        countdown_links_wb[response_message.id] = response_message.jump_url

        # Update the list message and pass the response_message.id
        await update_list_message_wb(ctx)

        # Wait for the countdown to finish or until the user reacts with "❌" emoji
        while end_time > datetime.datetime.now(datetime.UTC):
            await asyncio.sleep(1)

            try:
                reaction, _ = await bot.wait_for("reaction_add", check=check_reaction, timeout=1)
                # User reacted with "❌", stop the countdown
                break
            except asyncio.TimeoutError:
                pass

        # Remove the countdown from the dictionary once it expires
        del active_countdowns_wb[response_message.id]

        # Update the list message
        await update_list_message_wb(ctx)

        # Edit the original countdown message when the countdown is done
        await response_message.edit(content="6. Content ended!")

    except Exception as e:
        pass


async def update_list_message_wb(channel):
    global list_message_id_wb

    # Check if the list message exists
    if list_message_id_wb:
        try:
            list_message = await channel.fetch_message(list_message_id_wb)
            await list_message.delete()

        except discord.NotFound:
            # Log the NotFound error without causing the entire process to fail
            logger.error("Existing list message not found.")

    # Convert datetime objects to ISO format before saving to JSON
    serialized_countdowns = {
        key: (value[0].isoformat(), value[1], value[2]) for key, value in active_countdowns_wb.items()
    }

    # Save active_countdowns to a JSON file
    with open('wb_countdown.json', 'w') as json_file:
        json.dump(serialized_countdowns, json_file, default=str)

    # Create a new list message with an embedded layout
    embed = discord.Embed(
        title="Active Countdowns",
        color=discord.Color.blue(),
    )

    if not active_countdowns_wb:
        embed.description = "No countdowns currently active."
    else:
        embed.description = "_List of ongoing countdowns:_  \n 😵‍💫"
        for message_id, (end_time, author, custom_name) in active_countdowns_wb.items():
            formatted_info = f"**{custom_name}**" if custom_name else f"Content: {message_id}"
            day_name = end_time.strftime('%A')
            time_str = end_time.strftime('%H:%M')
            formatted_time = f"`📅 {day_name}`  `🕦 {time_str} UTC`"
            final_link = f"[🌎 {formatted_info}]({countdown_links_wb.get(message_id, 'Unknown')})" if countdown_links_wb.get(
                message_id) else formatted_info
            embed.add_field(
                name=f"Countdown",
                value=f"{final_link}\n{formatted_time}",
                inline=False
            )

    # Send the new list message to the same channel
    list_message = await channel.send(embed=embed)
    list_message_id_wb = list_message.id


# Function to authenticate with Google Sheets using service account credentials
def authenticate():
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_name('byte-417011-9f9c1f58b5fb.json', scope)
    return gspread.authorize(creds)


# Function to retrieve logs from the spreadsheet
def get_logs(username, spreadsheet):
    try:
        sheet = spreadsheet.sheet1
        all_values = sheet.get_all_values()
        user_logs = []
        for row in all_values[1:]:
            if len(row) >= 6:  # Ensure row has at least 6 columns
                if row[1] == username and int(
                        row[5]) > 0:  # Check if the Player matches the username and amount is positive
                    # Append a list of (Item, Enchantment, Amount) to the user_logs list
                    user_logs.append([row[2], row[3], row[5]])
            else:
                logging.error(f"9. Ignoring row with insufficient columns: {row}")
        return user_logs
    except Exception as e:
        logging.error(f"10. Error retrieving logs: {e}")
        return None


# Discord command to retrieve logs for a specific player
@bot.command()
async def log(ctx, Player):
    try:
        spreadsheet = authenticate().open_by_key('1ZKroN93892iTp8WfgcHT75uIFDflsJGvtafN8MSj6i8')
        user_logs = get_logs(Player, spreadsheet)
        if user_logs:
            # Calculate total amount
            total_amount = sum(int(row[2]) for row in user_logs)

            # Add a new column at the top of the table with the total amount
            headers = ["Item", "Enchantment", "Amount"]
            user_logs.insert(0, ["Total amount", "", f"**{total_amount}**"])  # Use Markdown bold for total amount

            # Format table
            table = tabulate.tabulate(user_logs, headers=headers, tablefmt="fancy_grid")
            chunks = split_message_chunks(table)

            # Send message in chunks
            for chunk in chunks:
                # Use Discord Markdown to color the total amount blue
                formatted_chunk = chunk.replace(f"**{total_amount}**",
                                                f"**{total_amount}**")  # Replace total amount with blue colored text
                await ctx.send(f"```{formatted_chunk}```")
        else:
            await ctx.send("12. No logs found for the user.")
    except Exception as e:
        await ctx.send(f"13. An error occurred: {e}")


# Function to check if a user has a specific role
def check_role(user):
    officer_role_name = 'Officer'
    council_role_name = 'Council'
    lowercase_roles = [role.name.lower() for role in user.roles]
    return any(
        role_name == officer_role_name.lower() or role_name == council_role_name.lower() for role_name in
        lowercase_roles)


@bot.command(name='loot_logger')
async def loot_logger(ctx):
    # Check if the user has one of the required roles
    if not check_role(ctx.author):
        await ctx.send("14. You do not have permission to use this command.")
        return

    if ctx.channel.id not in allowed_channel_ids:
        await ctx.send("15. This command is not allowed in this channel.")
        return

    from tabulate import tabulate
    try:
        attachments = ctx.message.attachments
        if not attachments or not all(attachment.filename.endswith('.csv') for attachment in attachments):
            await ctx.send("16. Please attach one or more CSV files.")
            return

        summary_tables = []

        for attachment in attachments:
            file_content = await attachment.read()

            csv_data = StringIO(file_content.decode())
            reader = csv.reader(csv_data, delimiter=';')
            deposit_data = list(reader)

            deposited_items = {}
            for row in deposit_data[1:]:
                guild = row[2]
                if guild.lower() not in ["smurfing monkeys", "surfing penguins"]:
                    continue

                user = row[3]
                item = row[5]
                quantity = row[6]
                if user not in deposited_items:
                    deposited_items[user] = {(item, quantity)}
                else:
                    deposited_items[user].add((item, quantity))

            user_logs = {(row[3], row[5]) for row in deposit_data[1:]}

            undeposited_items = {}
            for user, items in deposited_items.items():
                undeposited_items[user] = items - user_logs

            summary_table = []
            player_count = 1
            for user, items in undeposited_items.items():
                if items:
                    undeposited_items_str = "\n".join([f"{item[0]} ( {item[1]} )" for item in items])
                    summary_table.append((f"({player_count}) - [ {user} ]", len(items), undeposited_items_str))
                    player_count += 1

            summary_tables.append(summary_table)

        formatted_tables = []
        headers = ["User", "Undepo\nItems\nCount", "Undepo Items Names \n+count for each item \n( number )"]
        for summary_table in summary_tables:
            table_str = tabulate(summary_table, headers=headers, tablefmt="fancy_grid")
            formatted_tables.append(table_str)

        combined_table_str = "\n".join(formatted_tables)
        message_chunks = paginate_output(combined_table_str)

        current_page = 0
        message = await ctx.send(f"```Page {current_page + 1}/{len(message_chunks)}\n{message_chunks[current_page]}```")

        await message.add_reaction('⬅️')
        await message.add_reaction('➡️')

        while True:
            try:
                reaction, user = await bot.wait_for('reaction_add',
                                                    check=lambda r, u: u == ctx.author and str(r.emoji) in ['⬅️', '➡️',
                                                                                                            '🛂', '🔄'])

                if str(reaction.emoji) == '⬅️':
                    current_page = max(0, current_page - 1)
                elif str(reaction.emoji) == '➡️':
                    current_page = min(len(message_chunks) - 1, current_page + 1)

                await message.edit(
                    content=f"```Page {current_page + 1}/{len(message_chunks)}\n{message_chunks[current_page]}```")
                await reaction.remove(user)

            except Exception as e:
                await ctx.send(f"17. An error occurred: {e}")

    except Exception as e:
        await ctx.send(f"18. An error occurred: {e}")


def paginate_output(text: str, max_chars: int = 1950) -> List[str]:
    """Paginate the given text."""
    if len(text) <= max_chars:
        return [text]
    chunks = []
    current_chunk = ""
    for line in text.split('\n'):
        if len(current_chunk) + len(line) + 1 > max_chars:
            chunks.append(current_chunk)
            current_chunk = ""
        current_chunk += line + '\n'
    if current_chunk:
        chunks.append(current_chunk)
    return chunks


# Function to establish MySQL connection for DATABASE2
def establish_connection_2():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('HOST'),
            user=os.getenv('USER'),
            password=os.getenv('PASSWORD'),
            database=os.getenv('DATABASE2'),  # Select DATABASE2
            auth_plugin='mysql_native_password',
        )
        return conn
    except mysql.connector.Error as err:
        logger.error("Failed to establish MySQL connection:", exc_info=True)
        return None


# Function to format numbers with commas and add a coin emoji
def format_number(amount):
    formatted_amount = '{:,.0f}'.format(amount)  # Format number with commas
    return f'` {formatted_amount} ` :coin:'  # Add coin emoji at the end


# Command to allow users to sign up
@bot.command(name='signup')
async def signup(ctx, ign_username: str):
    if member_or_trial(ctx.author):
        conn = establish_connection_2()
        if conn:
            try:
                # Check if the user is already signed up
                cursor = conn.cursor()
                select_query = "SELECT COUNT(*) FROM users WHERE discord_user_id = %s"
                cursor.execute(select_query, (str(ctx.author.id),))
                result = cursor.fetchone()
                if result and result[0] > 0:
                    await ctx.send("You are already signed up!")
                else:
                    # Insert user data into the database
                    insert_query = "INSERT INTO users (discord_user_id, username, ignUsername) VALUES (%s, %s, %s)"
                    cursor.execute(insert_query, (str(ctx.author.id), ctx.author.name, ign_username))
                    conn.commit()
                    await ctx.send("You have been successfully signed up!")
                cursor.close()
                conn.close()
            except mysql.connector.Error as err:
                logger.error("Failed to insert or retrieve user data from the database:", exc_info=True)
                await ctx.send("An error occurred while signing up. Please try again later.")
        else:
            await ctx.send("An error occurred while connecting to the database. Please try again later.")
    else:
        await ctx.send("Sorry, only members or trials can use this command.")


# Command to allow admins to delete users from the database
@bot.command(name='delete_user')
async def delete_user(ctx, *users: discord.Member):
    # Check if the user invoking the command is an admin
    if ctx.author.guild_permissions.administrator:
        conn = establish_connection_2()
        if conn:
            try:
                cursor = conn.cursor()
                deleted_users = []
                not_found_users = []
                for user in users:
                    # Extract user ID from the mention
                    user_id = str(user.id)
                    # Check if the user exists in the database
                    select_query = "SELECT * FROM users WHERE discord_user_id = %s"
                    cursor.execute(select_query, (user_id,))
                    result = cursor.fetchone()
                    if result:
                        # Delete the user from the database
                        delete_query = "DELETE FROM users WHERE discord_user_id = %s"
                        cursor.execute(delete_query, (user_id,))
                        conn.commit()  # Commit the transaction
                        deleted_users.append(user.display_name)
                    else:
                        not_found_users.append(user.display_name)
                cursor.close()
                conn.close()
                if deleted_users:
                    await ctx.send(f"Users ** {', '.join(deleted_users)} ** have been deleted from the database.")
                if not_found_users:
                    await ctx.send(f"Users ** {', '.join(not_found_users)} ** were not found in the database.")
            except mysql.connector.Error as err:
                logger.error("Failed to delete users from the database:", exc_info=True)
                await ctx.send("An error occurred while deleting users from the database. Please try again later.")
        else:
            await ctx.send("An error occurred while connecting to the database. Please try again later.")
    else:
        await ctx.send("Sorry, only administrators can use this command.")


@bot.command(name='add')
async def add(ctx, *args):
    """
    Add money to the loot split.
    Usage: !add (<amount>) (<user names>) (<tax percentage>)
    """
    global user_name
    try:
        conn = establish_connection_2()  # Use the second database for adding to loot split
        if conn:
            cursor = conn.cursor()

            # Extract the ID of the user invoking the command and convert it to a string
            added_by_user_id = str(ctx.author.id)

            # Parse the arguments using regular expressions
            pattern = r'\((.*?)\)'
            parsed_args = re.findall(pattern, ' '.join(args))

            if len(parsed_args) != 3:
                await ctx.send(
                    "Incorrect usage. Please use the correct format: !add (<amount>) (<user names>) (<tax percentage>)")
                return

            amount = float(parsed_args[0])
            user_names = [name.strip() for name in parsed_args[1].split(',')]
            tax_percentage = int(parsed_args[2])

            split_amount = amount * (1 - (tax_percentage / 100))

            # Flag to track if at least one user is found
            user_found = False

            # Fetch user IDs based on usernames and insert into loot_splits table
            for user_name in user_names:
                # Fetch user ID from users table based on username
                cursor.execute("SELECT id FROM users WHERE username = %s OR ignUsername = %s",
                               (user_name, user_name,))
                result = cursor.fetchone()
                if result:
                    user_found = True
                    user_id = result[0]
                    # Insert the split amount for the user into the database
                    sql = 'INSERT INTO loot_splits (user_id, split_amount, split_date, added_by_user_id) VALUES (%s, %s, NOW(), %s)'
                    cursor.execute(sql, (user_id, split_amount, added_by_user_id))
                    conn.commit()
                else:
                    await ctx.send(f'User "**{user_names}**" not found.')

            if user_found:
                formatted_amount = format_number(split_amount)
                await ctx.send(f'Money added to **{user_names}** loot split successfully! Amount: {formatted_amount}')

            cursor.close()
            conn.close()
        else:
            await ctx.send("An error occurred while connecting to the database. Please try again later.")
    except Exception as e:
        await ctx.send(f'An error occurred: {e}')


@bot.command(name='payout')
async def payout(ctx, *args):
    try:
        conn = establish_connection_2()  # Use the second database for payouts
        if conn:
            cursor = conn.cursor()

            # Parse the arguments using regular expressions
            pattern = r'\((.*?)\)'
            parsed_args = re.findall(pattern, ' '.join(args))

            # Check if the arguments are provided in the correct format
            if len(parsed_args) != 1:
                await ctx.send("Incorrect usage. Please use the correct format: !payout (<user names>)")
                return

            user_names = [name.strip() for name in parsed_args[0].split(',')]

            for user_name in user_names:
                # Fetch the user ID from the users table based on the username
                cursor.execute("SELECT id FROM users WHERE username = %s OR ignUsername = %s",
                               (user_name, user_name,))
                result = cursor.fetchone()
                if result:
                    user_id = result[0]

                    # Fetch the total amount from user_totals
                    cursor.execute("SELECT total_amount FROM user_totals WHERE user_id = %s", (user_id,))
                    total_amount = cursor.fetchone()[0]

                    # Insert the payout details into the payouts table
                    sql = ('INSERT INTO payouts (user_id, payout_amount, payout_date, added_by_user_id) '
                           'VALUES (%s, %s, NOW(), %s)')
                    cursor.execute(sql, (user_id, total_amount, str(ctx.author.id)))
                    conn.commit()

                    formatted_amount = format_number(total_amount)
                    await ctx.send(f'Payout of {formatted_amount} made to user "**{user_name}**" successfully!')
                else:
                    await ctx.send(f'User "**{user_name}**" not found.')

            cursor.close()
            conn.close()
        else:
            await ctx.send("An error occurred while connecting to the database. Please try again later.")
    except Exception as e:
        await ctx.send(f'An error occurred: {e}')


@bot.command(name='ball')
async def check_total_amount(ctx, user_name: str):
    """
    Check the total amount for a user.
    Usage: !ball <user name>
    """
    try:
        conn = establish_connection_2()  # Assuming this function establishes a connection to your database
        if conn:
            cursor = conn.cursor()

            # Fetch user ID from users table based on username
            cursor.execute("SELECT id FROM users WHERE username = %s OR ignUsername = %s", (user_name, user_name,))
            result = cursor.fetchone()

            if result:
                user_id = result[0]

                # Fetch total amount from user_totals table based on user ID
                cursor.execute("SELECT total_amount FROM user_totals WHERE user_id = %s", (user_id,))
                result = cursor.fetchone()

                if result:
                    total_amount = result[0]
                    formatted_amount = format_number(total_amount)
                    await ctx.send(f'Total amount for user ** {user_name} **: {formatted_amount}')
                else:
                    await ctx.send(f'No total amount found for user ** {user_name} **.')
            else:
                await ctx.send(f'User ** {user_name} ** not found.')

            cursor.close()
            conn.close()
        else:
            await ctx.send("An error occurred while connecting to the database. Please try again later.")
    except Exception as e:
        await ctx.send(f'An error occurred: {e}')


# Function to get participant names from the provided battle board URL
def get_participant_names(json_data):
    try:
        data = json.loads(json_data)
        participant_names = []

        if 'players' in data:
            players = data['players']
            for player_id, player_info in players.items():
                if 'name' in player_info and 'guildName' in player_info:
                    guild_name = player_info['guildName'].lower()
                    if guild_name == 'smurfing monkeys':
                        participant_names.append(player_info['name'])

        return participant_names
    except Exception as e:
        raise ValueError("An error occurred: " + str(e))


# Function to convert public link to API link
def convert_public_link_to_api_link(public_link):
    # Split the public link by '/'
    parts = public_link.split('/')
    # Extract the battle ID from the last part of the URL
    battle_id = parts[-1]
    # Construct the API link using the battle ID
    api_link = f"https://gameinfo.albiononline.com/api/gameinfo/battles/{battle_id}"
    return api_link


# Command to fetch participant names from the provided link and add them to the database
@bot.command(name='add_link')
async def get_link(ctx, amount: int, link: str):
    try:
        api_link = convert_public_link_to_api_link(link)
        response = requests.get(api_link)
        if response.status_code == 200:
            participant_names = get_participant_names(response.text)

            if participant_names:
                conn = establish_connection_2()  # Use the second database for adding to loot split
                if conn:
                    cursor = conn.cursor()
                    added_by_user_id = str(ctx.author.id)
                    tax_percentage = 0  # Assuming no tax for simplicity

                    split_amount = amount * (1 - (tax_percentage / 100))

                    for user_name in participant_names:
                        # Fetch user ID from users table based on username
                        cursor.execute("SELECT id FROM users WHERE username = %s OR ignUsername = %s",
                                       (user_name, user_name,))
                        result = cursor.fetchone()
                        if result:
                            user_id = result[0]
                            # Insert the split amount for the user into the database
                            sql = 'INSERT INTO loot_splits (user_id, split_amount, split_date, added_by_user_id) VALUES (%s, %s, NOW(), %s)'
                            cursor.execute(sql, (user_id, split_amount, added_by_user_id))
                            conn.commit()
                            # Send a message for each user added to the database
                            formatted_amount = format_number(split_amount)
                            await ctx.send(
                                f"Money added to **{user_name}'s** loot split successfully! Amount: {formatted_amount}")
                        else:
                            await ctx.send(f'User **{user_name}** not found in the database.')

                    cursor.close()
                    conn.close()
                else:
                    await ctx.send("An error occurred while connecting to the database. Please try again later.")
            else:
                await ctx.send("No participant names found in the provided link.")
        else:
            await ctx.send("Failed to fetch battle board data. Please check the link and try again.")
    except Exception as e:
        await ctx.send(f'An error occurred: {e}')


# Function to retrieve all logs from the spreadsheet and sort them by total amount
def get_all_logs(spreadsheet_key):
    try:
        # Authenticate with Google Sheets
        client = authenticate()

        # Open the spreadsheet using the provided key
        spreadsheet = client.open_by_key(spreadsheet_key)
        sheet = spreadsheet.sheet1
        all_values = sheet.get_all_values()

        # Create a dictionary to store the total amount for each unique name
        user_logs = {}

        # Iterate over each row in the spreadsheet
        for row in all_values[1:]:
            # Check if the row is empty or if the 'Amount' column is not numeric
            if not row or not row[5].isdigit():
                continue

            # Extract the player name and player guild from the row
            player_name = row[1]
            player_guild = row[6]

            # Convert the 'Amount' column to an integer
            amount = int(row[5])

            # Add the player name to the user_logs dictionary if it doesn't exist
            if player_name not in user_logs:
                user_logs[player_name] = 0

            # Add the player guild name to the user_logs dictionary if it doesn't exist
            if player_guild not in user_logs:
                user_logs[player_guild] = 0

            # Increment the total amount for the player name and player guild
            user_logs[player_name] += amount
            user_logs[player_guild] += amount

        # Sort the dictionary by total amount (ascending order)
        sorted_user_logs = dict(sorted(user_logs.items(), key=lambda item: item[1]))

        return sorted_user_logs

    except Exception as e:
        logging.error(f"Error retrieving logs: {e}")
        return None


user_logger = "1ZKroN93892iTp8WfgcHT75uIFDflsJGvtafN8MSj6i8"  # Spreadsheet key


@bot.command(name='total_logger')
async def get_all_user_logs(ctx, spreadsheet_key=user_logger):
    from tabulate import tabulate
    try:
        # Get all logs from the spreadsheet
        all_logs = get_all_logs(spreadsheet_key)
        if all_logs:
            # Sort the dictionary by total amount (ascending order)
            sorted_user_logs = dict(sorted(all_logs.items(), key=lambda item: item[1]))

            # Format logs as a table using tabulate
            table = [["User", "Total Amount"]]
            for username, total_amount in sorted_user_logs.items():
                table.append([username, total_amount])

            # Format the table with tabulate
            formatted_table = tabulate(table, headers='firstrow', tablefmt='fancy_grid')

            # Paginate the formatted table
            message_chunks = split_message_chunks(formatted_table)

            # Send the paginated message
            current_page = 0
            message = await ctx.send(
                f"```Page {current_page + 1}/{len(message_chunks)}\n{message_chunks[current_page]}```")

            # Add pagination reactions
            await message.add_reaction('⬅️')
            await message.add_reaction('➡️')

            # Pagination loop
            while True:
                try:
                    reaction, user = await bot.wait_for('reaction_add',
                                                        check=lambda r, u: u == ctx.author and str(r.emoji) in ['⬅️',
                                                                                                                '➡️'])

                    if str(reaction.emoji) == '⬅️':
                        current_page = max(0, current_page - 1)
                    elif str(reaction.emoji) == '➡️':
                        current_page = min(len(message_chunks) - 1, current_page + 1)

                    # Update the message with the new page
                    await message.edit(
                        content=f"```Page {current_page + 1}/{len(message_chunks)}\n{message_chunks[current_page]}```")
                    await reaction.remove(user)

                except Exception as e:
                    await ctx.send(f"An error occurred: {e}")

        else:
            await ctx.send("No logs found in the spreadsheet.")

    except Exception as e:
        await ctx.send(f"An error occurred: {e}")


@bot.command(name='helpp')
async def help_command(ctx):
    embed = discord.Embed(
        title="Bot Commands",
        description="List of available commands and their usage:",
        color=discord.Color.blue()
    )

    embed.add_field(
        name="__**!signup <ign_username>**__",
        value="Allows users to sign up with their in-game username.",
        inline=False
    )

    embed.add_field(
        name="__**!delete_user <@user1> <@user2> ...**__",
        value="Allows admins to delete users from the database.",
        inline=False
    )

    embed.add_field(
        name="__**!add ( <amount> <user_names> <tax_percentage> )**__ ",
        value="Add money to the loot split.",
        inline=False
    )

    embed.add_field(
        name="__**!payout (IGN_user_names)**__",
        value="Initiate a payout to users.",
        inline=False
    )

    embed.add_field(
        name="__**!ball IGN_user_name**__",
        value="Check the total amount for a user.",
        inline=False
    )

    embed.add_field(
        name="__**!add_link <amount> <link>**__",
        value="Fetch participant names from the provided link and add them to the database.",
        inline=False
    )

    await ctx.send(embed=embed)


if __name__ == "__main__":
    bot.run(os.getenv('BOT_TOKEN2'))
