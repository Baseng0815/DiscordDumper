#!/usr/bin/env python3

import discord
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

intents = discord.Intents.default()
intents.members = True
client  = discord.Client(intents=intents)
db      = psycopg2.connect(user=os.getenv('DB_USER'), host='127.0.0.1', port=os.getenv('DB_PORT'), database=os.getenv('DB_DB'))
db.set_isolation_level(0)

async def save_channels(guild):
    # download channel messages
    cursor = db.cursor()

    # clean up old text channels table
    print('Cleaning up old text_channels table...')
    cursor.execute('DROP TABLE IF EXISTS text_channels CASCADE;')

    # create table and save text channels
    channels = await guild.fetch_channels()
    print('Creating new text_channels table...')
    cursor.execute(f'CREATE TABLE text_channels (channel_id TEXT PRIMARY KEY, name TEXT NOT NULL);')
    for channel in channels:
        if channel.type == discord.ChannelType.text:
            print(f'Saving channel {channel.name}...')
            cursor.execute(f'INSERT INTO text_channels VALUES (\'{channel.id}\', \'{channel.name}\');')

    print('Finished!')

async def save_members(guild):
    # download user list
    cursor = db.cursor()

    # clean up old members table
    print('Cleaning up old members table...')
    cursor.execute('DROP TABLE IF EXISTS members CASCADE;')

    # create table and save members
    print('Creating new members table...')
    cursor.execute(f'CREATE TABLE members (member_id TEXT PRIMARY KEY, name TEXT NOT NULL, nick TEXT,\
                   avatar_url TEXT NOT NULL);')
    async for member in guild.fetch_members():
        print(f'Saving member {member.name}...')
        cursor.execute(f'INSERT INTO members VALUES (\'{member.id}\', \'{member.name}\',\
                       \'{member.nick if member.nick != None else "NULL"}\', \'{member.avatar_url}\');')

    print('Finished!')

async def save_messages(guild):
    # download all text channel messages
    cursor = db.cursor()

    # clean up old messages table
    print('Cleaning up old messages table...')
    cursor.execute('DROP TABLE IF EXISTS messages CASCADE;')

    # create table and save text channels
    channels = await guild.fetch_channels()
    print('Creating new messages table...')
    cursor.execute(f'CREATE TABLE messages (msg_id TEXT PRIMARY KEY, content TEXT NOT NULL,\
                   channel_id TEXT REFERENCES text_channels(channel_id), member_id TEXT REFERENCES members(member_id), created_on TIMESTAMP NOT NULL);')
    for channel in channels:
        if channel.type == discord.ChannelType.text:
            count = 0
            async for message in channel.history(limit=None):
                print(f'Saving message #{count} from channel {channel.name}...')
                content = message.content.replace("'", "''")
                try:
                    cursor.execute(f'INSERT INTO messages VALUES (\'{message.id}\', \'{content}\',\
                                  \'{message.channel.id}\', \'{message.author.id}\', \'{message.created_at}\');')
                except:
                    print(f'WARNING: couldn\'t insert message \'{message.content}\'')

                count += 1

    print('Finished!')

@client.event
async def on_ready():
    print('We have logged in as {0.user}'.format(client))

    guild = await client.fetch_guild(os.getenv('GUILD'))
    await save_channels(guild)
    await save_members(guild)
    await save_messages(guild)
    await client.close()

client.run(os.getenv('TOKEN'))
