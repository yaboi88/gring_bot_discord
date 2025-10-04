#!/usr/bin/python3

import discord
from discord.ext import tasks, commands
import datetime
import json
from pandas import (
    DataFrame,
    concat
)

intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.members = True
intents.message_content = True  # Required to read messages

bot = commands.Bot(command_prefix="!", intents=intents)

user = None


def updateGrindDataframe(name, dataFrame, mention=0, post=0):
    '''
    Function which either creates or updates a dataframe with post and reply
    data
    '''
    assert not (mention and post)
    if name in dataFrame.index.values.tolist():
        dataFrame.at[name, 'Total'] += 1
        dataFrame.at[name, 'Mentions'] += mention
        dataFrame.at[name, 'Posts'] += post
    else:
        newRow = DataFrame({
                        'Total': 1,
                        'Mentions': mention,
                        'Posts': post},
                        index=[name])
        dataFrame = concat([dataFrame, newRow])
    return dataFrame


def communityNotes(message):
    '''
    If the most common reactions is ❌, then the message should not be counted
    '''
    crossHighest = False
    largestCount = 0
    for reaction in message.reactions:
        # If we have found cross emoji and new emoji is higher.
        # Return false as we have already determined cross emoji is not the
        # most common emoji
        if (crossHighest is True and largestCount < reaction.count):
            return False

        if (reaction.emoji == "❌" and reaction.count >= largestCount):
            crossHighest = True
        if largestCount > reaction.count:
            largestCount = reaction.count

    return crossHighest


async def handleChannel(channel, guild):
    '''
    Function which creates and posts a dataframe containing
    all post and mention information for all users in a channel
    '''
    now = datetime.datetime.utcnow()
    weekAgo = now - datetime.timedelta(days=7)
    twoWeeksAgo = weekAgo - datetime.timedelta(days=14)
    messages = channel.history(after=weekAgo)
    messagesTwoWeeksAgo = channel.history(before=weekAgo, after=twoWeeksAgo)

    grindTotals = DataFrame()

    # Find the messages from 2 weeks ago
    messagesTwoWeek = 0
    async for message in messagesTwoWeeksAgo:
        messagesTwoWeek += 1

    # Find messages from last week
    messagesWeek = 0
    async for message in messages:
        messagesWeek += 1
        if communityNotes(message):
            continue

        member = guild.get_member(message.author.id)
        if member.display_name is not None:
            name = member.display_name
        else:
            name = message.author.name
        mentions = set(message.mentions)
        for mention in mentions:
            memberMention = guild.get_member(message.author.id)
            if memberMention.display_name is not None:
                mentionName = memberMention.displayName
            else:
                mentionName = mention.name
            grindTotals = updateGrindDataframe(mentionName,
                                               grindTotals,
                                               mention=1)
        grindTotals = updateGrindDataframe(name,
                                           grindTotals,
                                           post=1)
    grindTotals = grindTotals.sort_values('Total', ascending=False)

    msg = (f"There were {messagesWeek} posted this week\n"
           f"A change of {messagesWeek - messagesTwoWeek} from week before\n"
           f"Top 10 posters\n"
           f"```\n{grindTotals[grindTotals.columns[0]].to_markdown()}\n```")

    if user is not None:
        await channel.send(msg)
        await user.send(msg)

    print(msg)


# creating a loop that runs every day at 3 PM UTC
@tasks.loop(time=datetime.time(hour=15, minute=0))
async def job_loop():
    weekday = datetime.datetime.utcnow().weekday()
    if (weekday == 6):  # Sunday
        for guild in bot.guilds:
            for channel in guild.channels:
                if 'grind-25' in channel.name:
                    await handleChannel(channel, guild)


@bot.event
async def on_ready():
    '''
    Once successfully logged in, this method runs through every server which
    has this bot installed and collect post and reply information
    '''
    if not job_loop.is_running():
        job_loop.start()
    print(f'Logged in as {bot.user}')


if __name__ == "__main__":
    # Get Json info from file
    with open('token.json') as f:
        tokenInfo = json.load(f)

    global user
    user = tokenInfo['user']

    bot.run(tokenInfo['token'])
