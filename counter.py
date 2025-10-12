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

user = ""
channelName = None


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
        if largestCount < reaction.count:
            largestCount = reaction.count

    return crossHighest


def getGrinds(message, dataFrame, guild):
    if communityNotes(message):
        return dataFrame

    member = guild.get_member(message.author.id)
    if member.display_name is not None:
        name = member.display_name
    else:
        name = message.author.name
    mentions = set(message.mentions)
    for mention in mentions:
        memberMention = guild.get_member(mention.id)
        if memberMention.display_name is not None:
            mentionName = memberMention.display_name
        else:
            mentionName = mention.name
        dataFrame = updateGrindDataframe(mentionName,
                                         dataFrame,
                                         mention=1)

    dataFrame = updateGrindDataframe(name,
                                     dataFrame,
                                     post=1)
    return dataFrame


async def handleChannel(channel, guild):
    '''
    Function which creates and posts a dataframe containing
    all post and mention information for all users in a channel
    '''
    now = datetime.datetime.utcnow()
    weekAgo = now - datetime.timedelta(days=7)
    twoWeeksAgo = now - datetime.timedelta(days=14)
    messages = channel.history(after=weekAgo)
    oldMessages = channel.history(after=twoWeeksAgo)

    grindTotals = DataFrame()
    oldGrindTotals = DataFrame()

    # Find the messages from 2 weeks ago
    async for message in oldMessages:
        oldGrindTotals = getGrinds(message, oldGrindTotals, guild)

    async for message in messages:
        grindTotals = getGrinds(message, grindTotals, guild)

    grindTotals = grindTotals.sort_values('Total', ascending=False)

    totalWeekly = grindTotals['Total'].sum()
    oldTotal = oldGrindTotals['Total'].sum()

    msg = (f"There were {totalWeekly} posts and mentions this week\n"
           f"A change of {oldTotal - totalWeekly} from week before\n"
           f"Posters\n"
           f"```\n{grindTotals[grindTotals.columns[0]].to_markdown()}\n```")

    # await channel.send(msg)

    print(msg)


# creating a loop that runs every day at 3 PM UTC
@tasks.loop(time=datetime.time(hour=9, minute=0))
async def job_loop():
    weekday = datetime.datetime.utcnow().weekday()
    if (weekday == 6):  # Sunday
        for guild in bot.guilds:
            for channel in guild.channels:
                if channelName in channel.name:
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

    user = tokenInfo['user']

    channelName = tokenInfo['channel']

    bot.run(tokenInfo['token'])
