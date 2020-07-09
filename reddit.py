# A script to fetch subreddit title, start dates and language to
# generate QuickStatements
# Copyright (C) 2020  Osama Khalid
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
from datetime import datetime
from prawcore.exceptions import Redirect, NotFound, Forbidden
import praw
import time

import utils
import secrets


VERBOSE = True
OUTPUT_FILE = 'output-reddit.txt'
LAST_FILE = 'reddit_last.txt'
IGNORED_FILE = 'reddit_ignored.json'
QUERY = """
SELECT ?item ?value
WHERE
{
        ?item p:P3984	 ?statement .
        VALUES ?pq { pq:P580 } .
        OPTIONAL {
                ?statement ?pq ?qualif .
        } .
        FILTER( !BOUND( ?qualif ) ) .
        ?prop wikibase:qualifier ?pq .
        ?statement  ps:P3984 ?value .
}
LIMIT 3000
"""



reddit = praw.Reddit(client_id=secrets.reddit_credentials['client_id'],
                     client_secret=secrets.reddit_credentials['client_secret'],
                     user_agent="Wikidata Quickstatement generator")

for result in utils.get_results(QUERY, OUTPUT_FILE, LAST_FILE,
                                IGNORED_FILE, verbose=VERBOSE):
    with open(OUTPUT_FILE, 'a') as fw:
        statement = ""
        qid = result['item']['value']
        username = result['value']['value']

        inaccessible = False
        trials = 0
        while trials < 5:
            try:
                subreddit = reddit.subreddit(username)
                created_utc = subreddit.created_utc
            except (NotFound, Redirect):
                inaccessible = True
                error_code = '404'
                break
            except Forbidden:
                inaccessible = True
                error_code = '403'
                break                
            else:
                break

        if inaccessible:
            utils.update_ignored(IGNORED_FILE, username,
                                 error_code)
            continue

        statement = f'{qid}\tP3984\t"{username}"'

        created_date = datetime.utcfromtimestamp(created_utc).strftime(utils.WIKIDATA_DAY_FORMAT)
        statement += f'\tP580\t{created_date}'

        statement += f'\tP1810\t"{subreddit.title}"'

        if subreddit.over18:
            statement += '\tP1552\tQ2716583'

        statement += '\tP407\t'
        if subreddit.lang == 'en':
            statement += 'Q1860'
        elif subreddit.lang == 'fr':
            statement += 'Q150'
        elif subreddit.lang == 'de':
            statement += 'Q188'
        elif subreddit.lang == 'pt':
            statement += 'Q5146'
        elif subreddit.lang == 'ru':
            statement += 'Q7737'
        elif subreddit.lang == 'pl':
            statement += 'Q809'
        elif subreddit.lang == 'zh':
            statement += 'Q7850'
        elif subreddit.lang == 'it':
            statement += 'Q652'
        elif subreddit.lang == 'bg':
            statement += 'Q7918'
        elif subreddit.lang == 'es':
            statement += 'Q1321'
        elif subreddit.lang == 'uk':
            statement += 'Q8798'
        elif subreddit.lang == 'hu':
            statement += 'Q9067'
        elif subreddit.lang == 'hr':
            statement += 'Q6654'
        elif subreddit.lang == 'lv':
            statement += 'Q9078'
        else:
            statement += subreddit.lang
            

        if VERBOSE:
            print(statement)
        fw.write(statement + '\n')
        # Make sure we are always within the rate limit of reddit
        time.sleep(0.8)
