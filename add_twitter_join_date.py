# A script to fetch Twitter account start dates and generate
# QuickStatements
# Copyright (C) 2020  Osama Khalid
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
from bs4 import BeautifulSoup
from datetime import datetime
from http.client import HTTPException
import csv
import requests
import time
import urllib

import utils


VERBOSE = True
OUTPUT_FILE = 'output-twitter.txt'
LAST_FILE = 'twitter_join_last.txt'
IGNORED_FILE = 'twitter_numerical.json'
QUERY = """SELECT ?item ?value
WHERE
{
        ?item p:P2002 ?statement .
        ?statement pq:P6552 ?twitter_id .
        VALUES ?pq { pq:P580 } .
        OPTIONAL {
                ?statement ?pq ?qualif .
        } .
        FILTER( !BOUND( ?qualif ) ) .
        ?prop wikibase:qualifier ?pq .
        ?statement  ps:P2002 ?value .
        FILTER(STRSTARTS(?value, "%s")).
}
LIMIT 3000
"""


for result in utils.get_results(QUERY, OUTPUT_FILE, LAST_FILE,
                                IGNORED_FILE, verbose=VERBOSE):
    with open(OUTPUT_FILE, 'a') as fw:
        with open('output-websites.csv', 'a') as website_file:
            website_writer = csv.writer(website_file)
            statement = ""
            qid = result['item']['value']
            username = result['value']['value']

            trials = 0
            server_error = False
            while trials < 10:
                if VERBOSE:
                    print(f"Getting {username}...")
                try:
                    response = requests.get('https://nitter.net/' + username, timeout=20)
                except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
                    print(f"Error: {e}.  Sleeping for 1 minute...")
                    time.sleep(60)
                    trials += 1
                    continue
                if response.status_code == 200:
                    server_error = False
                    break
                elif response.status_code == 404:
                    utils.update_ignored(IGNORED_FILE, username, 404) 
                    server_error = False
                    #fw.write(f'-{qid}\tP2002\t"{username}" /* Remove non-existing account */\n')
                    break
                elif response.status_code >= 400:
                    print(f"Error {response.status_code}.  Sleeping for 1 minute...")
                    server_error = True
                    time.sleep(60)
                    trials += 1

            # Do not look for data in case of a server error.
            if server_error:
                continue

            soup = BeautifulSoup(response.text, 'lxml')

            statement = f'{qid}\tP2002\t"{username}"'

            verified_badge = soup.select_one('.profile-card .verified-icon')
            if verified_badge:
                statement += '\tP1552\tQ28378282'

            join_date = soup.select_one('.profile-card .profile-joindate span')
            if join_date:
                clean_join_date = join_date['title'].split(' - ')[1]
                clean_join_date = datetime.strptime(clean_join_date, '%d %b %Y')
                clean_join_date = clean_join_date.strftime(utils.WIKIDATA_DAY_FORMAT)
                statement += f'\tP580\t{clean_join_date}'

            if not (verified_badge or join_date):
                continue

            website = soup.select_one('.profile-card .profile-website a')
            bio = soup.select_one('.profile-card .profile-bio p')
            if bio or website:
                if website:
                    clean_website = website['href']
                else:
                    clean_website = ""

                if bio:
                    clean_bio = bio.decode_contents()
                else:
                    clean_bio = ""

                website_writer.writerow([qid, username, clean_website, clean_bio])
                if VERBOSE:
                    print("Found a website and/or bio.")

            if VERBOSE:
                print(statement)
            fw.write(statement + '\n')
