# Copyright (C) 2020  Osama Khalid
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError
from datetime import date
from http.client import HTTPException
from urllib.error import URLError
import csv
import json
import string
import sys


WIKIDATA_DAY_FORMAT = '+%Y-%m-%dT00:00:00Z/11'
TODAY = date.today().strftime('+%Y-%m-%dT00:00:00Z/11')

def load_ignored(filename=None):
    ignored_users = {}

    if filename:
        try:
            with open(filename) as ignored_file:
                ignored_users = json.load(ignored_file)
        except FileNotFoundError:
            with open(filename, 'w') as ignored_file:
                json.dump(ignored_users, ignored_file)
    return ignored_users

def update_ignored(filename, value, flag):
    if filename:
        ignored_users = load_ignored(filename)
        ignored_users[value] = flag
        with open(filename, 'w') as ignored_file:
            json.dump(ignored_users, ignored_file)

def load_previous(filename=None):
    previous = []

    if filename:
        try:
            with open(filename) as f:
                reader = csv.reader(f, dialect=csv.excel_tab)
                for row in reader:
                    try:
                        qid = row[0]
                        value = row[2]
                    except IndexError:
                        # If line is empty
                        continue
                    previous.append((qid, value))
        except FileNotFoundError:
            pass
    return previous

def get_clean_response(query, user_agent=None, previous_filename=None,
                       ignored_filename=None, verbose=True):
    if not user_agent:
        user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])

    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    results = []
    trials = 0
    while trials < 10:
        try:
            response = sparql.query().convert()
        except (EndPointInternalError, ConnectionError, URLError, HTTPException) as e:
            print(f"Error: {e}.  Sleeping for 30 seconds...")
            time.sleep(30)
            trials += 1
        break

    results = response['results']['bindings']

    clean_results = []

    # Removal of previous and ignored items
    previous = load_previous(previous_filename)
    ignored = load_ignored(ignored_filename)
    for result in results:
        # Common clean-up: The full URL is useless
        qid = result['item']['value'].replace('http://www.wikidata.org/entity/', '')
        result['item']['value'] = qid

        value = result['value']['value']

        if (qid, value) in previous:
            if verbose:
                print(f"{qid} was scanned before.  Skipping...")
            continue
        if value in ignored:
            if verbose:
                print(f"{qid} was skipped before ({ignored[value]}).")
                continue
        clean_results.append(result)

    return clean_results, len(response['results']['bindings'])

def get_next_startswith(startswith, chars, skip_until=False):
    while True:
        if skip_until and len(startswith) == 0:
            startswith = chars[0]
        elif len(startswith) == 0:
            return
        elif skip_until and startswith[-1] == chars[-1]:
            startswith += chars[0]
            break
        elif startswith[-1] == chars[-1]:
            # If we reached the end of one level
            # (e.g. 4-letter), move back to the level before
            # it (e.g. 3-letter)
            startswith = startswith[:-1]
        else:
            last_char = startswith[-1] 
            next_index = chars.index(last_char) + 1
            startswith = startswith[:-1] + chars[next_index]
            break

    return startswith

def get_results(query, previous_filename=None, last_filename=None,
                ignored_filename=None, extra_chars="",
                user_agent=None, verbose=True):
    # Check if the query is incremental
    if "%s" in query:
        if last_filename:
            try:
                with open(last_filename) as f:
                    skip_until = f.read().strip()
            except FileNotFoundError:
                skip_until = None
        else:
            skip_until = None

        chars = string.ascii_letters + string.digits + '_' + extra_chars
        if skip_until:
            startswith = skip_until
        else:
            startswith = ""
        while True:
            if verbose:
                if startswith:
                    print(f"Getting items that start with \"{startswith}\"...")
                else:
                    print("Getting items without a STARTSWITH filter...")

            results, unclean_count = get_clean_response(query % startswith,
                                                        user_agent,
                                                        previous_filename,
                                                        ignored_filename,
                                                        verbose=verbose)
            if unclean_count < 3000:
                if verbose:
                    print(f"We got {unclean_count} results ({len(results)} cleaned).")

                for result in results:
                    yield result

                startswith = get_next_startswith(startswith, chars)
                
                if startswith is None:
                    return

                if last_filename:
                    with open(last_filename, 'w') as f:
                        f.write(startswith)

            else:
                # If we fail to grap all results, add a new character
                # level
                startswith += chars[0]
                if verbose:
                    print("We got more than 3000 results.  Let's "
                          "get more specific.  Working with "
                          f"{startswith}...")
    else:
        results = get_clean_query(query, user_agent,
                                  previous_filename,
                                  ignored_filename,
                                  verbose=verbose)
        return results
