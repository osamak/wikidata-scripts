from SPARQLWrapper import SPARQLWrapper, JSON
from http.client import HTTPException
import csv
import string
import sys
import urllib


WIKIDATA_DAY_FORMAT = '+%Y-%m-%dT00:00:00Z/11'

def load_previous(filenames):
    previous = []
    for filename in filenames:
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

def get_clean_response(query, user_agent=None, previous_filenames=[]):
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
        except (ConnectionError, urllib.error.URLError, HTTPException) as e:
            print(f"Error: {e}")
            trials += 1
        break

    results = response['results']['bindings']

    # Common clean-up: The full URL is useless
    index = 0
    for result in results:
        qid = result['item']['value'].replace('http://www.wikidata.org/entity/', '')
        results[index]['item']['value'] = qid
        index += 1

    # Removal of previous encounter
    if previous_filenames:
        previous = load_previous(previous_filenames)
    else:
        previous = []

    clean_results = results
    if previous:
        for result in results:
            qid = result['item']['value']
            value = result['value']['value']
            if (qid, value) in previous:
                if verbose:
                    print(f"{qid} was scanned before.  Skipping...")
                clean_results.remove((qid, value))

    return clean_results, len(response['results']['bindings'])

def get_next_startswith(startswith, chars, skip_until=False):
    while True:
        if skip_until and len(startswith) == 0:
            startswith = chars[0]
        elif len(startswith) == 0:
            return
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
    print(f"Returning \"{startswith}\" as a startswith...")
    return startswith

def get_results(query, previous_filenames=[], last_filename=None,
                extra_chars="", user_agent=None,
                verbose=True):
    # Check if the query is incremental
    if "%s" in query:
        if last_filename:
            try:
                with open(last_filename) as f:
                    skip_until = f.read().strip()
            except IOError:
                skip_until = None
        else:
            skip_until = None

        chars = string.ascii_letters + string.digits + '_' + extra_chars
        startswith = ""
        while True:
            if skip_until == startswith:
                skip_until = False
            elif skip_until:
                if verbose:
                    print(f"Skipping {startswith}...")
                startswith = get_next_startswith(startswith, chars,
                                                 skip_until=True)
                continue

            print("requesting", startswith)
            results, unclean_count = get_clean_response(query % startswith,
                                                        user_agent,
                                                        previous_filenames)
            if unclean_count < 3000:
                if verbose:
                    print(f"We got {unclean_count} results using {startswith}.")

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
                for char in chars:
                    startswith += char            
    else:
        results = get_clean_query(query, user_agent,
                                  previous_filenames)
        return results
