from datetime import datetime
from http.client import HTTPException
from lxml import objectify
import time
import urllib
import utils


OUTPUT_FILE = 'output-youtube-.txt'
LAST_FILE = 'youtube_published_last.txt'
QUERY = """
SELECT ?item ?value 
WHERE
{
	?item p:P2397 ?statement .
	VALUES ?pq { pq:P580 } .
	OPTIONAL {
		?statement ?pq ?qualif .
	} .
	FILTER( !BOUND( ?qualif ) ) .
	?prop wikibase:qualifier ?pq .
	?statement ps:P2397 ?value .
        FILTER(STRSTARTS(?value, "UC%s")).
}
LIMIT 3000
"""

for result in utils.get_results(QUERY, [OUTPUT_FILE], LAST_FILE, extra_chars="-"):
    with open(OUTPUT_FILE, 'a') as fw:
        for result in results:
            statement = ""
            qid = result['item']['value']
            channel_id = result['value']['value']

            feed_url = f'https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}'

            trials = 0
            start_time = title = None
            while trials < 10:
                try:
                    with urllib.request.urlopen(feed_url) as r:
                        doc = objectify.parse(r)
                        try:
                            start_time = str(doc.getroot().published)
                            title = str(doc.getroot().title)
                        except AttributeError:
                            print(f"Couldn't find published/title in {feed_url}. Skipping...")
                            break
                        clean_start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S+00:00')
                        clean_start_time = clean_start_time.strftime(utils.WIKIDATA_DAY_FORMAT)
                except urllib.error.HTTPError:
                    break
                except (HTTPException, urllib.error.URLError):
                    print("Failed. Sleeping for 10 seconds...")
                    time.sleep(10)
                    trials += 1
                else:
                    break

            if not start_time:
                continue

            statement = f'{qid}\tP2397\t"{channel_id}"\tP1810\t"{title}"\tP580\t{clean_start_time}'
            print(statement)
            fw.write(statement + '\n')