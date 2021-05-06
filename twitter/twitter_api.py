import requests
import json

import twitter_config  # local file


# clear previous logs
with open("twitter_log.txt", mode='w') as f:
    f.write('')
# add to current logs
def log(message):
    with open("twitter_log.txt", mode='a') as f:
        f.write(str(message) + '\n')
    print(message)


headers = {'Authorization': f"Bearer {twitter_config.get_app_bearer()}"}
obscure_query = "?q=axinomancy&tweet_mode=extended"
nz_query = "?geocode=-41.29738,173.21863,1000km&tweet_mode=extended&count=100"
# If tweet_mode=extended is not specified, response will have tweets truncated to 140 characters
base_url = "https://api.twitter.com/1.1/search/tweets.json"

more_results = True
current_query = nz_query
all_tweets = []

while more_results:
    r = requests.get(base_url + current_query, headers=headers)
    tweets = json.loads(r.content.decode())

    # Unpacking contents
    log(f"API LOAD #{len(all_tweets) + 1}")
    log(tweets['search_metadata'])
    log('')
    log(tweets['statuses'][0].keys())
    log('')
    log(tweets['statuses'][0]['user'].keys())
    log('')
    
    if len(tweets['statuses']) == 0:
        more_results = False
        break

    for i, status in enumerate(tweets['statuses']):
        log(f"Post {i + 1} @ {status['created_at']}")
        log(f"Name: {status['user']['name']}")
        log(f"@username: {status['user']['screen_name']}")
        log(f"Verified: {status['user']['verified']}")
#         log(status['user'])

        log(f"Attached GEO: {status['geo']}")
        log(f"Profile GEO: {status['user']['location']}")    

        log("Tweet:")
        if status['truncated']:
            log(repr(status['text']))
        else:
            log(repr(status['full_text']))

        log('')
    print(len(tweets['statuses']))
    all_tweets.append(tweets['statuses'])
    
    if len(tweets['statuses']) > 0:# and len(all_tweets) < 20:
        current_query = tweets['search_metadata']['next_results'] + "&tweet_mode=extended"
    else:
        more_results = False
        
log(f"Number of retrieved tweets: {(len(all_tweets) - 1) * 100 + len(all_tweets[-1])}")

