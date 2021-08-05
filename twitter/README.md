# Twitter API
*This readme/code was last updated May 2021, and has not been updated since then because it seemed unlikely that using Twitter data (and then inferring overall NZ sentiment) would be approved within Stats.*

There is two versions of the Twitter API: v1.1 and v2 ([full documentation](https://developer.twitter.com/en/docs/twitter-api/rate-limits)). The new version (v2) was released Aug 2020 and is _Early Access_ since Twitter has not implemented all the types of users/plans. The (rough) timeline for completely replacing v1.1 with v2 can be found [here](https://developer.twitter.com/en/products/twitter-api/early-access/guide#rollingout).

## Can Stats NZ use this?
When signing up for an "Individual developer account" there are some screening questions before the account can be approved. It is not clear what would prevent an account from being approved (ie. whether Stats NZ would be allowed). These were (paraphrased):
- General intended use
- How would you analyze Twitter data?
- Tweet, retweet or like? (Which parts of the data/API will you use?)
- Will you show Tweets or Twitter information off Twitter?
- **Will you be providing Tweets or Twitter information to government entites?**

The questions for the other types of developer accounts are likely similar to these.

**Also, Twitter has [restrictions when using the Twitter APIs](https://developer.twitter.com/en/developer-terms/more-on-restricted-use-cases) which include deriving/inferring sensitive information about Twitter uses, matching a Twitter account with an "off-Twitter identifier", and the redistribution of downloaded Twitter content.**

## Plans
API v1.1 has standard (free), premium and enterprise account types. The full details for standard and premium can be found [here](https://developer.twitter.com/en/pricing/search-fullarchive) or seen below. Signing up for the enterprise plan (and getting details of what the plan offers) requires contacting Twitter. The standard and premium plans allowing querying the full Twitter archives, but the standard plan is limited to 50 requests/month as it is the free option. The premium option is priced depending on the number of total request per month, starting with 99 USD for "Up to 100" requests and ending with 1,899 USD for "Up to 2,500" requests (look in link above for intermediate pricing).

Package                | Standard/Sandbox                    | Premium
:--------------------- | ----------------------------------- | --------------------------------------
Rate limit             | 30 requests/min AND 10 requests/sec | 60 requests/min AND 10 requests/sec
Tweets per request     | 100                                 | 500
Effective Tweets limit | 3000 tweets/min AND 1000 tweets/min | 30,000 tweets/min AND 5,000 tweets/min

API v2 has standard (free) and academic research account types. The free tier only allows the retrieval of Tweets within the previous week, while the academic tier has access to the full Twitter archive.

## Retrieving NZ Tweets from 2020 onwards
API v1.1 has the "geocode" operator which can be used to return Tweets within a given radius of a given latitude-longitude coordinate. This could be used (with a few different circles) to get Tweets from New Zealand. From brief testing, this seems to be limited to Tweets within the last week, but [this page](https://developer.twitter.com/en/pricing/search-fullarchive) implies that there is a way to access the full archives.

Any location-related operators in API v2 require an academic research account. The location operators can be used to request Tweets that are: tagged with a specific location name (place), from a given country (place_country) or are within two types of latitude-longitude areas (point_radius, bounding_box). Also, using v2 to search for Tweets before a week ago requires an academic research account.
