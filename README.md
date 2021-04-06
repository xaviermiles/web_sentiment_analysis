# c19-sentiment-analysis

## Hedonometer
The main implementation is at http://hedonometer.org/, which provides a daily index of the positivity of people’s tweets (Twitter). This is informed by the Twitter Decahose API, which provides a 10% random sample of all posts on Twitter (approx. 50 million tweets per day in 100GB of raw JSON).  The daily score reflects which words are being use most (about 200 million unique per day) and their associated positivity, and is constructed using a bag-of-words model. More formally, the weighted average level of happiness for a given text T is calculated as

h_avg (T)= ∑_(i=1)^N h_avg(w_i) p_i,

where h_avg(w_i) is their estimate of the average happiness of a given word w_i, p_i is the normalised frequency of that word w_i, and there is N unique words.

The methodology behind this hedonometer is described in _Temporal patterns of happiness and information in a global social network: Hedonometrics and Twitter_ (2011; Dodds et al.).

### How did they get “happiness scores” for each word?
Using Amazon Mechanical Turk, they paid users to rate how 10,222 different words made them feel on a (integer) scale from 1-9, obtaining 50 individual evaluations per word. They are confident in the derived happiness scores as they strongly agree with the scores obtained for 1034 words in the Affective Norms for English Words (ANEW) study (correlation coefficient r = 0.944 and p-value < 10^-10), despite the new scores being obtained from global users (Mechanical Turk) and the scores from ANEW being obtained exclusively from University of Florida students.
Their English happiness scores can be viewed on http://hedonometer.org/words/labMT-en-v2/ or downloaded from https://hedonometer.org/api/v1/words/?format=json&wordlist__title=labMT-en-v2.

### Combining the scores for each word.
The overall happiness score ignores words which have a happiness score between 4 and 6 (somewhat neutral). This exclusion range is a tuning parameter; a smaller range will provide more robustness to outlier/extreme events but less day-to-day sensitivity, and a wider range will provide more sensitivity but less robustness. This exclusion of somewhat neutral words results in only 3,686 of the original 10,222 words being used (23%).

### Other related 
The main indicator of public sentiment is the _Time Series of Happiness_, which is informed by posts on Twitter. There are other related projects on the site: _Happiness of Stories_ which is informed by books and movies, _Happiness of the News_ which is informed by the New York Times and CBS, and _Happiness of Outside_ which reflects a collection of articles on a blogging site about outdoor activities. These projects all present per word average happiness shift between two bodies of text (e.g. newspaper section, article), and do not include time series plots. The authors are strong proponents of the word shift presentation, as it allows for more information about why sentiment has changed between two time periods (or bodies of text). 

The NZ news sentiment lineplot/visualisation could include these word shifts as additional information like how hedonometer presents these as overlays. For a given day, the hedonometer uses these word shifts to show any words which are being used significantly more or less than the previous week. The hedonometer also uses Wikipedia to automatically detect important/significant events on any given day.
