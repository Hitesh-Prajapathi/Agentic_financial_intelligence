# Skill: tweet scraping
Apify actor (`APIFY_ACTOR_ID`, default `apidojo/tweet-scraper`). Query: `($TICKER OR "Company") lang:en -filter:retweets since:… until:…`
(cashtag only for ambiguous short tickers). Guards: monthly budget cap, per-run hard spend cap, 6 h cache,
2 h cooldown after an empty result (Apify bills empty runs too), quality filter (no retweets/spam/short posts).
