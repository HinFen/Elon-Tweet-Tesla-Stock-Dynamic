import sys
import pandas as pd
from pyspark.sql import SparkSession, types, functions
from datetime import datetime, timedelta
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar

spark = SparkSession.builder.appName("ElonTweets").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

tweet_schema = types.StructType(
    [
        types.StructField("id1", types.IntegerType()),
        types.StructField("id2", types.IntegerType()),
        types.StructField("datetime", types.StringType()),
        types.StructField("tweet id", types.LongType()),
        types.StructField("text", types.StringType()),
        types.StructField("username", types.StringType()),
        types.StructField("location", types.StringType()),
        types.StructField("reply count", types.IntegerType()),
        types.StructField("retweet count", types.IntegerType()),
        types.StructField("like count", types.IntegerType()),
        types.StructField("language", types.StringType()),
        types.StructField("twitter access point", types.StringType()),
        types.StructField("follower count", types.IntegerType()),
        types.StructField("friends count", types.IntegerType()),
        types.StructField("verified", types.StringType()),
        types.StructField("date", types.StringType()),
        types.StructField("mentions", types.StringType()),
        types.StructField("sentiment", types.StringType()),
    ]
)


# datetime is in UTC format
# UTC market opens at 13:30 and Closes at 20:00
# if he tweets after it closes and before it opens then we move the date to the next day the market is open
def adjust_tweet_datetime(tweet_datetime):
    market_close_time = datetime.strptime("20:00", "%H:%M").time()

    tweet_datetime = datetime.strptime(tweet_datetime, "%Y-%m-%d %H:%M:%S%z")

    # Check if the tweet is after market close
    if market_close_time < tweet_datetime.time():
        # Move the date to the next day
        tweet_datetime += timedelta(days=1)

    # returns the date without out the time
    return tweet_datetime.date()


# checks if the passed in date is a holiday or a weekend
def is_weekend_or_holiday(tweet_date, holidays):
    return tweet_date.weekday() >= 5 or tweet_date in holidays


# adjusts the passed in date so that it is on the same date that it is first possible
# to do a trade after the tweet has been posted
def adjust_date(tweet_date):
    cal = calendar()
    holidays = cal.holidays(tweet_date.year, tweet_date.year + 1)
    # we have to do it like this in case after changing its date its another holiday or weekend
    while is_weekend_or_holiday(tweet_date, holidays):
        tweet_date += timedelta(days=1)
    return tweet_date


# returns the type of sentiment a tweet has
def get_sentiment_type(tweet_sentiment):
    # Converts the string into a pair
    pair_string = tweet_sentiment.strip("[]")
    sentiment, _ = pair_string.split(", ")
    sentiment = sentiment.strip("''")

    return sentiment


# returns the sentiment score a tweet has
def get_sentiment_score(tweet_sentiment):
    # Converts the string into a pair
    pair_string = tweet_sentiment.strip("[]")
    _, score = pair_string.split(", ")
    score = float(score)

    return score


# makes some udf functions for spark
udf_adjust_tweet_datetime = functions.udf(adjust_tweet_datetime, types.DateType())
udf_adjust_date = functions.udf(adjust_date, types.DateType())
udf_get_sentiment_type = functions.udf(get_sentiment_type, types.StringType())
udf_get_sentiment_score = functions.udf(get_sentiment_score, types.FloatType())


def main(input_file):
    # elon_tweets = spark.read.csv(input_directory, schema=tweet_schema, header=False)
    # we can read it in as a pandas dataframe at first because the file is only 4 mb which can fit on main memory
    # and the reason we dont directly want to read it as a spark df is because spark cant read zip files
    # and if we unzip the format of the .csv breaks (due to format of text field)and we cant read in rows properly
    elon_tweets = pd.read_csv(input_file)

    # converts pandas dataframe to a spark dataframe
    elon_tweets = spark.createDataFrame(elon_tweets, schema=tweet_schema)
    # remove unwanted columns
    elon_tweets = elon_tweets.drop(
        "id1",
        "id2",
        "tweet id",
        "username",
        "location",
        "retweet count",
        "like count",
        "twitter access point",
        "follower count",
        "friends count",
        "verified",
        "date",
        "mentions",
    )

    # Get the dates column in the right format and if he tweets after market
    # closes and before it opens then we move the date to the next day the market is open
    elon_tweets = elon_tweets.withColumn(
        "datetime", udf_adjust_tweet_datetime("datetime")
    ).withColumnRenamed("datetime", "date")

    # adjusts the passed in date so that it is on the same date that it is first possible
    # to do a trade after the tweet has been posted
    elon_tweets = elon_tweets.withColumn("date", udf_adjust_date("date"))

    # splits the sentiment (pair) into two seperate columns
    elon_tweets = elon_tweets.withColumn(
        "sentiment_type", udf_get_sentiment_type("sentiment")
    ).withColumn("sentiment_score", udf_get_sentiment_score("sentiment"))

    # cached here because elon_tweets will be used to create filtered tweets df and we want to ouptut it
    elon_tweets = elon_tweets.cache()

    # Filtering for tweets with only emojis, @, and links
    en_tweets = elon_tweets.filter(elon_tweets["language"] == "en")
    en_tweets = en_tweets.drop("language", "sentiment")

    # Filtering for tweets with >= median number of retweets
    median_retweets = (
        en_tweets.groupBy()
        .agg(functions.median("reply count").alias("median"))
        .first()["median"]
    )

    # only keeps tweets with >= median_retweets
    filtered_tweets = en_tweets.filter(en_tweets["reply count"] >= median_retweets)

    # gets all the text in lowercase
    filtered_tweets = filtered_tweets.withColumn(
        "text", functions.lower(functions.col("text"))
    )

    # only keeps tweets that have text with the keyword tesla in it
    filtered_tweets = filtered_tweets.filter(filtered_tweets.text.contains("tesla"))

    filtered_tweets = filtered_tweets.drop("text")
    elon_tweets = elon_tweets.drop("text", "language", "sentiment")

    # writes out the files
    filtered_tweets.write.csv(
        "etl_data/filteredTweets/", compression=None, mode="overwrite"
    )
    elon_tweets.write.csv(
        "etl_data/tweets/", compression=None, mode="overwrite"
    )

if __name__ == "__main__":
    input_file = sys.argv[1]
    main(input_file)
