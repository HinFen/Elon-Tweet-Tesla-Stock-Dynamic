import pandas as pd
import glob
import sys

def main(out_directory):
    # reading in the data
    filtered_elon_tweet_files = glob.glob("./etl_data/filteredTweets/*.csv")
    filtered_elon_tweets_list = (
        pd.read_csv(file, index_col=None, header=None, sep=",")
        for file in filtered_elon_tweet_files
    )
    filtered_elon_tweet_data = pd.concat(filtered_elon_tweets_list, ignore_index=True)
    filtered_elon_tweet_data.rename(
        inplace=True, columns={0: "date", 1: "reply count" , 2: "sentiment type", 3: "sentiment score"}
    )

    elon_tweet_files = glob.glob("./etl_data/tweets/*.csv")
    elon_tweets_list = (
        pd.read_csv(file, index_col=None, header=None, sep=",")
        for file in elon_tweet_files
    )
    elon_tweet_data = pd.concat(elon_tweets_list, ignore_index=True)
    elon_tweet_data.rename(
        inplace=True, columns={0: "date", 1: "reply count", 2: "sentiment type", 3: "sentiment score"}
    )

    tesla_stock_data = pd.read_csv("etl_data/cleaned_tesla_stock_data.csv")
    sp500_stock_data = pd.read_csv("etl_data/cleaned_sp500_stock_data.csv")

    # seperates the filtered tweet data into dataframes containing tweets of only 1 sentiment types
    positive_filtered_tweets = filtered_elon_tweet_data[
        filtered_elon_tweet_data["sentiment type"] == "positive"
    ]
    negative_filtered_tweets = filtered_elon_tweet_data[
        filtered_elon_tweet_data["sentiment type"] == "negative"
    ]
    neutral_filtered_tweets = filtered_elon_tweet_data[
        filtered_elon_tweet_data["sentiment type"] == "neutral"
    ]

    # seperates the tweet data into dataframes containing tweets of only one sentiment types
    positive_tweets = elon_tweet_data[elon_tweet_data["sentiment type"] == "positive"]
    negative_tweets = elon_tweet_data[elon_tweet_data["sentiment type"] == "negative"]
    neutral_tweets = elon_tweet_data[elon_tweet_data["sentiment type"] == "neutral"]

    # only keep the tweet on a date with the highest sentiment score for each type
    positive_filtered_tweets = positive_filtered_tweets.groupby("date").max()
    negative_filtered_tweets = negative_filtered_tweets.groupby("date").max()
    neutral_filtered_tweets = neutral_filtered_tweets.groupby("date").max()

    positive_tweets = positive_tweets.groupby("date").max()
    negative_tweets = negative_tweets.groupby("date").max()
    neutral_tweets = neutral_tweets.groupby("date").max()

    # renames the sp500 columns
    sp500_stock_data.rename(
        inplace=True, columns={"change_percent": "change_percent2", "volume": "volume2"}
    )

    # joins all the tweet dataframes with the tesla stock data and sp500 stock data
    filtered_elon_tweet_data = pd.merge(
        filtered_elon_tweet_data, tesla_stock_data, on="date", how="inner"
    )
    filtered_pos_elon_tesla_stock_data = pd.merge(
        positive_filtered_tweets, tesla_stock_data, on="date", how="inner"
    )
    filtered_neg_elon_tesla_stock_data = pd.merge(
        negative_filtered_tweets, tesla_stock_data, on="date", how="inner"
    )
    filtered_neu_elon_tesla_stock_data = pd.merge(
        neutral_filtered_tweets, tesla_stock_data, on="date", how="inner"
    )
    pos_elon_tesla_stock_data = pd.merge(
        positive_tweets, tesla_stock_data, on="date", how="inner"
    )
    neg_elon_tesla_stock_data = pd.merge(
        negative_tweets, tesla_stock_data, on="date", how="inner"
    )
    neu_elon_tesla_stock_data = pd.merge(
        neutral_tweets, tesla_stock_data, on="date", how="inner"
    )
    elon_sp500_pos = pd.merge(
        filtered_pos_elon_tesla_stock_data, sp500_stock_data, on="date", how="inner"
    )
    elon_sp500_neg = pd.merge(
        negative_filtered_tweets, sp500_stock_data, on="date", how="inner"
    )
    elon_sp500_neu = pd.merge(
        neutral_filtered_tweets, sp500_stock_data, on="date", how="inner"
    )

    # joins all the tweet dataframes with the tesla stock data and sp500 stock data
    filtered_elon_tweet_data.to_csv(
        out_directory + "/filtered_elon_tesla_stock_data.csv", mode="w", index=False
    )

    filtered_pos_elon_tesla_stock_data.to_csv(
        out_directory + "/filtered_pos_elon_tesla_stock_data.csv", mode="w", index=False
    )
    filtered_neg_elon_tesla_stock_data.to_csv(
        out_directory + "/filtered_neg_elon_tesla_stock_data.csv", mode="w", index=False
    )
    filtered_neu_elon_tesla_stock_data.to_csv(
        out_directory + "/filtered_neu_elon_tesla_stock_data.csv", mode="w", index=False
    )
    pos_elon_tesla_stock_data.to_csv(
        out_directory + "/pos_elon_tesla_stock_data.csv", mode="w", index=False
    )
    neg_elon_tesla_stock_data.to_csv(
        out_directory + "/neg_elon_tesla_stock_data.csv", mode="w", index=False
    )
    neu_elon_tesla_stock_data.to_csv(
        out_directory + "/neu_elon_tesla_stock_data.csv", mode="w", index=False
    )

    elon_sp500_pos.to_csv(out_directory + "/elon_sp500_pos.csv", mode="w", index=False)
    elon_sp500_neg.to_csv(out_directory + "/elon_sp500_neg.csv", mode="w", index=False)
    elon_sp500_neu.to_csv(out_directory + "/elon_sp500_neu.csv", mode="w", index=False)


if __name__ == "__main__":
    out_directory = sys.argv[1]
    main(out_directory)
