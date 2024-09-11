# Elon-Tweet-Tesla-Stock-Dynamic

This project is made to analyze the relationship between Elon Musk's Twitter tweets and
Tesla Stocks. The end goal is to create a machine model that can predict changes in Tesla Stocks
using features of Elon's tweets.

This project was made by HiFen Kong and David Choi.

# Data

We used datasets found on Kaggle
Elon Tweet Data: https://www.kaggle.com/datasets/alexhuggler/elon-tweets-wsentimentclassified-via-roberta

SP500 Stock Data: https://www.kaggle.com/datasets/guillemservera/sp500-nasdaq-spy-qqq-ohlcv-data?select=SP500.csv

Tesla Stock Data: https://www.kaggle.com/datasets/guillemservera/tsla-stock-data

# Running The Code

1. ETL code

   - ETL for Elon Tweet Data
     Command: spark-submit ./etl/elon_tweets_etl.py ./data/elonTweets.zip
     Input: Original Files From Kaggle
     Output: Outputs .csv files into the ./etl_data/filteredTweets and ./etl_data/tweets subdirectories
     
   - ETL for Tesla Stock Data
     Command: python3 ./etl/tesla_stock_etl.py ./data/teslaStock.zip
     Input: data/teslaStock.zip
     Output: etl_data/cleaned_tesla_stock_data.csv

   - ETL for SP00 Stock Data
     Command: python3 ./etl/sp500_stock_etl.py ./data/sp500Stock.zip
     Input: data/sp500Stock.zip
     Output: etl_data/cleaned_sp500_stock_data.csv
     
   - ETL for joining the data
     Command: python3 ./etl/joining_data_etl.py ./etl_data
     Input: ./etl_data/filteredTweets and ./etl_data/tweets
     Output: Outputs .csv files into ./etl_data subdirectory with files that seperates the filteredTweets and tweets data into seperate files depending if they had negaitve, neutral, or 
     positive sentiments.

2. Correlation between the data

   - Run in the Jupyter Notebook in ./correlation
   - It uses the etl_data
   - It outputs plots ./figures

3. Historical Analysis

   - Run Jupyter Notebooks in ./yearly_analysis
   - They use the etl_data
   - The yearly_tesla_analysis.ipynb outputs a plot that appers in figures
   
4. Machine Learning Models

   - Run in the Jupyter Notebook in ./model
   - They use the elt_data
   - They print model scores
