import json
import requests
import datetime
from kafka import KafkaProducer

def lambda_handler(event, context):
    # Dynamically set date range to recent (last 1 day)
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    API_URL = "https://newsapi.org/v2/everything"
    params = {
        "q": "apple",
        "from": str(yesterday),
        "to": str(today),
        "sortBy": "popularity",
        "apiKey": "6bd79400b4aa4eb9b50aeecd0ca84a81"
    }

    # Fetch data from News API
    response = requests.get(API_URL, params=params)
    news_data = response.json()

    # ✅ Only proceed if API call was successful
    if response.status_code == 200 and news_data.get("status") == "ok":
        articles = news_data.get("articles", [])
        
        if not articles:
            return {"statusCode": 204, "body": "No articles found"}

        # Send each article to Kafka
        producer = KafkaProducer(
            bootstrap_servers=['13.204.47.102:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        for article in articles:
            producer.send('news-topic', article)
        producer.flush()

        return {"statusCode": 200, "body": f"{len(articles)} articles sent to Kafka"}
    
    else:
        # ❌ Log the error response, do not send it to Kafka
        print("Error from News API:", news_data)
        return {
            "statusCode": 500,
            "body": f"News API error: {news_data.get('message', 'Unknown error')}"
        }
