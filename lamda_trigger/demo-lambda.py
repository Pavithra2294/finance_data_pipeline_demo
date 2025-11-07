import boto3
import os

def lambda_handler(event, context):
    # Name of the Glue crawler (can also be passed from event)
    #crawler_name = os.getenv("CRAWLER_NAME", "my-glue-crawler")

    glue = boto3.client('glue')

    try:
        # Start the crawler
        response = glue.start_crawler(Name='finance_crawler')
        print(f"Successfully started Glue crawler:finance_crawler")
        return {
            'statusCode': 200,
            'body': f'Successfully started Glue crawler:finance_crawler'
        }

    except glue.exceptions.CrawlerRunningException:
        # If crawler is already running
        print(f"finance_crawler is already running.")
        return {
            'statusCode': 200,
            'body': f"finance_crawler is already running."
        }

    except Exception as e:
        print(f"Error starting finance_crawler:{str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error starting crawler: {str(e)}"
        }
