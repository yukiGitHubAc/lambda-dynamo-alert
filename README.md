# lambda-dynamo-alert

It is a script for detecting throughput error of DynamoDB.

This script works on AWSLambda.

日本語は[README.jp.me]

# How to use

npm install

Zip this repository.

Upload to AWS Lambda.

Finally, set the necessary environment variables.

## Environment variables

REGION: Region of AWS

token: Slack's token

channel: Slack's channel name

username: Slack's user name

icon: Slack user icon URL
