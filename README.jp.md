# lambda-dynamo-alert
DynamoDBのスループット超過検知用のLambdaスクリプト

## 使い方
依存するpackageをnpmでインストールして下さい

```
npm i
```

ZIP圧縮してLambdaにアップロードしてください。
アップしたアカウントの指定したリージョンにあるDynamoDBを全走査します。

## Lambda環境変数

REGION: リージョン

token: Slackのトークン

channel: Slackのポストするチャンネル名

username: Slackにポストする時のユーザー名

icon: Slackに投稿する際のアイコン（RUL指定）
