import boto3
import requests
import csv
import io
from datetime import datetime, timezone

s3 = boto3.client('s3')

BUCKET_NAME = "ttneck-labs"
PREFIX = "insta-views/"

# 환경변수로 세팅 권장
ACCESS_TOKEN = "YOUR_LONG_LIVED_ACCESS_TOKEN"
IG_USER_ID = "YOUR_IG_USER_ID"

def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    minute = now.strftime("%M")

    # Instagram Graph API 호출
    url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
    params = {
        "fields": "id,caption,media_type,timestamp,insights.metric(plays,impressions,reach,engagement)",
        "access_token": ACCESS_TOKEN
    }
    response = requests.get(url, params=params)
    data = response.json()

    rows = []
    if "data" in data:
        for item in data["data"]:
            insights = {}
            if "insights" in item and "data" in item["insights"]:
                for metric in item["insights"]["data"]:
                    insights[metric["name"]] = metric.get("values", [{}])[0].get("value", 0)

            rows.append({
                "snapshot_at_utc": now.isoformat(),
                "post_id": item["id"],
                "caption": item.get("caption", "").replace("\n", " ")[:100],  # 100자 제한
                "media_type": item.get("media_type", ""),
                "timestamp": item.get("timestamp", ""),
                "plays": insights.get("plays", 0),
                "impressions": insights.get("impressions", 0),
                "reach": insights.get("reach", 0),
                "engagement": insights.get("engagement", 0)
            })

    # CSV 버퍼 생성
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=[
        "snapshot_at_utc","post_id","caption","media_type","timestamp",
        "plays","impressions","reach","engagement"
    ])
    writer.writeheader()
    writer.writerows(rows)

    # 저장 경로
    key = f"{PREFIX}date={date}/hour={hour}/minute={minute}/snapshot.csv"

    # S3 업로드
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue().encode("utf-8")
    )

    return {
        "statusCode": 200,
        "body": f"Uploaded {len(rows)} rows to {key}"
    }
