# lambda_function.py
# 10분 주기 실시간 수집: Instagram Graph API → views 전용 CSV → S3 업로드 (UTF-8 BOM)
# 환경변수: ACCESS_TOKEN, IG_USER_ID, [SLACK_WEBHOOK_URL], [CUTOFF_UTC], [MAX_PAGES]
# 필요: requests 레이어
# 스케줄: EventBridge cron(0/10 * * * ? *)

import os
import csv
import io
import json
import time
import traceback
import boto3
import requests
from datetime import datetime, timezone

# ===== 설정 =====
BUCKET_NAME = "bucket name sample"              # ← 실제 버킷명으로 교체하십시오.
API_VER = "v23.0"
PAGE_SIZE = 100                           # 페이지당 최대치
REQUEST_TIMEOUT_SEC = 12
RETRIES = 2
RETRY_BACKOFF_SEC = 1.2

# 안전상 한도(폭주 방지). 기본 50페이지(=최대 5,000건) 까지. 환경변수로 조절 가능.
MAX_PAGES = int(os.environ.get("MAX_PAGES", "50"))

# 컷오프: 10/02 00:00:00 KST = 2025-10-01T15:00:00Z (ISO8601 +0000 포맷 유지)
CUTOFF_UTC = os.environ.get("CUTOFF_UTC", "2025-10-01T15:00:00+0000")

ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
IG_USER_ID   = os.environ["IG_USER_ID"]
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

# 릴스/이미지/캐러셀만
ALLOWED_TYPES = {"IMAGE", "VIDEO", "CAROUSEL_ALBUM"}

s3 = boto3.client("s3")


def floor_minute_to_10(dt_utc: datetime) -> datetime:
    return dt_utc.replace(minute=(dt_utc.minute // 10) * 10, second=0, microsecond=0)


def send_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        return
    try:
        payload = {"text": text}
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
    except Exception as e:
        print(f"[Slack Error] {e}")


def _get(url, params=None):
    last_err = None
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            data = r.json()
            if isinstance(data, dict) and data.get("error"):
                last_err = data["error"]
                raise RuntimeError(json.dumps(data["error"], ensure_ascii=False))
            return data
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
            else:
                raise
    raise RuntimeError(last_err)


def fetch_media_batch(after_cursor=None):
    params = {
        "fields": "id,media_type,timestamp,caption,permalink",
        "limit": PAGE_SIZE,
        "access_token": ACCESS_TOKEN,
    }
    if after_cursor:
        params["after"] = after_cursor
    return _get(f"https://graph.facebook.com/{API_VER}/{IG_USER_ID}/media", params)


def fetch_views_for_media(media_id: str):
    # 릴스/영상 기준 'views' 인사이트 조회 (계정/콘텐츠 상태에 따라 'plays' 등으로 다를 수 있음)
    data = _get(
        f"https://graph.facebook.com/{API_VER}/{media_id}/insights",
        {"metric": "views", "access_token": ACCESS_TOKEN},
    )
    for item in data.get("data", []):
        if item.get("name") == "views":
            vals = item.get("values") or []
            if vals and isinstance(vals, list):
                return vals[0].get("value")
    return None


def lambda_handler(event, context):
    try:
        now_utc = datetime.now(timezone.utc)
        bucket_utc = floor_minute_to_10(now_utc)

        rows = []
        after = None
        page = 0

        while True:
            if page >= MAX_PAGES:
                print(f"[INFO] reached MAX_PAGES={MAX_PAGES}, stopping for safety")
                break

            media_resp = fetch_media_batch(after)
            media_list = media_resp.get("data", [])
            if not media_list:
                break

            # 기본 정렬: 최신 → 과거. 컷오프보다 과거가 보이면 중단.
            stop = False
            for m in media_list:
                ts = (m.get("timestamp") or "")
                mt = (m.get("media_type") or "")
                if ts and ts < CUTOFF_UTC:
                    stop = True
                    break
                if mt not in ALLOWED_TYPES:
                    continue

                media_id = str(m["id"])  # 지수표기 방지
                try:
                    views = fetch_views_for_media(media_id)
                except Exception as e:
                    print(f"[WARN] insights error for {media_id}: {e}")
                    views = None

                rows.append({
                    "media_id": media_id,
                    "media_type": mt,
                    "timestamp_utc": ts,  # 구조 유지
                    "caption": (m.get("caption") or "").replace("\r", " ").replace("\n", " "),
                    "permalink": m.get("permalink", ""),
                    "views": views,
                    "collected_at_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%S+0000"),
                })

            if stop:
                break

            paging = media_resp.get("paging", {})
            cursors = paging.get("cursors") or {}
            after = cursors.get("after")
            page += 1
            if not after:
                break

        # CSV 작성 (엑셀 한글 깨짐 방지: UTF-8 BOM)
        fieldnames = [
            "media_id", "media_type", "timestamp_utc", "caption", "permalink", "views", "collected_at_utc"
        ]
        csv_buf = io.StringIO(newline="")
        writer = csv.DictWriter(csv_buf, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

        # BOM 부착
        csv_bytes = csv_buf.getvalue().encode("utf-8-sig")

        # 10분 파티션 경로
        key = (
            f"insta-views/"
            f"date={bucket_utc.strftime('%Y-%m-%d')}/"
            f"hour={bucket_utc.strftime('%H')}/"
            f"minute={bucket_utc.strftime('%M')}/snapshot.csv"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=csv_bytes,
            ContentType="text/csv; charset=utf-8",
            ContentDisposition="attachment; filename=snapshot.csv"
        )

        msg = f"✅ 성공: {len(rows)} rows 업로드 완료 → s3://{BUCKET_NAME}/{key}"
        print(msg)
        send_slack(msg)  # 초기엔 켜두고, 안정화 후 비활성 권장

        return {"statusCode": 200, "body": msg}

    except Exception as e:
        err_msg = f"🚨 실패: {type(e).__name__} — {str(e)}"
        try:
            rid = getattr(context, "aws_request_id", None)
            if rid:
                err_msg += f" | request_id={rid}"
        except:
            pass
        err_msg += f"\n{traceback.format_exc()[:900]}"

        print(err_msg)
        send_slack(err_msg)
        raise
