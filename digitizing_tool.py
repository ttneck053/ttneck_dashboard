# digitize_from_manifest.py
# Usage: python digitize_from_manifest.py
# Input : /mnt/data/digitize_manifest_template_200.csv
# Output: /mnt/data/digitize_input_all.csv

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from pathlib import Path

MANIFEST_PATH = Path("/mnt/data/digitize_manifest_template_200.csv")
OUT_PATH      = Path("/mnt/data/digitize_input_all.csv")

# --- utils --------------------------------------------------------------

def read_manifest(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="cp949")
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def parse_views(x):
    """ robust numeric parser: '17696', '17696.0', '90,557', '9.5만' 모두 처리 """
    if pd.isna(x): 
        return None
    if isinstance(x, (int, float, np.integer, np.floating)):
        return int(round(float(x)))
    s = str(x).strip().lower()
    if "만" in s:
        s_num = re.sub(r"[^0-9\.]", "", s)
        return int(round(float(s_num) * 10000)) if s_num else None
    s = s.replace(",", "")
    try:
        return int(round(float(s)))
    except:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else None

def parse_kst(ts):
    if pd.isna(ts) or str(ts).strip() == "":
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(str(ts).strip(), fmt)
        except ValueError:
            continue
    return None

def make_series(upload_dt, end_dt, final_views, steep=8.0, shift=0.15, seed=0):
    """ 10분 간격 시계열 + 로지스틱형 누적곡선 (단조·정수·상한=final_views 보장) """
    times, cur = [], upload_dt
    while cur <= end_dt:
        times.append(cur)
        cur += timedelta(minutes=10)
    n = len(times)
    if n <= 1:
        return times, np.zeros(n, dtype=int)

    rng = np.random.default_rng(seed)
    k = steep * (0.9 + 0.2 * rng.random())   # ±10%
    s = shift + 0.05 * (rng.random() - 0.5)  # ±0.025

    t = np.linspace(0, 1, n)
    curve = final_views / (1 + np.exp(-k * (t - s)))
    curve = curve - curve.min()
    maxv  = curve.max() if curve.max() > 0 else 1.0
    curve = curve / maxv * final_views
    curve = np.maximum.accumulate(np.round(curve).astype(int))
    curve = np.minimum(curve, final_views)
    return times, curve

# --- main ---------------------------------------------------------------

def run(target_ids=None):
    """
    target_ids: ['001','002',...] 지정 시 해당 media만 생성.
    None이면 매니페스트 전체 처리.
    """
    mf = read_manifest(MANIFEST_PATH)

    # media_id 정규화
    mid = mf.get("media_id", pd.Series([""] * len(mf))).astype(str).str.strip()
    fn  = mf.get("filename", pd.Series([""] * len(mf))).astype(str)
    mf["media_id_norm"] = mid.replace("nan","")
    mf.loc[mf["media_id_norm"]=="", "media_id_norm"] = fn.str.split(".").str[0]
    mf["media_id_norm"] = mf["media_id_norm"].str.zfill(3)

    if target_ids:
        target_ids = set(target_ids)
        mf = mf[mf["media_id_norm"].isin(target_ids)].copy()

    # 파싱
    mf["upload_dt_kst"] = mf["upload_ts_kst"].apply(parse_kst)
    mf["end_dt_kst"]    = mf["end_ts_kst"].apply(parse_kst)
    mf["final_views"]   = mf["final_views"].apply(parse_views)
    mf["media_type"]    = "IMAGE"

    # 기본값 보정 (end_ts 없으면 업로드+21일)
    for i, r in mf.iterrows():
        if r["upload_dt_kst"] is None:
            mf.at[i, "upload_dt_kst"] = datetime(2025,7,16,0,0,0)
        if r["end_dt_kst"] is None:
            mf.at[i, "end_dt_kst"] = mf.at[i, "upload_dt_kst"] + timedelta(days=21)
        if pd.isna(r["final_views"]) or r["final_views"] is None:
            mf.at[i, "final_views"] = 0

    # 생성
    out_frames = []
    for _, r in mf.iterrows():
        mid = r["media_id_norm"]
        upload_dt = r["upload_dt_kst"]
        end_dt    = r["end_dt_kst"]
        fv        = int(r["final_views"])
        seed      = int(re.sub(r"\D", "", mid) or 0)

        times, views = make_series(upload_dt, end_dt, fv, seed=seed)
        df = pd.DataFrame({
            "media_id":       mid,
            "media_type":     r.get("media_type", "IMAGE"),
            "caption":        r.get("caption", ""),
            "permalink":      r.get("permalink", ""),
            "upload_ts_kst":  upload_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "obs_ts_kst":     [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in times],
            "views_cum":      views
        })
        out_frames.append(df)

    result = pd.concat(out_frames, ignore_index=True)
    result.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print(f"saved -> {OUT_PATH}")
    # 검증: 마지막 값이 final_views와 같은지 체크
    check = result.sort_values(["media_id","obs_ts_kst"]) \
                  .groupby("media_id")["views_cum"].last().rename("last_views").to_frame()
    check = check.join(mf.set_index("media_id_norm")["final_views"].rename("manifest_final"))
    print(check)

if __name__ == "__main__":
    # 예: 처음 9개만 처리하려면 아래처럼 지정
    run(target_ids=[f"{i:03d}" for i in range(1, 10)])
    # 전체 처리하려면: run()
