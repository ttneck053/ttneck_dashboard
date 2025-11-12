# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from datetime import timedelta

# === 1️⃣ 평가용 원본 데이터 불러오기 ===
df = pd.read_csv("C:/temp2/auto_data_test.csv")
df = df.sort_values(["media_id", "collected_at_utc"])

# === 2️⃣ UTC 시각 변환 ===
df["collected_at_utc"] = pd.to_datetime(df["collected_at_utc"], utc=True, errors="coerce")

# === 3️⃣ 시간 인덱스(t) 부여 ===
df["t"] = df.groupby("media_id").cumcount()

# === 4️⃣ t=0 가짜 시점(조회수 0, 10분 전) 추가 ===
fake_rows = []
for mid, group in df.groupby("media_id"):
    first_time = group["collected_at_utc"].min()
    new_row = {
        "media_id": mid,
        "t": 0,
        "caption": group["caption"].iloc[0],
        "views": 0,
        "collected_at_utc": first_time - timedelta(minutes=10)
    }
    temp = group.copy()
    temp["t"] = temp["t"] + 1
    fake_rows.append(pd.concat([pd.DataFrame([new_row]), temp], ignore_index=True))

df = pd.concat(fake_rows, ignore_index=True)
df = df.sort_values(["media_id", "t"]).reset_index(drop=True)

# === 5️⃣ 포화시점 계산 함수 ===
# 기준: 증가율 2% 미만(threshold=0.02)이 3회 연속 지속(patience=3)
def find_saturation_point(group, threshold=0.02, patience=3):
    views = group["views"].values
    times = group["t"].values
    if len(views) < patience + 1:
        return pd.Series({"saturation_t": np.nan, "saturation_views": np.nan})

    growth_rate = (views[1:] - views[:-1]) / np.clip(views[:-1], 1, None)
    below = (growth_rate < threshold).astype(int)
    run_length = 0

    for i, val in enumerate(below):
        if val == 1:
            run_length += 1
        else:
            run_length = 0
        if run_length >= patience:
            return pd.Series({
                "saturation_t": times[i + 1],
                "saturation_views": views[i + 1]
            })

    return pd.Series({
        "saturation_t": times[-1],
        "saturation_views": views[-1]
    })

# === 6️⃣ 포화시점 계산 ===
saturation_df = df.groupby("media_id", group_keys=False).apply(find_saturation_point).reset_index()

# === 7️⃣ 상위 10% 라벨링 ===
threshold_value = saturation_df["saturation_views"].quantile(0.9)
saturation_df["label"] = (saturation_df["saturation_views"] >= threshold_value).astype(int)

# === 8️⃣ pivot 변환 ===
pivot_df = (
    df.pivot_table(index=["media_id", "caption"], columns="t", values="views", aggfunc="last")
    .ffill(axis=1)
)
pivot_df.columns = [f"t{i}" for i in pivot_df.columns]
pivot_df.reset_index(inplace=True)

# === 9️⃣ 라벨 병합 ===
final_df = pivot_df.merge(
    saturation_df[["media_id", "saturation_t", "saturation_views", "label"]],
    on="media_id",
    how="left"
)

# === 🔟 출력 폴더 생성 ===
output_dir = "C:/temp2/svm_eval_datasets"
os.makedirs(output_dir, exist_ok=True)

# === 11️⃣ 3h / 6h / 12h 데이터셋 생성 ===
time_windows = {"3h": 3 * 6, "6h": 6 * 6, "12h": 12 * 6}
t_cols = sorted([c for c in final_df.columns if c.startswith("t")],
                key=lambda x: int(x[1:]))

for name, cutoff in time_windows.items():
    if len(t_cols) < cutoff:
        print(f"⚠️ {name} 구간({cutoff}포인트) 잘라낼 만큼 데이터가 부족합니다. 현재 t컬럼 개수: {len(t_cols)}")
        continue

    time_cols = t_cols[:cutoff]
    subset_cols = ["media_id", "caption"] + time_cols + ["label"]
    subset = final_df[subset_cols].copy()

    save_path = f"{output_dir}/svm_eval_{name}.csv"
    subset.to_csv(save_path, index=False, encoding="utf-8-sig")
    print(f"{name} 평가 데이터셋 생성 완료 → {save_path} (shape: {subset.shape})")

# === 12️⃣ 포화시점 summary 파일 생성 ===
caption_map = df.groupby("media_id")["caption"].first().reset_index()
summary = saturation_df.merge(caption_map, on="media_id", how="left")

last_views = df.groupby("media_id")["views"].last().reset_index().rename(columns={"views": "final_views"})
summary = summary.merge(last_views, on="media_id", how="left")
summary["saturation_ratio"] = summary["final_views"] / summary["saturation_views"]

summary_path = f"{output_dir}/insta_eval_summary.csv"
summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
print(f"\n포화시점 요약 파일 생성 완료 → {summary_path} (shape: {summary.shape})")