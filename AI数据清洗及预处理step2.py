from pymongo import MongoClient
from openai import OpenAI
import json
from datetime import datetime, timezone
import time

# ======================================
# 配置（和你 Ai.py 完全一致）
# ======================================
MONGO_URI = "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/"
SOURCE_DB = "textusing"
SOURCE_COLLECTION = "final_clean"          # 你已经洗好的数据
TARGET_DB = "textusing"
TARGET_COLLECTION = "finally_cleaning"      # AI 过滤后的最终干净数据

BATCH_SIZE = 20
API_KEY = "3ecf27d6-b1aa-49af-8657-bf5eb9673fa8"
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3/"
MODEL = "doubao-seed-2-0-pro-260215"

# ======================================
# 连接客户端
# ======================================
client_ai = OpenAI(api_key=API_KEY, base_url=BASE_URL)
mongo_client = MongoClient(MONGO_URI)
source_coll = mongo_client[SOURCE_DB][SOURCE_COLLECTION]
target_coll = mongo_client[TARGET_DB][TARGET_COLLECTION]

print("✅ MongoDB 连接成功")

# ======================================
# 读取已清洗的数据
# ======================================
all_docs = list(source_coll.find({}, {"_id": 0}))
total = len(all_docs)
print(f"📊 读取已清洗数据：{total} 条")

input_data = []
for doc in all_docs:
    input_data.append({
        "movie_name": doc.get("movie_name", ""),
        "comment_original": doc.get("comment_original", ""),
        "comment_cleaned": doc.get("comment_cleaned", ""),
        "video_id": doc.get("video_id", ""),
        "view_count": doc.get("view_count", 0),
        "like_count": doc.get("like_count", 0),
        "sentiment_label": doc.get("sentiment_label", 0),
        "publish_time": doc.get("publish_time", ""),
        "crawl_time": doc.get("crawl_time", "")
    })

# ======================================
# AI 批量过滤：纯表情包 + 无关评论
# ======================================
def process_filter_batch(batch):
    prompt = """
You are a comment filter for movie reviews.
Your job is ONLY to keep VALID movie-related comments.

FILTER OUT (DELETE):
1. Any comment that is ONLY EMOJIS, symbols, punctuation, no real words.
2. Any comment NOT related to the movie (subscribe, like, sub, follow, link, ad, spam, channel, etc.)

ONLY KEEP:
- Comments that are meaningful and related to the MOVIE.

Return ONLY a JSON array of the KEPT items, NO extra text.
Do NOT include filtered-out items in the output.
Keep all original fields.
"""

    prompt += f"\n\nComments to filter:\n{json.dumps(batch, ensure_ascii=False)}"

    try:
        resp = client_ai.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            timeout=120,
            max_tokens=8192
        )
        return resp.choices[0].message.content
    except:
        return None

# ======================================
# 分批执行
# ======================================
for i in range(0, len(input_data), BATCH_SIZE):
    batch = input_data[i:i+BATCH_SIZE]
    print(f"\n🔍 正在过滤第 {i//BATCH_SIZE +1} 批...")

    res = None
    for _ in range(5):
        res = process_filter_batch(batch)
        if res and res.strip().startswith("[") and res.strip().endswith("]"):
            break
        time.sleep(3)

    if not res:
        print("❌ 过滤失败，跳过")
        continue

    try:
        kept = json.loads(res)
        target_coll.insert_many(kept)
        print(f"✅ 保留 {len(kept)} 条有效评论")
    except:
        print("❌ JSON 解析失败")

# ======================================
# 完成
# ======================================
print("\n🎉 AI 全自动过滤完成！")
print("✅ 纯表情包评论已删除")
print("✅ 无关电影评论已删除")
print("✅ 最终干净数据 → textusing.text_final")

mongo_client.close()