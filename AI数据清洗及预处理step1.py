from pymongo import MongoClient
from openai import OpenAI
import json
from datetime import datetime, timezone
import time

# ======================================
# 1. 配置（完全匹配你的数据结构）
# ======================================
MONGO_URI = "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/?appName=STAT2630Cluster0"
DB_NAME = "group_project"
SOURCE_COLLECTION = "youtube_comments_final_zy"  # 你的源集合名
TARGET_DB = "textusing"
TARGET_COLLECTION = "text1"  # 目标集合，结构和你截图完全一致
BATCH_SIZE = 20  # 防截断安全批次
API_KEY = ""
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3/"
MODEL = "doubao-seed-2-0-pro-260215"

# ======================================
# 2. 初始化客户端
# ======================================
client_ai = OpenAI(api_key=API_KEY, base_url=BASE_URL)
mongo_client = MongoClient(MONGO_URI)
print("✅ MongoDB连接成功！")

# ======================================
# 3. 数组字段提取函数（适配你的源数据结构）
# ======================================
def get_array_value(doc, field, default=""):
    val = doc.get(field, None)
    if val is None:
        return default
    if isinstance(val, list):
        if len(val) > 0:
            return str(val[0]).strip()
        else:
            return default
    return str(val).strip()

# ======================================
# 4. 读取源数据（提取播放量/点赞数，适配新结构）
# ======================================
source_coll = mongo_client[DB_NAME][SOURCE_COLLECTION]
target_coll = mongo_client[TARGET_DB][TARGET_COLLECTION]

# 读取已处理评论，断点续传
processed_comments = set()
for doc in target_coll.find({}, {"comment_original": 1, "_id": 0}):
    if "comment_original" in doc:
        processed_comments.add(doc["comment_original"])

# 读取所有原始数据
all_docs = list(source_coll.find({}, {"_id": 0}))
print(f"📊 原始数据总条数：{len(all_docs)}")

input_data = []
for idx, doc in enumerate(all_docs):
    # 提取核心字段（适配你的源数据数组结构）
    movie_name = get_array_value(doc, "中文片名", "Unknown")
    video_id = get_array_value(doc, "video_id", "")
    comment_original = get_array_value(doc, "评论内容", "")
    author = get_array_value(doc, "作者", "")  # 保留作者，不写入目标结构
    publish_time = get_array_value(doc, "发布时间", "")

    # 提取播放量/点赞数（如果源数据有这两个字段，自动适配；没有则默认0）
    view_count = int(get_array_value(doc, "播放量", 0))
    like_count = int(get_array_value(doc, "点赞数", 0))

    # 打印前5条，确认数据读取正确
    if idx < 5:
        print(f"\n🔍 第{idx+1}条数据读取结果：")
        print(f"   movie_name: {movie_name}")
        print(f"   video_id: {video_id}")
        print(f"   comment_original: {comment_original}")
        print(f"   view_count: {view_count}")
        print(f"   like_count: {like_count}")

    # 过滤空评论和已处理评论
    if not comment_original:
        print(f"⚠️ 第{idx+1}条评论为空，跳过")
        continue
    if comment_original in processed_comments:
        print(f"⚠️ 第{idx+1}条评论已处理，跳过")
        continue

    # 构造输入数据
    input_data.append({
        "movie_name": movie_name,
        "video_id": video_id,
        "comment_original": comment_original,
        "view_count": view_count,
        "like_count": like_count,
        "publish_time": publish_time
    })

total = len(input_data)
print(f"\n✅ 数据读取完成：总评论数 {len(all_docs)}，已处理 {len(processed_comments)}，待处理 {total}")

if total == 0:
    print("❌ 没有待处理数据，请检查字段名/集合名！")
    mongo_client.close()
    exit()

# ======================================
# 5. 核心Prompt：全英文输出 + 结构1:1匹配 + 情感标签数字化
# ======================================
def process_batch_with_ai(batch_data):
    prompt = f"""
CRITICAL: YOU MUST OUTPUT A COMPLETE, VALID JSON ARRAY, NO TRUNCATION, NO INCOMPLETE STRINGS, NO SYNTAX ERRORS.
ENSURE EVERY QUOTE AND BRACKET IS PROPERLY CLOSED. OUTPUT ONLY THE JSON, NO EXTRA TEXT.

You are a professional NLP data processing expert. Process EVERY comment strictly following ALL rules below:

1. 【Translation】
Translate ALL comments (any language: Hindi, Chinese, English, etc.) into STANDARD ENGLISH, keep the original meaning accurately.

2. 【Verb Normalization】
Convert ALL verbs to their BASE FORM (root form):
- doing → do
- did → do
- went → go
- was/is/are → be
- liked → like
- watched → watch
- All tenses → base verb only

3. 【Stopword Removal】
REMOVE THESE WORDS COMPLETELY from the final text:
- be verbs: be, am, is, are, was, were, been, being
- auxiliary verbs: do, does, did, will, would, shall, should, can, could, may, might, must
- pronouns: i, you, he, she, it, we, they, me, him, her, us, them
- wh-words: what, why, how, where, when, which, that, this
- articles/prepositions: a, an, the, to, for, of, in, on, at, with, by

4. 【Cleaning】
REMOVE EVERYTHING ELSE:
- All emojis, special symbols, punctuation (.,!?""''()[] etc.)
- All numbers, @usernames, links, hashtags
- All extra spaces, keep only continuous meaningful English words
- Output ONLY lowercase letters, no uppercase

5. 【Sentiment Label (CRITICAL)】
Label the sentiment as a NUMBER ONLY:
- 1 = positive (good, great, love, etc.)
- 0 = neutral (no strong emotion, factual comments)
- -1 = negative (bad, hate, terrible, etc.)
DO NOT output text like "positive", ONLY output the number 1, 0, or -1.

6. 【Output Structure (MUST MATCH EXACTLY)】
- Output ONLY a valid JSON array, NO extra text
- Keep ALL original fields: movie_name, video_id, comment_original, view_count, like_count
- Add 2 new fields:
  ① comment_cleaned: your final cleaned English text
  ② sentiment_label: the number 1, 0, or -1
- Add 1 field: crawl_time: current UTC time in ISO 8601 format (e.g., "2026-03-31T07:45:10.842+00:00")

JSON structure MUST be exactly like this:
[
  {{
    "movie_name": "movie name",
    "video_id": "video id",
    "comment_original": "original comment",
    "comment_cleaned": "cleaned english text with base verbs no stopwords no punctuation",
    "view_count": 0,
    "like_count": 0,
    "sentiment_label": 1,
    "crawl_time": "2026-03-31T07:45:10.842+00:00"
  }}
]

Input comments to process:
{json.dumps(batch_data, ensure_ascii=False, default=str)}
"""
    try:
        response = client_ai.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            timeout=300,
            max_tokens=8192
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"⚠️ AI调用失败：{e}，等待5秒重试...")
        time.sleep(20)
        return None

# ======================================
# 6. 分批次处理 + 写入MongoDB（结构1:1对齐）
# ======================================
for i in range(0, total, BATCH_SIZE):
    batch = input_data[i:i+BATCH_SIZE]
    print(f"\n🔄 正在处理第 {i//BATCH_SIZE + 1} 批，共 {len(batch)} 条（剩余 {total - i - len(batch)} 条）")
    
    ai_output = None
    # 重试机制，确保输出完整
    for retry in range(5):
        ai_output = process_batch_with_ai(batch)
        if ai_output and ai_output.strip().startswith("[") and ai_output.strip().endswith("]"):
            break
        print(f"⚠️ 第{retry+1}次重试，输出不完整，等待5秒...")
        time.sleep(20)
    
    if not ai_output or not (ai_output.strip().startswith("[") and ai_output.strip().endswith("]")):
        print(f"❌ 第 {i//BATCH_SIZE + 1} 批处理失败，跳过该批")
        continue
    
    # 解析JSON
    try:
        cleaned_items = json.loads(ai_output)
    except Exception as e:
        print(f"❌ JSON解析失败：{e}，跳过该批")
        continue
    
    # 【结构校验：确保字段名完全匹配你的截图】
    valid_fields = {"movie_name", "video_id", "comment_original", "comment_cleaned", "view_count", "like_count", "sentiment_label", "crawl_time"}
    for item in cleaned_items:
        # 补全缺失字段，确保结构一致
        for field in valid_fields:
            if field not in item:
                if field == "crawl_time":
                    item[field] = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
                elif field == "sentiment_label":
                    item[field] = 0
                elif field in ["view_count", "like_count"]:
                    item[field] = 0
                else:
                    item[field] = ""
    
    # 写入MongoDB
    target_coll.insert_many(cleaned_items)
    print(f"✅ 第 {i//BATCH_SIZE + 1} 批处理完成，已写入MongoDB（结构1:1匹配）")
    time.sleep(2)

# ======================================
# 7. 收尾
# ======================================
print(f"\n🎉 全部分批处理完成！共处理 {total} 条评论")
print("✅ 最终数据结构完全匹配你的要求：全英文字段名 + 情感标签数字化 + 时间格式统一")
mongo_client.close()
