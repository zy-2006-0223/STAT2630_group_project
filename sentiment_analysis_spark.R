# ============================================================
#  YouTube 电影评论情感分析 - 优化版 (Spark + MongoDB)
# ============================================================

# ==========================
# 1. 环境安装（Linux/macOS）
# ==========================
system("apt-get update && apt-get install -y openjdk-17-jdk libssl-dev libsasl2-dev")

.libPaths("/usr/local/lib/R/site-library")
options(timeout = 3600)
options(download.file.method = "wget")

install.packages(c("sparklyr", "mongolite", "dplyr", "tidyr"), dependencies = TRUE)

library(sparklyr)
library(mongolite)
library(dplyr)
library(tidyr)

# ==========================
# 2. 连接 Spark
# ==========================
cfg <- spark_config()
cfg$spark.driver.memory       <- "12g"
cfg$spark.executor.memory     <- "12g"
cfg$sparklyr.cores.local      <- 4
cfg$spark.sql.shuffle.partitions <- 8

sc <- spark_connect(master = "local", config = cfg)
message("✅ Spark 连接成功！")

# ==========================
# 3. MongoDB 连接
# ==========================
mongo_uri <- "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/group_project"

con <- mongo(collection = "finally_cleaning", db = "textusing", url = mongo_uri)
df <- con$find()

message("读取到 ", nrow(df), " 行，", ncol(df), " 列")

# ==========================
# 4. 数据清洗
# ==========================
df <- df[, !sapply(df, function(x) inherits(x, c("POSIXt", "POSIXct", "Date", "chron", "POSIXlt")))]
df <- df[, !grepl("time|date|Time|Date", names(df))]
df <- as.data.frame(df, stringsAsFactors = FALSE)
names(df) <- make.names(names(df), unique = TRUE)

message("清洗后：", nrow(df), " 行，", ncol(df), " 列")

# ==========================
# 5. 上传 Spark
# ==========================
sdf <- copy_to(sc, df, overwrite = TRUE)

sdf <- sdf %>%
  filter(
    !is.na(comment_cleaned),
    length(comment_cleaned) >= 3
  )

message("过滤后有效数据：", sdf %>% count() %>% pull(n), " 条")

# ==========================
# 6. 增强情感标注
# ==========================

# 6.1 核心情感词典（使用词边界避免误匹配）
pos_words <- c(
  "good", "great", "love", "best", "excellent", "amazing", "perfect",
  "awesome", "fantastic", "beautiful", "wonderful", "brilliant",
  "outstanding", "superb", "magnificent", "masterpiece", "incredible",
  "recommend", "enjoy", "enjoyable", "fun", "exciting", "thrilling",
  "funny", "hilarious", "touching", "emotional", "inspiring",
  "gorgeous", "stunning", "spectacular", "impressive",
  "favorite", "liked", "nice", "pleasant", "satisfying",
  "watch again", "worth it", "must see", "goosebumps",
  "legendary", "epic", "iconic", "top tier", "second to none",
  "喜欢", "好看", "精彩", "棒", "赞", "爱", "完美", "经典",
  "感动", "推荐", "期待", "惊喜", "震撼", "优秀", "满意",
  "值得", "神作", "无敌", "顶级", "一流", "高分", "绝"
)

neg_words <- c(
  "bad", "worst", "boring", "terrible", "waste", "hate", "awful",
  "horrible", "disappointing", "disappointed", "annoying",
  "ridiculous", "pathetic", "garbage", "trash", "crap", "sucks",
  "poor", "weak", "failed", "fail", "overrated", "predictable",
  "slow", "tedious", "nonsense", "pointless", "waste of time",
  "don't watch", "don't recommend", "skip this", "not worth",
  "okay", "meh", "mediocre", "bland", "flat", "dull",
  "avoid", "skip", "terrible", "abysmal", "atrocious",
  "flop", "cringe", "mess", "disaster", "nightmare",
  "regret watching", "worst movie", "hate this", "stupid movie",
  "难 看", "无聊", "烂片", "差", "失望", "垃圾片", "恐怖片",
  "反感", "差评", "负分", "坑", "无语", "后悔", "烂",
  "乏味", "拖沓", "狗血", "尴尬", "恶心", "毁三观", "渣",
  "无语", "没劲", "睡着", "坑爹"
)

negation_words <- c(
  "not", "no", "never", "neither", "nobody", "nothing",
  "nowhere", "hardly", "barely", "scarcely",
  "dont", "doesnt", "didnt", "wont", "wouldnt",
  "shouldnt", "couldnt", "isnt", "arent", "wasnt", "werent",
  "没", "不", "非", "无", "别", "未", "莫", "勿", "休"
)

intensifier_words <- c(
  "very", "really", "extremely", "absolutely", "completely",
  "totally", "highly", "incredibly", "so", "too", "quite",
  "super", "uber", "mega", "truly",
  "非常", "特别", "极其", "十分", "超", "真", "太", "超级",
  "绝对", "完全", "实在", "着实", "确实", "真是", "太"
)

# 6.2 R 端情感评分函数（改进版）
score_sentiment <- function(text) {
  text <- tolower(as.character(text))

  # 词边界正则，避免 "goodness" 误匹配 "good"
  pos_pat <- paste0("\\b(", paste(pos_words, collapse = "|"), ")\\b")
  neg_pat <- paste0("\\b(", paste(neg_words, collapse = "|"), ")\\b")
  neg_mod <- paste0("\\b(", paste(negation_words, collapse = "|"), ")\\b")
  int_pat <- paste0("\\b(", paste(intensifier_words, collapse = "|"), ")\\b")

  pos_matches <- gregexpr(pos_pat, text, perl = TRUE)[[1]]
  neg_matches <- gregexpr(neg_pat, text, perl = TRUE)[[1]]
  pos_count <- sum(pos_matches > 0)
  neg_count <- sum(neg_matches > 0)

  has_intens <- any(gregexpr(int_pat, text, perl = TRUE)[[1]] > 0)

  # 否定词处理：检测情感词是否被否定
  words <- unlist(strsplit(gsub("[^a-z\\s]", " ", text), "\\s+"))
  pos_negated_count <- 0
  neg_negated_count <- 0

  for (i in seq_along(words)) {
    w <- words[i]
    # 前面最多 3 个词
    prev <- paste(words[max(1, i-3):max(0, i-1)], collapse = " ")
    for (nm in negation_words) {
      if (grepl(paste0("\\b", nm, "\\b"), prev, perl = TRUE)) {
        if (grepl(pos_pat, w, perl = TRUE)) pos_negated_count <- pos_negated_count + 1
        if (grepl(neg_pat, w, perl = TRUE)) neg_negated_count <- neg_negated_count + 1
      }
    }
  }

  # 无情感词 → 中性
  if (pos_count == 0 && neg_count == 0) return(0.5)

  # 有效计数（排除被否定的）
  pos_net <- pos_count - pos_negated_count
  neg_net <- neg_count - neg_negated_count

  # 强度加成
  if (has_intens) {
    pos_net <- pos_net * 1.3
    neg_net <- neg_net * 1.3
  }

  net <- pos_net - neg_net

  if (net > 0.5)   return(1)
  if (net < -0.5)  return(0)
  return(0.5)
}

# 6.3 情感评分
comment_data <- sdf %>%
  select(movie_name, comment_cleaned, view_count, like_count) %>%
  collect()

message("开始情感评分（", nrow(comment_data), " 条评论）...")

comment_data$label <- sapply(comment_data$comment_cleaned, score_sentiment)

# 同时计算原始情感分数（0/0.5/1，用于最终 pos_rate）
comment_data$sentiment_raw <- comment_data$label

message("情感评分完成！")
message("正面(1):   ", sum(comment_data$label == 1,   na.rm = TRUE), " 条")
message("负面(0):   ", sum(comment_data$label == 0,   na.rm = TRUE), " 条")
message("中性(0.5): ", sum(comment_data$label == 0.5, na.rm = TRUE), " 条")

# 6.4 重新上传评分后数据到 Spark
sdf_scored <- copy_to(sc, comment_data, overwrite = TRUE)

# ==========================
# 7. TF-IDF 优化
# ==========================
sdf_scored <- sdf_scored %>%
  ft_tokenizer(input_col = "comment_cleaned", output_col = "words") %>%
  ft_ngram(input_col = "words", output_col = "bigrams", n = 2L) %>%
  mutate(all_tokens = concat_ws(" ", words, bigrams)) %>%
  ft_tokenizer(input_col = "all_tokens", output_col = "all_words") %>%
  ft_hashing_tf(
    input_col    = "all_words",
    output_col   = "rawFeatures",
    num_features = 4096L
  ) %>%
  ft_idf(
    input_col    = "rawFeatures",
    output_col   = "features",
    min_doc_freq = 2L
  )

message("✅ TF-IDF 特征提取完成")

# ==========================
# 8. 模型训练
# ==========================

# 过滤中性样本，仅用明确正/负面训练
sdf_train <- sdf_scored %>%
  filter(label != 0.5) %>%
  mutate(label_int = as.integer(label))

# --- 逻辑回归 ---
lr_model <- ml_logistic_regression(
  sdf_train,
  features_col       = "features",
  label_col          = "label_int",
  prediction_col     = "lr_prediction",
  probability_col    = "lr_probability",
  reg_param          = 0.01,
  elastic_net_param  = 0.0
)
sdf_lr <- ml_predict(lr_model, sdf_scored) %>%
  select(movie_name, comment_cleaned, label, sentiment_raw, lr_prediction, view_count, like_count)

# --- SVM ---
svm_model <- ml_linear_svc(
  sdf_train,
  features_col        = "features",
  label_col           = "label_int",
  prediction_col      = "svm_prediction",
  raw_prediction_col   = "svm_rawPrediction",
  reg_param           = 0.01
)
sdf_svm <- ml_predict(svm_model, sdf_scored) %>%
  select(svm_prediction)

# --- KMeans 聚类 ---
km_model <- ml_kmeans(
  sdf_scored,
  features_col   = "features",
  k              = 3L,
  prediction_col = "topic"
)
sdf_km <- ml_predict(km_model, sdf_scored) %>%
  select(topic)

# --- 合并三个模型结果 ---
sdf_final <- sdf_lr %>%
  sdf_bind_cols(sdf_svm) %>%
  sdf_bind_cols(sdf_km) %>%
  rename(prediction = lr_prediction)

message("✅ 模型训练完成")

# ==========================
# 8.5 LDA 主题建模
# ==========================
message("开始 LDA 主题建模...")

# LDA 需要 CountVectorizer
cv_model <- ft_count_vectorizer(
  sdf_scored,
  input_col  = "words",
  output_col = "termFreq",
  vocab_size = 1000L
)

# 训练 LDA（10 个主题，更细分）
k_topics <- 10L

lda_model <- ml_lda(
  cv_model,
  k              = k_topics,
  max_iter       = 30L,
  features_col   = "termFreq",
  topic_distribution_col = "topic_dist"
)

message("✅ LDA 模型训练完成（", k_topics, " 个主题）")

# ===== 提取主题关键词 =====
# 方法：从 Spark 获取 vocabulary 和 topic-word 矩阵
tryCatch({
  # 获取 CountVectorizer 的词汇表
  cv_jobj <- cv_model %>%
    spark_dataframe() %>%
    invoke("getVocab")

  vocab <- if (is.character(cv_jobj)) {
    cv_jobj
  } else if (is.list(cv_jobj)) {
    as.character(cv_jobj)
  } else {
    # 备选：从 Java list 提取
    tryCatch({
      vocab <- invoke(cv_model$model$java_model, "vocabulary")
      if (is.list(vocab)) vocab <- as.character(vocab)
    }, error = function(e) NULL)
  }

  # 获取 LDA 的 beta 矩阵（topic-word 分布）
  beta_jobj <- invoke(lda_model$model$java_model, "beta")
  beta_matrix <- if (is.matrix(beta_jobj)) {
    beta_jobj
  } else if (is.list(beta_jobj)) {
    do.call(cbind, beta_jobj)
  } else {
    NULL
  }

  if (!is.null(vocab) && !is.null(beta_matrix)) {
    cat("\n========== LDA 主题关键词 ==========\n")
    vocab <- as.character(vocab)
    for (t in 1:k_topics) {
      topic_col <- if (is.matrix(beta_matrix)) beta_matrix[, t] else beta_matrix[[t]]
      top_word_idx <- order(topic_col, decreasing = TRUE)[1:15]
      top_words_in_topic <- vocab[top_word_idx]
      cat(sprintf("主题 %d: %s\n", t, paste(top_words_in_topic, collapse = ", ")))
    }
  } else {
    stop("无法获取词汇表或 beta 矩阵")
  }
}, error = function(e) {
  cat("\n⚠️ 无法直接提取主题关键词（Spark 版本限制）\n")
  cat("改用代表性评论方式展示各主题...\n")
})

# ===== 全局高频词 =====
cat("\n========== 全局高频词（Top 50）==========\n")
all_words <- paste(comment_data$comment_cleaned, collapse = " ")
word_freq <- table(unlist(strsplit(tolower(all_words), "\\s+")))
top_words <- sort(word_freq, decreasing = TRUE)[1:100]
stopwords <- c(
  "the", "a", "an", "is", "are", "was", "were", "be", "been",
  "to", "of", "in", "for", "on", "with", "as", "at", "by",
  "and", "or", "it", "this", "that", "i", "you", "he", "she",
  "we", "they", "my", "your", "his", "her", "its", "our", "their",
  "have", "has", "had", "do", "does", "did", "will", "would", "could",
  "just", "like", "get", "got", "make", "made", "so", "but", "not",
  "if", "when", "what", "which", "who", "how", "than", "then",
  "very", "really", "even", "all", "only", "also", "much", "out",
  "more", "most", "no", "can", "from", "up", "about", "into",
  "because", "after", "before", "some", "any", "there", "here",
  "me", "him", "us", "them", "am", "being", "thing", "things",
  "one", "two", "time", "way", "movie", "film", "movies", "films",
  "see", "watch", "watching", "watched", "dont", "im", "youre", "hes",
  "shes", "theyre", "thats", "whats", "youll", "ill", "ive", "youve",
  "isnt", "arent", "wasnt", "werent", "doesnt", "didnt", "wont", "cant",
  "couldnt", "shouldnt", "wouldnt", "hasnt", "havent",
  "own", "back", "come", "came", "go", "going", "went", "know", "knew",
  "think", "thought", "feel", "felt", "say", "said", "want", "wanted",
  "take", "took", "give", "gave", "look", "looking", "seen",
  "let", "put", "keep", "kept", "tell", "told", "show", "showed",
  "need", "try", "call", "called", "use", "used",
  "find", "found", "may", "might", "must", "shall",
  "scene", "scenes", "character", "characters", "plot", "story", "ending", "endings",
  "people", "point", "life", "lot", "year", "years", "first", "last", "new", "old",
  "good", "bad", "best", "worst", "great", "love", "hate", "fun", "boring",
  "amazing", "perfect", "cant", "wont", "wasnt"
)
top_words_clean <- top_words[!names(top_words) %in% stopwords]
cat(paste(names(top_words_clean)[1:50], collapse = ", "), "\n")

# ===== 每主题代表性评论（展示各主题的实际内容）=====
cat("\n========== 各主题代表性评论 ==========\n")

# 转换 LDA 结果（cv_model 已经包含 words 列）
sdf_lda <- ml_transform(lda_model, cv_model) %>%
  select(movie_name, comment_cleaned, topic_dist)

# 收集结果
lda_result <- sdf_lda %>% collect()

# 提取主主题
lda_result$main_topic <- sapply(lda_result$topic_dist, function(x) {
  if (!is.null(x) && length(x) > 0) as.integer(which.max(x) - 1) else NA_integer_
})

# 按电影汇总主题分布
cat("\n========== 各电影主题分布（Top 3 主题）==========\n")
topic_by_movie <- lda_result %>%
  filter(!is.na(main_topic)) %>%
  group_by(movie_name, main_topic) %>%
  summarise(count = n(), .groups = "drop") %>%
  group_by(movie_name) %>%
  mutate(percentage = round(count / sum(count) * 100, 1)) %>%
  arrange(movie_name, desc(count)) %>%
  slice_head(n = 3)

for (movie in unique(topic_by_movie$movie_name)) {
  cat(sprintf("\n%s:\n", movie))
  movie_topics <- topic_by_movie[topic_by_movie$movie_name == movie, ]
  for (i in 1:nrow(movie_topics)) {
    cat(sprintf("  主题 %d: %d 条 (%.1f%%)\n",
                movie_topics$main_topic[i] + 1,
                movie_topics$count[i],
                movie_topics$percentage[i]))
  }
}

# 主题与情感的关系
cat("\n========== 主题与情感关系 ==========\n")

# 先获取情感标签
result_with_topic <- merge(
  lda_result,
  result[, c("comment_cleaned", "label", "ensemble_score")],
  by = "comment_cleaned",
  all.x = TRUE
)

result_with_topic <- result_with_topic %>%
  mutate(sentiment_label = case_when(
    label > 0.5 ~ "正面",
    label < 0.5 ~ "负面",
    TRUE ~ "中性"
  ))

topic_sentiment <- result_with_topic %>%
  filter(!is.na(main_topic), sentiment_label != "中性") %>%
  group_by(main_topic, sentiment_label) %>%
  summarise(count = n(), .groups = "drop") %>%
  pivot_wider(names_from = sentiment_label, values_from = count, values_fill = 0) %>%
  mutate(total = 正面 + 负面, pos_rate = round(正面 / total * 100, 1)) %>%
  arrange(desc(pos_rate))

print(topic_sentiment)

# 识别高正/负主题
cat("\n========== 主题情感洞察 ==========\n")
if (nrow(topic_sentiment) > 0) {
  top_pos <- topic_sentiment %>% slice_max(pos_rate, n = 3)
  top_neg <- topic_sentiment %>% slice_min(pos_rate, n = 3)

  cat("高正向主题（讨论这些话题的评论普遍正面）：\n")
  for (i in 1:nrow(top_pos)) {
    cat(sprintf("   主题 %d: %s%% 正面率\n", top_pos$main_topic[i] + 1, top_pos$pos_rate[i]))
  }

  cat("高负向主题（讨论这些话题的评论普遍负面）：\n")
  for (i in 1:nrow(top_neg)) {
    cat(sprintf("   主题 %d: %s%% 正面率\n", top_neg$main_topic[i] + 1, top_neg$pos_rate[i]))
  }
}

# ===== 每主题代表性评论（让用户直接理解主题含义）=====
cat("\n========== 各主题代表性评论（每主题展示 3 条）==========\n")
result_with_topic2 <- result_with_topic %>%
  filter(!is.na(main_topic), !is.na(comment_cleaned))

for (t in 0:(k_topics - 1)) {
  topic_comments <- result_with_topic2[result_with_topic2$main_topic == t, ]
  n_comments <- nrow(topic_comments)

  if (n_comments >= 1) {
    cat(sprintf("\n--- 主题 %d（共 %d 条评论）---\n", t + 1, n_comments))

    # 展示最具代表性的 3 条（随机抽样或按情感极端程度）
    # 高正向主题展示正面评论，高负向展示负面评论
    topic_row <- topic_sentiment[topic_sentiment$main_topic == t, ]
    topic_pos_rate <- if (nrow(topic_row) > 0) topic_row$pos_rate[1] else 50

    if (topic_pos_rate > 70) {
      # 高正向主题：展示正面评论
      sample_comments <- topic_comments[topic_comments$sentiment_label == "正面", ]
      if (nrow(sample_comments) == 0) sample_comments <- topic_comments
    } else if (topic_pos_rate < 60) {
      # 高负向主题：展示负面评论
      sample_comments <- topic_comments[topic_comments$sentiment_label == "负面", ]
      if (nrow(sample_comments) == 0) sample_comments <- topic_comments
    } else {
      sample_comments <- topic_comments
    }

    sample_comments <- sample_comments[!is.na(sample_comments$comment_cleaned), ]
    sample_size <- min(3, nrow(sample_comments))
    if (sample_size > 0) {
      for (j in 1:sample_size) {
        comment <- substr(sample_comments$comment_cleaned[j], 1, 120)
        sentiment <- sample_comments$sentiment_label[j]
        cat(sprintf("  [%s] %s...\n", sentiment, comment))
      }
    }
  }
}

# ===== 全局高频词（Top 情感词）=====
cat("\n========== 情感相关高频词（Top 30）==========\n")
all_text <- paste(comment_data$comment_cleaned, collapse = " ")
words_all <- unlist(strsplit(tolower(all_text), "\\s+"))

# 情感词列表（从之前的词典中提取）
emotion_words <- c(
  pos_words, neg_words
)
emotion_words <- emotion_words[emotion_words != "okay"]  # 去掉可能有歧义的词

emotion_freq <- sort(table(words_all[words_all %in% emotion_words]), decreasing = TRUE)
cat(paste(names(emotion_freq)[1:30], collapse = ", "), "\n")

# ==========================
# 9. 收集结果
# ==========================
result <- sdf_final %>%
  select(
    movie_name,
    comment_cleaned,
    label,
    sentiment_raw,
    prediction,
    svm_prediction,
    topic,
    view_count,
    like_count
  ) %>%
  collect()

# 集成预测：综合 LR 和 SVM，保留中性
result <- result %>%
  mutate(
    ensemble_score = case_when(
      prediction == 1L & svm_prediction == 1L ~ 1L,
      prediction == 0L & svm_prediction == 0L ~ 0L,
      prediction == 1L & svm_prediction == 0L ~ 0L,   # 两者矛盾时偏向负面
      prediction == 0L & svm_prediction == 1L ~ 0L,
      TRUE ~ as.integer(label)                            # 其他情况用原始情感标签
    )
  )

message("收集到 ", nrow(result), " 条结果")

# ==========================
# 10. 写回 MongoDB
# ==========================
con_out <- mongo(
  collection = "youtube_sentiment_result_optimized5",
  db         = "textusing",
  url        = mongo_uri
)
con_out$drop()
con_out$insert(result)
message("✅ 结果已写入 youtube_sentiment_result_optimized，共 ", nrow(result), " 条")

# ==========================
# 11. 相关性分析
# ==========================
imdb_con <- mongo(collection = "imdb_movies_new", db = "group_project", url = mongo_uri)
imdb <- imdb_con$find()

# pos_rate 基于原始情感标签（排除中性后求平均）
sentiment_summary <- result %>%
  filter(label != 0.5) %>%               # 只用有情感的评论
  group_by(movie_name) %>%
  summarise(
    pos_rate    = mean(label, na.rm = TRUE),  # label 已是 0/1
    n_emotion   = n(),
    .groups     = "drop"
  ) %>%
  mutate(movie_name_clean = tolower(trimws(gsub("_", " ", movie_name))))

# IMDb 标准化
imdb <- imdb %>%
  mutate(
    movie_name_clean = tolower(trimws(`中文片名`)),
    IMDb_score       = as.numeric(`IMDb评分`)
  )

# 合并
final_df <- merge(imdb, sentiment_summary, by = "movie_name_clean", all.x = TRUE)

# 数据概览
cat("\n========== 数据匹配情况 ==========\n")
cat("IMDb 电影数: ", nrow(imdb), "\n")
cat("情感分析电影数: ", length(unique(result$movie_name)), "\n")
cat("匹配成功: ", sum(!is.na(final_df$pos_rate)), " 部\n")

cat("\n各电影情感正向率：\n")
print(final_df[!is.na(final_df$pos_rate), c("中文片名", "IMDb评分", "pos_rate", "n_emotion")])

# 计算相关系数
matched <- final_df[!is.na(final_df$pos_rate) & !is.na(final_df$IMDb_score), ]
if (nrow(matched) >= 3) {
  corr <- cor(matched$IMDb_score, matched$pos_rate, use = "complete.obs")

  cat("\n=========================================\n")
  cat("📊 IMDb 评分 & 情感正向率 相关系数 = ", round(corr, 4), "\n")
  cat("=========================================\n")
} else {
  cat("\n⚠️ 匹配数据不足\n")
}

# ==========================
# 12. 模型评估
# ==========================
cat("\n========== 情感分布（ensemble_score）==========\n")
labeled <- result[result$label != 0.5, ]
if (nrow(labeled) > 0) {
  acc_lr  <- mean(labeled$prediction     == labeled$label, na.rm = TRUE)
  acc_svm <- mean(labeled$svm_prediction == labeled$label, na.rm = TRUE)
  acc_ens <- mean(labeled$ensemble_score == labeled$label, na.rm = TRUE)

  cat("逻辑回归准确率: ", round(acc_lr * 100, 2), "%\n")
  cat("SVM 准确率:      ", round(acc_svm * 100, 2), "%\n")
  cat("集成模型准确率:  ", round(acc_ens * 100, 2), "%\n")
}

cat("\n========== 情感分布 ==========\n")
cat("正面(1):   ", sum(result$ensemble_score == 1, na.rm = TRUE), " 条\n")
cat("负面(0):   ", sum(result$ensemble_score == 0, na.rm = TRUE), " 条\n")
cat("中性(0.5): ", sum(result$label == 0.5, na.rm = TRUE), " 条\n")

# ==========================
# 13. 关闭 Spark
# ==========================
spark_disconnect(sc)
message("✅ 全部完成！")
