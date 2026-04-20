# YouTube 电影评论情感分析项目

## 项目概述

本项目通过爬取 YouTube 电影预告片评论，结合 IMDb 电影评分数据，进行情感分析与关联性研究。

**核心目标**：分析电影 YouTube 热度与 IMDb 评分的相关性，探索社交媒体评论是否能预测电影质量。

---

## 技术栈

| 环节 | 技术/工具 |
|------|----------|
| 数据存储 | MongoDB Atlas |
| 数据爬取 | YouTube Data API v3 |
| AI 分类 | 豆包 API (Doubao) |
| 数据预处理 | Python (pymongo, openai) |
| 模型训练 | Apache Spark (sparklyr) |
| 可视化 | R Shiny + Plotly |

---

## 完整工作流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              数据导入 (Step 0)                               │
│  movies_imdb.csv → mongodb_imdb_import.R → MongoDB: group_project.imdb_movies_new  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                          YouTube 评论爬取 (Step 1)                          │
│  sentiment_analysis_spark.R → MongoDB: group_project.youtube_comments_final_zy      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AI 数据清洗预处理 (Step 2)                            │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  AI数据清洗及预处理step1.py (Step 2.1)                                 │  │
│  │  - 翻译成英文                                                          │  │
│  │  - 动词还原 (loved → love)                                             │  │
│  │  - 去停用词 (the, is, and...)                                          │  │
│  │  - 去表情符号/标点                                                     │  │
│  │  - 情感标签数字化 (1/0/-1)                                              │  │
│  │  → MongoDB: textusing.text1 / textusing.final_clean                    │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      ↓                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  AI数据清洗及预处理step2.py (Step 2.2)                                 │  │
│  │  - AI 过滤纯表情包/无关评论                                            │  │
│  │  - 仅保留有效电影相关评论                                              │  │
│  │  → MongoDB: textusing.finally_cleaning                                 │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Spark 模型训练 (Step 3)                             │
│                                                                             │
│  sentiment_analysis_spark.R                                                   │
│                                                                             │
│  数据预处理:                                                                 │
│  - TF-IDF 特征提取                                                          │
│  - 分词 + N-gram                                                            │
│                                                                             │
│  模型训练:                                                                   │
│  - 逻辑回归 (Logistic Regression)                                           │
│  - SVM (支持向量机)                                                         │
│  - LDA 主题建模                                                             │
│  - K-Means 聚类                                                             │
│                                                                             │
│  关联分析:                                                                   │
│  - YouTube 热度 vs IMDb 评分 相关系数                                       │
│                                                                             │
│  → MongoDB: textusing.youtube_sentiment_result_optimized5                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                           可视化展示 (Step 4)                                │
│  sentiment_dashboard_shiny.R → R Shiny Dashboard                             │
│                                                                             │
│  页面功能:                                                                   │
│  - 总览: 电影数量/评论总数/平均正向率/相关系数                                │
│  - 电影详情: 单部电影情感分布/点赞分布/评论示例                               │
│  - 词云分析: 正面/负面情感词云                                               │
│  - 模型评估: 准确率对比/混淆矩阵                                             │
│  - 数据表格: 完整数据浏览                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 文件说明

### 数据文件
| 文件 | 说明 |
|------|------|
| `movies_imdb.csv` | IMDb 电影数据（中文片名、英文片名、评分、年代、类型等） |

### R 脚本
| 文件 | 说明 |
|------|------|
| `mongodb_imdb_import.R` | 将 CSV 数据导入 MongoDB |
| `youtube_comments_scraper.R` | YouTube API 爬虫，爬取电影预告片评论 |
| `sentiment_analysis_spark.R` | Spark 模型训练与关联分析 |
| `sentiment_dashboard_shiny.R` | R Shiny 可视化 Dashboard |

### Python 脚本
| 文件 | 说明 |
|------|------|
| `AI数据清洗及预处理step1.py` | AI 批量处理：翻译、词形还原、去停用词、情感标注 |
| `AI数据清洗及预处理step2.py` | AI 过滤表情包/无关评论，保留有效数据 |

---

## MongoDB 数据库结构

### 数据库: `group_project`
| 集合 | 内容 |
|------|------|
| `imdb_movies_new` | 导入的 IMDb 电影数据 |
| `youtube_comments_final_zy` | YouTube 爬取的原始评论 |

### 数据库: `textusing`
| 集合 | 内容 |
|------|------|
| `text1` / `final_clean` | Step1 处理后的数据 |
| `finally_cleaning` | Step2 过滤后的干净数据 |
| `youtube_sentiment_result_optimized5` | Spark 模型预测结果 |

---

## 数据预处理详解

### Step 1: AI 批量清洗
1. **翻译**：将所有语言翻译成标准英文
2. **动词还原**：`doing → do`, `was → be`, `liked → like`
3. **去停用词**：`the`, `is`, `and`, `I`, `you` 等
4. **去除表情符号和标点**
5. **情感标签**：`1`=正面, `0`=中性, `-1`=负面

### Step 2: AI 过滤
- 过滤纯表情包评论
- 过滤与电影无关的评论（订阅、点赞请求、广告等）
- 仅保留有意义的电影相关评论

---

## 模型训练详解

### 特征工程
- **分词 (Tokenization)**：句子切分为单词
- **N-gram**：提取 2-gram 词组
- **TF-IDF**：词频-逆文档频率特征

### 机器学习模型
| 模型 | 用途 |
|------|------|
| 逻辑回归 | 情感分类（正面/负面） |
| SVM | 二分类，边界最大化 |
| LDA | 主题建模，发现潜在话题 |
| K-Means | 聚类分析 |

### 集成方法
- 综合逻辑回归与 SVM 预测结果
- 矛盾时偏向负面标签

---

## 关联分析

### 分析目标
研究电影 YouTube 热度与 IMDb 评分的相关性

### 分析方法
1. 计算每部电影的评论情感正向率
2. 合并 IMDb 评分数据
3. 计算 Pearson 相关系数

### 预期结论
- 相关系数 > 0：正相关（YouTube 热度高 → IMDb 评分高）
- 相关系数 < 0：负相关
- 相关系数 ≈ 0：无显著相关性

---

## 运行指南

### 1. 环境准备
```bash
# 安装系统依赖
sudo apt-get update -y
sudo apt-get install -y libssl-dev libsasl2-dev libmongoc-dev libbson-dev

# R 包
install.packages(c("mongolite", "tidyverse", "readr"))
install.packages(c("shiny", "shinyjs", "plotly", "wordcloud2", "DT", "ggplot2"))

# Python 包
pip install pymongo openai
```

### 2. 运行顺序
```bash
# Step 0: 导入 IMDb 数据
Rscript mongodb_imdb_import.R

# Step 1: 爬取 YouTube 评论
Rscript youtube_comments_scraper.R

# Step 2: AI 数据清洗
python AI数据清洗及预处理step1.py
python AI数据清洗及预处理step2.py

# Step 3: Spark 模型训练
Rscript sentiment_analysis_spark.R

# Step 4: 可视化
Rscript sentiment_dashboard_shiny.R
```

### 3. 启动 Shiny Dashboard
```bash
# 在 R 中运行
shiny::runApp("sentiment_dashboard_shiny.R", port = 3838)
```

---

## 配置说明

### MongoDB 连接
```r
mongo_uri <- "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/"
```

### YouTube API
```r
YOUTUBE_API_KEY <- "AIzaSyBF74uv0dAr3Kopm1I_agndEPvofZhvsMo"
```

### 豆包 API
```python
API_KEY = "3ecf27d6-b1aa-49af-8657-bf5eb9673fa8"
BASE_URL = "https://ark.cn-beijing.volces.com/api/v3/"
MODEL = "doubao-seed-2-0-pro-260215"
```

---

## 项目结构
```
group_project/
├── README.md                          # 本文件
├── movies_imdb.csv                    # IMDb 电影数据
├── mongodb_imdb_import.R              # 数据导入
├── youtube_comments_scraper.R         # YouTube 爬虫
├── sentiment_analysis_spark.R         # Spark 分析
├── sentiment_dashboard_shiny.R       # Shiny 可视化
├── AI数据清洗及预处理step1.py         # AI 清洗 Step1
└── AI数据清洗及预处理step2.py         # AI 过滤 Step2
```

---

## 注意事项

1. **API 限额**：YouTube API 有每日配额限制，大规模爬取需申请更高配额
2. **费用控制**：豆包 API 按调用次数计费，注意批次大小设置
3. **数据安全**：妥善保管 API Key，不要提交到公共仓库
4. **Spark 内存**：本地运行 Spark 建议至少 12GB 内存

---

## 作者

Group Project - STAT2630
