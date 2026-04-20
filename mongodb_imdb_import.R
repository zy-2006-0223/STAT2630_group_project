# ==========================
# 系统依赖（必须）
# ==========================
system("sudo apt-get update -y")
system("sudo apt-get install -y libssl-dev libsasl2-dev")
system("sudo apt-get install -y libmongoc-dev libbson-dev")

# ==========================
# 安装包
# ==========================
install.packages(c("mongolite", "tidyverse", "readr"))
library(mongolite)
library(tidyverse)
library(readr)

# ==========================
# 1. 读取你的 IMDb CSV
# ==========================
imdb_data <- read_csv("/content/movies_imdb.csv")

# ==========================
# 2. 提取 电影名称 + IMDb评分
# ==========================
movie_data <- imdb_data %>%
  select(
    中文片名 = 名称,    # 你的列名叫"名称"
    IMDb评分 = IMDb评分
  )

# ==========================
# 3. 连接 你的 MongoDB
# ==========================
con <- mongo(
  collection = "imdb_movies_new",   # 新表名（自动创建）
  db = "group_project",        # 你的数据库名
  url = "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/"
)

# ==========================
# 4. 写入 MongoDB（全覆盖）
# ==========================
con$drop()                     # 清空旧数据
con$insert(movie_data)         # 插入新数据

# ==========================
# 5. 提示成功
# ==========================
cat("✅ 成功存入 MongoDB！\n")
cat("✅ 数据库：group_project\n")
cat("✅ 集合：imdb_movies\n")
cat("✅ 共插入", nrow(movie_data), "部电影\n")
