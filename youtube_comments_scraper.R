# ==========================
# 系统依赖
# ==========================
system("sudo apt-get update -y")
system("sudo apt-get install -y libssl-dev libsasl2-dev")
system("sudo apt-get install -y libmongoc-dev libbson-dev")

# ==========================
# 安装必需包
# ==========================
install.packages(c("mongolite", "httr", "jsonlite", "dplyr"))
install.packages("stringr", dependencies=TRUE)

library(mongolite)
library(httr)
library(jsonlite)
library(dplyr)
library(stringr)

# ==========================
# 你的 YouTube API Key
# ==========================
YOUTUBE_API_KEY <- "AIzaSyBF74uv0dAr3Kopm1I_agndEPvofZhvsMo"
MAX_VIDEOS_PER_MOVIE <- 10
MAX_COMMENTS_PER_VIDEO <- 100

# ==========================
# 连接你的 MongoDB
# ==========================
con_movies <- mongo(
  collection = "imdb_movies_new",
  db = "group_project",
  url = "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/"
)

con_comments <- mongo(
  collection = "youtube_comments_final_zy",
  db = "group_project",
  url = "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/"
)

# ==========================
# 读取电影 + 【修复：过滤空名字】
# ==========================
movies <- con_movies$find() %>% filter(!is.na(中文片名) & 中文片名 != "")

cat("✅ 有效电影数量：", nrow(movies), "\n")
if(nrow(movies)==0) stop("❌ 没有有效电影名！")

con_comments$drop()

# ==========================
# 开始爬取（修复版）
# ==========================
for (i in 1:nrow(movies)) {
  movie_name <- movies$中文片名[i]
  cat("\n=============================================\n")
  cat("🎬 正在处理：", movie_name, "\n")

  search_url <- paste0(
    "https://www.googleapis.com/youtube/v3/search?",
    "q=", URLencode(paste(movie_name, "trailer")),
    "&part=id&maxResults=", MAX_VIDEOS_PER_MOVIE,
    "&type=video&key=", YOUTUBE_API_KEY
  )

  res <- GET(search_url)
  data <- fromJSON(rawToChar(res$content), flatten=TRUE)

  if (length(data$items) == 0) {
    cat("⚠️ 未找到视频\n")
    next
  }

  video_ids <- data$items$id.videoId

  for (vid in video_ids) {
    cat("   抓取视频：", vid, "\n")

    comment_url <- paste0(
      "https://www.googleapis.com/youtube/v3/commentThreads?",
      "videoId=", vid,
      "&part=snippet&maxResults=", MAX_COMMENTS_PER_VIDEO,
      "&textFormat=plainText&key=", YOUTUBE_API_KEY
    )

    try({
      res_c <- GET(comment_url)
      c_data <- fromJSON(rawToChar(res_c$content), flatten=TRUE)

      if (length(c_data$items) > 0) {
        comments <- c_data$items$snippet.topLevelComment.snippet.textDisplay
        authors <- c_data$items$snippet.topLevelComment.snippet.authorDisplayName
        times <- c_data$items$snippet.topLevelComment.snippet.publishedAt

        for (j in 1:length(comments)) {
          doc <- list(
            中文片名 = movie_name,
            video_id = vid,
            评论内容 = comments[j],
            作者 = authors[j],
            发布时间 = times[j]
          )
          con_comments$insert(doc)
        }
      }
    }, silent=TRUE)
  }
}

cat("\n🎉 🎉 🎉 全部爬取完成！\n")
count <- con_comments$count()
cat("✅ 成功爬取评论数：", count, "\n")
