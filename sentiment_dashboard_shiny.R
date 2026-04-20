system("sudo apt-get update -y")
system("sudo apt-get install -y libssl-dev libsasl2-dev")
system("sudo apt-get install -y libmongoc-dev libbson-dev")

# ==========================
#  再安装 mongolite
# ==========================
install.packages("mongolite")
install.packages(c("shiny", "shinyjs", "plotly", "wordcloud2", "DT", "ggplot2"))
# ============================================================
#  YouTube 电影评论情感分析 - Shiny 可视化应用
# ============================================================

library(shiny)
library(shinyjs)
library(dplyr)
library(tidyr)
library(ggplot2)
library(plotly)
library(mongolite)
library(stringr)
library(wordcloud2)
library(DT)

# ==========================
# 1. 数据连接配置
# ==========================
mongo_uri <- "mongodb+srv://stat2630_db_user:123456lele@stat2630cluster0.dn0nos0.mongodb.net/group_project"

# ==========================
# 2. 数据加载函数
# ==========================
load_data <- function() {
  # 加载情感分析结果
  con_result <- mongo(collection = "youtube_sentiment_result_optimized5",
                       db = "textusing", url = mongo_uri)
  result <- con_result$find()

  # 加载 IMDb 数据
  con_imdb <- mongo(collection = "imdb_movies_new", db = "group_project", url = mongo_uri)
  imdb <- con_imdb$find()

  # 数据预处理
  result$ensemble_score <- as.integer(result$ensemble_score)
  result$label <- as.numeric(result$label)

  imdb <- imdb %>%
    mutate(
      movie_name_clean = tolower(trimws(`中文片名`)),
      IMDb_score = as.numeric(`IMDb评分`)
    )

  # 计算每部电影的情感统计
  sentiment_by_movie <- result %>%
    filter(label != 0.5) %>%
    group_by(movie_name) %>%
    summarise(
      total_comments = n(),
      pos_count = sum(ensemble_score == 1, na.rm = TRUE),
      neg_count = sum(ensemble_score == 0, na.rm = TRUE),
      pos_rate = mean(ensemble_score, na.rm = TRUE) * 100,
      avg_likes = mean(like_count, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(movie_name_clean = tolower(trimws(gsub("_", " ", movie_name))))

  # 合并 IMDb 数据
  final_data <- merge(imdb, sentiment_by_movie,
                      by = "movie_name_clean", all.x = TRUE)

  list(
    result = result,
    imdb = imdb,
    sentiment_by_movie = sentiment_by_movie,
    final_data = final_data
  )
}

# 情感词典（用于词云）
pos_words <- c(
  "good", "great", "love", "best", "excellent", "amazing", "perfect",
  "awesome", "fantastic", "beautiful", "wonderful", "brilliant",
  "outstanding", "superb", "magnificent", "masterpiece", "incredible",
  "recommend", "enjoy", "enjoyable", "fun", "exciting", "thrilling",
  "funny", "hilarious", "touching", "emotional", "inspiring",
  "gorgeous", "stunning", "spectacular", "impressive",
  "favorite", "liked", "nice", "pleasant", "satisfying",
  "喜欢", "好看", "精彩", "棒", "赞", "爱", "完美", "经典"
)

neg_words <- c(
  "bad", "worst", "boring", "terrible", "waste", "hate", "awful",
  "horrible", "disappointing", "disappointed", "annoying",
  "ridiculous", "pathetic", "garbage", "trash", "crap", "sucks",
  "poor", "weak", "failed", "fail", "overrated", "predictable",
  "slow", "tedious", "nonsense", "pointless", "waste of time",
  "okay", "meh", "mediocre", "bland", "flat", "dull",
  "avoid", "skip", "flop", "cringe", "mess", "disaster",
  "难 看", "无聊", "烂片", "差", "失望", "垃圾片", "恐怖片"
)

# ==========================
# 3. UI 定义
# ==========================
ui <- fluidPage(
  useShinyjs(),

  # 标题和样式
  titlePanel("YouTube 电影评论情感分析 Dashboard"),

  tags$head(
    tags$style(HTML("
      .well { background-color: #f8f9fa; }
      .shiny-html-output h4 { color: #2c3e50; margin-top: 20px; }
      .info-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 15px;
        border-radius: 10px;
        text-align: center;
        margin: 10px 0;
      }
      .metric-card {
        background: white;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        text-align: center;
      }
      .metric-value { font-size: 36px; font-weight: bold; color: #667eea; }
      .metric-label { font-size: 14px; color: #666; }
      .tab-content { padding: 20px 0; }
    "))
  ),

  mainPanel(width = "100%",
    # 主内容区
    tabsetPanel(
      id = "mainTabs",

      # ==================== 总览 ====================
      tabPanel("总览",
        fluidRow(
          column(3,
            div(class = "metric-card",
              div(class = "metric-value", textOutput("total_movies")),
              div(class = "metric-label", "电影数量")
            )
          ),
          column(3,
            div(class = "metric-card",
              div(class = "metric-value", textOutput("total_comments")),
              div(class = "metric-label", "评论总数")
            )
          ),
          column(3,
            div(class = "metric-card",
              div(class = "metric-value", textOutput("avg_pos_rate")),
              div(class = "metric-label", "平均正向率")
            )
          ),
          column(3,
            div(class = "metric-card",
              div(class = "metric-value", textOutput("correlation")),
              div(class = "metric-label", "与IMDb相关系数")
            )
          )
        ),

        fluidRow(
          column(6,
            h4("各电影情感正向率"),
            plotlyOutput("pos_rate_bar", height = "400px")
          ),
          column(6,
            h4("IMDb评分 vs 情感正向率"),
            plotlyOutput("correlation_scatter", height = "400px")
          )
        ),

        fluidRow(
          column(12,
            h4("情感分布对比"),
            plotlyOutput("sentiment_dist", height = "350px")
          )
        )
      ),

      # ==================== 电影详情 ====================
      tabPanel("电影详情",
        sidebarLayout(
          sidebarPanel(
            selectInput("select_movie", "选择电影:",
                       choices = NULL,
                       width = "100%"),
            hr(),
            h5("电影信息"),
            htmlOutput("movie_info"),
            width = 3
          ),
          mainPanel(
            fluidRow(
              column(4,
                div(class = "metric-card",
                  div(class = "metric-value", textOutput("movie_pos_rate")),
                  div(class = "metric-label", "正向率")
                )
              ),
              column(4,
                div(class = "metric-card",
                  div(class = "metric-value", textOutput("movie_comment_count")),
                  div(class = "metric-label", "有效评论数")
                )
              ),
              column(4,
                div(class = "metric-card",
                  div(class = "metric-value", textOutput("movie_imdb")),
                  div(class = "metric-label", "IMDb评分")
                )
              )
            ),
            fluidRow(
              column(6,
                h4("情感分布"),
                plotlyOutput("movie_sentiment_pie", height = "300px")
              ),
              column(6,
                h4("点赞分布"),
                plotlyOutput("movie_likes_dist", height = "300px")
              )
            ),
            fluidRow(
              column(12,
                h4("评论示例"),
                DT::dataTableOutput("movie_comments")
              )
            )
          )
        )
      ),

      # ==================== 词云 ====================
      tabPanel("词云分析",
        sidebarLayout(
          sidebarPanel(
            selectInput("wordcloud_movie", "选择电影（留空为全局）:",
                       choices = c("全部电影" = ""),
                       width = "100%"),
            radioButtons("wordcloud_type", "词云类型:",
                        choices = c(
                          "正面词云" = "positive",
                          "负面词云" = "negative",
                          "全部情感词" = "all"
                        )),
            actionButton("generate_wordcloud", "生成词云",
                        icon = icon("refresh"),
                        class = "btn-primary"),
            width = 3
          ),
          mainPanel(
            h4("情感词云"),
            wordcloud2Output("sentiment_wordcloud", height = "500px"),
            width = 9
          )
        )
      ),

      # ==================== 模型评估 ====================
      tabPanel("模型评估",
        fluidRow(
          column(4,
            div(class = "metric-card",
              div(class = "metric-value", textOutput("model_accuracy_lr")),
              div(class = "metric-label", "逻辑回归准确率")
            )
          ),
          column(4,
            div(class = "metric-card",
              div(class = "metric-value", textOutput("model_accuracy_svm")),
              div(class = "metric-label", "SVM 准确率")
            )
          ),
          column(4,
            div(class = "metric-card",
              div(class = "metric-value", textOutput("model_accuracy_ens")),
              div(class = "metric-label", "集成模型准确率")
            )
          )
        ),

        fluidRow(
          column(6,
            h4("模型对比"),
            plotlyOutput("model_comparison", height = "300px")
          ),
          column(6,
            h4("预测 vs 实际标签"),
            plotlyOutput("confusion_heatmap", height = "300px")
          )
        ),

        fluidRow(
          column(12,
            h4("评论情感分布（原始标签）"),
            plotlyOutput("label_distribution", height = "300px")
          )
        )
      ),

      # ==================== 数据表格 ====================
      tabPanel("数据表格",
        fluidRow(
          column(12,
            h4("完整数据"),
            DT::dataTableOutput("full_data_table")
          )
        )
      )
    )
  )
)

# ==========================
# 4. Server 逻辑
# ==========================
server <- function(input, output, session) {

  # 加载数据
  data <- load_data()
  result <- data$result
  imdb <- data$imdb
  sentiment_by_movie <- data$sentiment_by_movie
  final_data <- data$final_data

  # 更新电影选择器
  movie_choices <- c("全部电影" = "", sort(unique(result$movie_name)))
  updateSelectInput(session, "select_movie", choices = movie_choices)
  updateSelectInput(session, "wordcloud_movie", choices = movie_choices)

  # ==================== 总览指标 ====================
  output$total_movies <- renderText({
    length(unique(result$movie_name))
  })

  output$total_comments <- renderText({
    nrow(result)
  })

  output$avg_pos_rate <- renderText({
    labeled <- result[result$label != 0.5, ]
    paste0(round(mean(labeled$ensemble_score, na.rm = TRUE) * 100, 1), "%")
  })

  output$correlation <- renderText({
    matched <- final_data[!is.na(final_data$pos_rate) & !is.na(final_data$IMDb_score), ]
    if (nrow(matched) >= 3) {
      corr <- cor(matched$IMDb_score, matched$pos_rate, use = "complete.obs")
      round(corr, 3)
    } else {
      "N/A"
    }
  })

  # ==================== 总览图表 ====================
  output$pos_rate_bar <- renderPlotly({
    plot_data <- sentiment_by_movie %>%
      arrange(desc(pos_rate)) %>%
      mutate(movie_display = substr(movie_name, 1, 15))

    colors <- ifelse(plot_data$pos_rate > 70, "#27ae60",
                    ifelse(plot_data$pos_rate > 50, "#f39c12", "#e74c3c"))

    p <- ggplot(plot_data, aes(x = reorder(movie_display, pos_rate), y = pos_rate,
                              fill = pos_rate)) +
      geom_bar(stat = "identity") +
      scale_fill_gradient(low = "#e74c3c", high = "#27ae60", name = "正向率(%)") +
      coord_flip() +
      labs(x = "电影", y = "情感正向率 (%)", title = "") +
      theme_minimal() +
      theme(legend.position = "none",
            text = element_text(size = 12))

    ggplotly(p, tooltip = c("x", "y")) %>%
      layout(height = 400)
  })

  output$correlation_scatter <- renderPlotly({
    plot_data <- final_data[!is.na(final_data$pos_rate) & !is.na(final_data$IMDb_score), ]

    p <- ggplot(plot_data, aes(x = IMDb_score, y = pos_rate,
                               text = `中文片名`)) +
      geom_point(aes(size = total_comments, color = pos_rate), alpha = 0.7) +
      geom_smooth(method = "lm", se = TRUE, color = "#667eea") +
      scale_color_gradient(low = "#e74c3c", high = "#27ae60") +
      labs(x = "IMDb 评分", y = "情感正向率 (%)",
           title = paste("相关系数: ", round(cor(plot_data$IMDb_score, plot_data$pos_rate), 3))) +
      theme_minimal()

    ggplotly(p, tooltip = c("text", "x", "y", "size"))
  })

  output$sentiment_dist <- renderPlotly({
    # 按电影分组的情感分布
    plot_data <- sentiment_by_movie %>%
      tidyr::gather(key = "sentiment", value = "count", pos_count, neg_count) %>%
      mutate(
        sentiment = factor(sentiment, levels = c("neg_count", "pos_count"),
                          labels = c("负面", "正面")),
        movie_display = substr(movie_name, 1, 15)
      )

    p <- ggplot(plot_data, aes(x = reorder(movie_display, count), y = count,
                                fill = sentiment)) +
      geom_bar(stat = "identity", position = "stack") +
      scale_fill_manual(values = c("负面" = "#e74c3c", "正面" = "#27ae60")) +
      labs(x = "电影", y = "评论数量", fill = "情感") +
      coord_flip() +
      theme_minimal()

    ggplotly(p)
  })

  # ==================== 电影详情 ====================
  observeEvent(input$select_movie, {
    req(input$select_movie != "")

    movie_data <- result[result$movie_name == input$select_movie, ]
    movie_stats <- sentiment_by_movie[sentiment_by_movie$movie_name == input$select_movie, ]
    movie_imdb <- final_data[final_data$movie_name == input$select_movie, ]

    output$movie_info <- renderUI({
      HTML(paste0(
        "<b>中文片名:</b> ", movie_imdb$`中文片名`[1], "<br>",
        "<b>英文片名:</b> ", movie_imdb$`英文片名`[1], "<br>",
        "<b>年份:</b> ", movie_imdb$`年代`[1], "<br>",
        "<b>类型:</b> ", movie_imdb$`类型`[1], "<br>"
      ))
    })

    output$movie_pos_rate <- renderText({
      if (nrow(movie_stats) > 0) {
        paste0(round(movie_stats$pos_rate[1], 1), "%")
      } else "N/A"
    })

    output$movie_comment_count <- renderText({
      if (nrow(movie_stats) > 0) movie_stats$total_comments[1] else "N/A"
    })

    output$movie_imdb <- renderText({
      if (nrow(movie_imdb) > 0) movie_imdb$IMDb_score[1] else "N/A"
    })
  })

  output$movie_sentiment_pie <- renderPlotly({
    req(input$select_movie != "")
    movie_data <- result[result$movie_name == input$select_movie, ]

    labeled <- movie_data[movie_data$label != 0.5, ]

    if (nrow(labeled) == 0) {
      return(plotly_empty() %>% add_text(text = "无有效评论"))
    }

    pie_data <- data.frame(
      sentiment = c("正面", "负面"),
      count = c(sum(labeled$ensemble_score == 1, na.rm = TRUE),
                sum(labeled$ensemble_score == 0, na.rm = TRUE))
    )

    plot_ly(pie_data, labels = ~sentiment, values = ~count, type = "pie",
            colors = c("正面" = "#27ae60", "负面" = "#e74c3c"),
            textinfo = "label+percent") %>%
      layout(showlegend = FALSE)
  })

  output$movie_likes_dist <- renderPlotly({
    req(input$select_movie != "")
    movie_data <- result[result$movie_name == input$select_movie, ]

    p <- ggplot(movie_data, aes(x = like_count, fill = factor(ensemble_score))) +
      geom_histogram(bins = 30, alpha = 0.7) +
      scale_fill_manual(values = c("0" = "#e74c3c", "1" = "#27ae60", "0.5" = "#95a5a6"),
                       labels = c("负面", "正面", "中性")) +
      labs(x = "点赞数", y = "评论数", fill = "情感") +
      theme_minimal()

    ggplotly(p)
  })

  output$movie_comments <- DT::renderDataTable({
    req(input$select_movie != "")
    movie_data <- result[result$movie_name == input$select_movie, ]

    display_data <- movie_data %>%
      select(comment_cleaned, ensemble_score, like_count, view_count) %>%
      mutate(
        sentiment_label = case_when(
          ensemble_score == 1 ~ "正面",
          ensemble_score == 0 ~ "负面",
          TRUE ~ "中性"
        ),
        comment_cleaned = substr(comment_cleaned, 1, 100)
      ) %>%
      rename(
        "评论内容" = comment_cleaned,
        "情感" = sentiment_label,
        "点赞数" = like_count,
        "观看数" = view_count
      ) %>%
      arrange(desc(点赞数))

    DT::datatable(display_data, options = list(
      pageLength = 10,
      lengthMenu = c(5, 10, 20, 50),
      dom = "Bfrtip"
    ))
  })

  # ==================== 词云 ====================
  wordcloud_data <- eventReactive(input$generate_wordcloud, {
    if (input$wordcloud_movie == "") {
      texts <- result$comment_cleaned
    } else {
      texts <- result$comment_cleaned[result$movie_name == input$wordcloud_movie]
    }

    all_text <- paste(texts, collapse = " ")
    words <- unlist(strsplit(tolower(all_text), "\\s+"))

    # 停用词
    stopwords <- c(
      "the", "a", "an", "is", "are", "was", "were", "be", "been",
      "to", "of", "in", "for", "on", "with", "as", "at", "by",
      "and", "or", "it", "this", "that", "i", "you", "he", "she",
      "we", "they", "my", "your", "his", "her", "its", "our", "their",
      "have", "has", "had", "do", "does", "did", "will", "would",
      "just", "like", "get", "got", "make", "made", "so", "but",
      "if", "when", "what", "which", "who", "how", "than", "then",
      "very", "really", "even", "all", "only", "also", "much", "out",
      "more", "most", "no", "can", "from", "up", "about", "into",
      "movie", "film", "movies", "films", "see", "watch", "watching",
      "dont", "im", "youre", "thats", "cant", "wont", "wasnt",
      "scene", "scenes", "character", "characters", "plot", "story",
      "thing", "things", "one", "way", "lot", "point", "people",
      "first", "last", "new", "old", "year", "years", "time"
    )

    words <- words[!words %in% stopwords]
    word_freq <- sort(table(words), decreasing = TRUE)

    # 根据类型筛选
    if (input$wordcloud_type == "positive") {
      target_words <- pos_words
    } else if (input$wordcloud_type == "negative") {
      target_words <- neg_words
    } else {
      target_words <- c(pos_words, neg_words)
    }

    freq_df <- data.frame(
      word = names(word_freq),
      freq = as.numeric(word_freq),
      stringsAsFactors = FALSE
    )
    freq_df <- freq_df[freq_df$word %in% target_words, ]
    freq_df <- freq_df[order(freq_df$freq, decreasing = TRUE), ]

    head(freq_df, 100)
  })

  output$sentiment_wordcloud <- renderWordcloud2({
    wc_data <- wordcloud_data()
    if (nrow(wc_data) == 0) {
      return(wordcloud2(data.frame(word = "无数据", freq = 1)))
    }
    wordcloud2(wc_data, size = 0.8, color = "random-light",
               backgroundColor = "white", minRotation = 0, maxRotation = 0)
  })

  # ==================== 模型评估 ====================
  output$model_accuracy_lr <- renderText({
    labeled <- result[result$label != 0.5, ]
    paste0(round(mean(labeled$prediction == labeled$label, na.rm = TRUE) * 100, 1), "%")
  })

  output$model_accuracy_svm <- renderText({
    labeled <- result[result$label != 0.5, ]
    paste0(round(mean(labeled$svm_prediction == labeled$label, na.rm = TRUE) * 100, 1), "%")
  })

  output$model_accuracy_ens <- renderText({
    labeled <- result[result$label != 0.5, ]
    paste0(round(mean(labeled$ensemble_score == labeled$label, na.rm = TRUE) * 100, 1), "%")
  })

  output$model_comparison <- renderPlotly({
    acc_data <- data.frame(
      模型 = c("逻辑回归", "SVM", "集成模型"),
      准确率 = c(
        mean(result[result$label != 0.5, ]$prediction == result[result$label != 0.5, ]$label, na.rm = TRUE) * 100,
        mean(result[result$label != 0.5, ]$svm_prediction == result[result$label != 0.5, ]$label, na.rm = TRUE) * 100,
        mean(result[result$label != 0.5, ]$ensemble_score == result[result$label != 0.5, ]$label, na.rm = TRUE) * 100
      )
    )

    p <- ggplot(acc_data, aes(x = 模型, y = 准确率, fill = 模型)) +
      geom_bar(stat = "identity", width = 0.5) +
      scale_fill_manual(values = c("逻辑回归" = "#3498db", "SVM" = "#9b59b6", "集成模型" = "#667eea")) +
      ylim(0, 100) +
      geom_text(aes(label = paste0(round(准确率, 1), "%")), vjust = -0.5) +
      labs(x = "", y = "准确率 (%)", title = "") +
      theme_minimal() +
      theme(legend.position = "none")

    ggplotly(p)
  })

  output$confusion_heatmap <- renderPlotly({
    labeled <- result[result$label != 0.5, ]

    # 计算混淆矩阵数据
    conf_data <- labeled %>%
      filter(!is.na(ensemble_score), !is.na(label)) %>%
      group_by(label, ensemble_score) %>%
      summarise(count = n(), .groups = "drop")

    p <- ggplot(conf_data, aes(x = factor(label), y = factor(ensemble_score),
                               fill = count, text = count)) +
      geom_tile() +
      scale_fill_gradient(low = "white", high = "#667eea") +
      labs(x = "实际标签", y = "预测标签", fill = "数量") +
      theme_minimal()

    ggplotly(p, tooltip = "text")
  })

  output$label_distribution <- renderPlotly({
    labeled <- result[result$label != 0.5, ]

    dist_data <- labeled %>%
      group_by(movie_name, ensemble_score) %>%
      summarise(count = n(), .groups = "drop") %>%
      mutate(ensemble_score = factor(ensemble_score, levels = c(0, 1),
                                    labels = c("负面", "正面")))

    p <- ggplot(dist_data, aes(x = reorder(movie_name, count), y = count,
                               fill = ensemble_score)) +
      geom_bar(stat = "identity", position = "fill") +
      scale_fill_manual(values = c("负面" = "#e74c3c", "正面" = "#27ae60")) +
      labs(x = "电影", y = "比例", fill = "预测情感") +
      coord_flip() +
      theme_minimal()

    ggplotly(p)
  })

  # ==================== 数据表格 ====================
  output$full_data_table <- DT::renderDataTable({
    display_data <- result %>%
      mutate(
        sentiment_label = case_when(
          ensemble_score == 1 ~ "正面",
          ensemble_score == 0 ~ "负面",
          TRUE ~ "中性"
        ),
        comment_display = substr(comment_cleaned, 1, 80)
      ) %>%
      select(movie_name, comment_display, sentiment_label,
             like_count, view_count, label) %>%
      rename(
        "电影" = movie_name,
        "评论" = comment_display,
        "情感" = sentiment_label,
        "点赞" = like_count,
        "观看" = view_count,
        "原始标签" = label
      )

    DT::datatable(display_data, options = list(
      pageLength = 20,
      lengthMenu = c(10, 20, 50, 100),
      dom = "Bfrtip",
      scrollX = TRUE
    ))
  })
}

# ==========================
# 5. 启动应用
# ==========================
shinyApp(ui = ui, server = server)
