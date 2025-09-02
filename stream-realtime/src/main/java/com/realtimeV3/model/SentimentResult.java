package com.realtimeV3.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * DWD层情感分析结果数据模型
 * 包含原始数据和情感分析特征
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SentimentResult {
    @JsonProperty("data_id")
    private String dataId;

    @JsonProperty("user_id")
    private Integer userId;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("comment_time")
    private Timestamp commentTime;

    @JsonProperty("comment_title")
    private String commentTitle;

    @JsonProperty("comment_content")
    private String commentContent;

    @JsonProperty("original_rating")
    private Integer originalRating;

    @JsonProperty("sentiment_score")
    private Integer sentimentScore;

    @JsonProperty("comment_length")
    private Integer commentLength;

    @JsonProperty("has_positive_words")
    private Boolean hasPositiveWords;

    @JsonProperty("has_negative_words")
    private Boolean hasNegativeWords;

    @JsonProperty("sentiment_category")
    private String sentimentCategory;

    @JsonProperty("sentiment_confidence")
    private Double sentimentConfidence;

    @JsonProperty("process_time")
    private Timestamp processTime;

    @JsonProperty("product_category")
    private String productCategory;

    @JsonProperty("hour_of_day")
    private Integer hourOfDay;

    @JsonProperty("day_of_week")
    private Integer dayOfWeek;

    @JsonProperty("is_consistent")
    private Boolean isConsistent;

    /**
     * 情感评分与原始评分是否一致
     */
    public boolean isRatingConsistent() {
        if (originalRating == null || sentimentScore == null) {
            return false;
        }
        return Math.abs(originalRating - sentimentScore) <= 1;
    }

    /**
     * 获取情感强度描述
     */
    public String getSentimentIntensity() {
        if (sentimentConfidence == null) {
            return "未知";
        }
        if (sentimentConfidence > 0.8) {
            return "强烈";
        }
        if (sentimentConfidence > 0.6) {
            return "中等";
        }
        if (sentimentConfidence > 0.4) {
            return "轻微";
        }
        return "微弱";
    }
}