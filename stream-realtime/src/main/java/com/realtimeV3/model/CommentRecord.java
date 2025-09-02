package com.realtimeV3.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ODS层原始评论数据模型
 * 对应Kafka中的JSON消息结构
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommentRecord {
    @JsonProperty("data_id")
    private String dataId;

    @JsonProperty("user_id")
    private Integer userId;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("comment_timestamp")
    private Long commentTimestamp;

    @JsonProperty("comment_title")
    private String commentTitle;

    @JsonProperty("comment_content")
    private String commentContent;

    @JsonProperty("rating")
    private Integer rating;

    @JsonProperty("process_time")
    private Long processTime;

    /**
     * 获取格式化的评论时间
     */
    public String getFormattedCommentTime() {
        if (commentTimestamp == null) {
            return "未知时间";
        }
        return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(new java.util.Date(commentTimestamp));
    }

    /**
     * 验证数据基本完整性
     */
    public boolean isValid() {
        return dataId != null && !dataId.trim().isEmpty() &&
                userId != null && userId > 0 &&
                productId != null && !productId.trim().isEmpty() &&
                commentTimestamp != null && commentTimestamp > 0 &&
                rating != null && rating >= 1 && rating <= 5;
    }
}