package com.realtimeV3.process;

import com.realtimeV3.model.CommentRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * 数据预处理函数
 * 负责数据清洗、验证和基础转换
 */
public class CommentPreprocess extends RichMapFunction<CommentRecord, CommentRecord> {
    private transient SimpleDateFormat dateFormat;
    private transient Counter processedCounter;
    private transient Counter errorCounter;
    private transient Meter processingRate;

    // 正则表达式模式
    private static final Pattern HTML_TAG_PATTERN = Pattern.compile("<[^>]+>");
    private static final Pattern SPECIAL_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9\\u4e00-\\u9fa5\\s，。！？；：（）【】《》、]");
    private static final Pattern MULTI_SPACE_PATTERN = Pattern.compile("\\s+");
    private static final Pattern URL_PATTERN = Pattern.compile("https?://\\S+");
    private static final Pattern EMOJI_PATTERN = Pattern.compile("[\\uD800-\\uDBFF][\\uDC00-\\uDFFF]");

    @Override
    public void open(Configuration parameters) throws Exception {
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 初始化指标
        processedCounter = getRuntimeContext().getMetricGroup().counter("processed_records");
        errorCounter = getRuntimeContext().getMetricGroup().counter("error_records");
        processingRate = getRuntimeContext().getMetricGroup().meter("processing_rate", new Meter() {
            @Override
            public void markEvent() {
                markEvent(1);
            }

            @Override
            public void markEvent(long n) {
                // 自动处理
            }

            @Override
            public double getRate() {
                return 0;
            }

            @Override
            public long getCount() {
                return 0;
            }
        });
    }

    @Override
    public CommentRecord map(CommentRecord record) throws Exception {
        try {
            processingRate.markEvent();

            // 跳过错误记录
            if (record.getUserId() != null && record.getUserId() == -999) {
                errorCounter.inc();
                return record;
            }

            // 数据清洗
            cleanCommentData(record);

            // 数据验证和补全
            validateAndComplete(record);

            processedCounter.inc();
            return record;

        } catch (Exception e) {
            errorCounter.inc();
            System.err.println("数据预处理失败: " + e.getMessage());
            return createErrorRecord(record, e);
        }
    }

    /**
     * 清洗评论数据
     */
    private void cleanCommentData(CommentRecord record) {
        if (record.getCommentContent() != null) {
            String cleanedContent = cleanText(record.getCommentContent());
            record.setCommentContent(cleanedContent);
        }

        if (record.getCommentTitle() != null) {
            String cleanedTitle = cleanText(record.getCommentTitle());
            record.setCommentTitle(cleanedTitle);
        }
    }

    /**
     * 文本清洗
     */
    private String cleanText(String text) {
        if (text == null || text.trim().isEmpty()) {
            return "无内容";
        }

        String cleaned = text;

        // 去除HTML标签
        cleaned = HTML_TAG_PATTERN.matcher(cleaned).replaceAll("");
        // 去除URL
        cleaned = URL_PATTERN.matcher(cleaned).replaceAll("[链接]");
        // 去除Emoji
        cleaned = EMOJI_PATTERN.matcher(cleaned).replaceAll("[表情]");
        // 去除特殊字符
        cleaned = SPECIAL_CHAR_PATTERN.matcher(cleaned).replaceAll(" ");
        // 合并多个空格
        cleaned = MULTI_SPACE_PATTERN.matcher(cleaned).replaceAll(" ").trim();

        // 限制长度
        if (cleaned.length() > 2000) {
            cleaned = cleaned.substring(0, 2000) + "...";
        }

        return cleaned;
    }

    /**
     * 数据验证和补全
     */
    private void validateAndComplete(CommentRecord record) {
        // 验证评分范围
        if (record.getRating() == null || record.getRating() < 1) {
            record.setRating(1);
        } else if (record.getRating() > 5) {
            record.setRating(5);
        }

        // 补全必要字段
        if (record.getDataId() == null || record.getDataId().trim().isEmpty()) {
            record.setDataId("gen_" + System.currentTimeMillis() + "_" + (int)(Math.random() * 1000));
        }

        if (record.getUserId() == null) {
            record.setUserId(-1);
        }

        if (record.getProductId() == null || record.getProductId().trim().isEmpty()) {
            record.setProductId("unknown_product");
        }

        if (record.getCommentTimestamp() == null) {
            record.setCommentTimestamp(System.currentTimeMillis());
        }

        // 设置处理时间
        record.setProcessTime(System.currentTimeMillis());
    }

    /**
     * 创建错误记录
     */
    private CommentRecord createErrorRecord(CommentRecord original, Exception e) {
        return new CommentRecord(
                original != null ? original.getDataId() : "err_" + System.currentTimeMillis(),
                original != null ? original.getUserId() : -999,
                original != null ? original.getProductId() : "error_product",
                System.currentTimeMillis(),
                "预处理错误",
                "错误信息: " + e.getMessage() + " | 原始标题: " + (original != null ? original.getCommentTitle() : "null"),
                3,
                System.currentTimeMillis()
        );
    }

    @Override
    public void close() throws Exception {
        // 清理资源
        if (dateFormat != null) {
            // 无需要清理的资源
        }
    }
}