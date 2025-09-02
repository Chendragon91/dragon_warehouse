package com.realtimeV3.process;

import com.realtimeV3.model.CommentRecord;
import com.realtimeV3.model.SentimentResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * 情感分析处理器
 * 基于规则的情感分析，提取文本特征
 */
public class SentimentAnalyzer extends RichMapFunction<CommentRecord, SentimentResult> {

    // 情感词典
    private static final Set<String> POSITIVE_WORDS = new HashSet<>(Arrays.asList(
            "好", "很好", "非常好", "不错", "满意", "喜欢", "推荐", "超值", "划算", "漂亮",
            "好用", "质量好", "正品", "快递快", "服务好", "值得", "惊喜", "物美价廉", "完美",
            "优秀", "很棒", "给力", "舒服", "便捷", "高效", "耐用", "实惠", "惊喜", "惊艳",
            "点赞", "好评", "信赖", "放心", "愉快", "开心", "高兴", "满意", "赞", "棒"
    ));

    private static final Set<String> NEGATIVE_WORDS = new HashSet<>(Arrays.asList(
            "差", "很差", "不好", "不满意", "垃圾", "假货", "破损", "慢", "贵", "不值",
            "失望", "问题", "故障", "退货", "投诉", "骗人", "劣质", "糟糕", "后悔", "坑爹",
            "垃圾", "废物", "破烂", "慢死", "贵死", "上当", "欺骗", "垃圾货", "差劲", "烂",
            "恶心", "讨厌", "愤怒", "生气", "失望", "后悔", "投诉", "举报", "假", "劣质"
    ));

    // 强度修饰词
    private static final Set<String> INTENSIFIERS = new HashSet<>(Arrays.asList(
            "非常", "特别", "极其", "十分", "相当", "挺", "比较", "有点", "稍微", "略微"
    ));

    private static final Pattern NEGATION_PATTERN = Pattern.compile("(不|没|无|非|未)");

    private transient Calendar calendar;
    private transient Counter positiveCounter;
    private transient Counter negativeCounter;
    private transient Counter neutralCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        calendar = Calendar.getInstance();

        // 初始化指标
        positiveCounter = getRuntimeContext().getMetricGroup().counter("positive_comments");
        negativeCounter = getRuntimeContext().getMetricGroup().counter("negative_comments");
        neutralCounter = getRuntimeContext().getMetricGroup().counter("neutral_comments");
    }

    @Override
    public SentimentResult map(CommentRecord record) throws Exception {
        try {
            String content = record.getCommentContent() != null ? record.getCommentContent().toLowerCase() : "";
            String title = record.getCommentTitle() != null ? record.getCommentTitle().toLowerCase() : "";
            String fullText = title + " " + content;

            // 情感分析
            SentimentAnalysisResult analysisResult = analyzeSentiment(fullText);

            // 提取时间特征
            calendar.setTimeInMillis(record.getCommentTimestamp());
            int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
            int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

            // 构建结果
            SentimentResult result = buildSentimentResult(record, analysisResult, hourOfDay, dayOfWeek);

            // 更新指标
            updateMetrics(result.getSentimentCategory());

            return result;

        } catch (Exception e) {
            System.err.println("情感分析失败: " + e.getMessage());
            return createErrorResult(record, e);
        }
    }

    /**
     * 情感分析核心逻辑
     */
    private SentimentAnalysisResult analyzeSentiment(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new SentimentAnalysisResult(3, "中性", 0.5, false, false);
        }

        int positiveCount = countWords(text, POSITIVE_WORDS);
        int negativeCount = countWords(text, NEGATIVE_WORDS);
        boolean hasNegation = NEGATION_PATTERN.matcher(text).find();

        // 考虑否定词的影响
        if (hasNegation) {
            // 简单的否定处理：如果有否定词，反转情感倾向
            int temp = positiveCount;
            positiveCount = negativeCount;
            negativeCount = temp;
        }

        int score = calculateScore(positiveCount, negativeCount);
        String category = determineCategory(score, positiveCount, negativeCount);
        double confidence = calculateConfidence(text, positiveCount, negativeCount);

        return new SentimentAnalysisResult(score, category, confidence, positiveCount > 0, negativeCount > 0);
    }

    private int calculateScore(int positiveCount, int negativeCount) {
        if (positiveCount == 0 && negativeCount == 0) {
            return 3;
        }
        if (positiveCount > negativeCount * 2) {
            return 5;
        }
        if (positiveCount > negativeCount) {
            return 4;
        }
        if (positiveCount == negativeCount) {
            return 3;
        }
        if (negativeCount > positiveCount) {
            return 2;
        }
        return 1;
    }

    private String determineCategory(int score, int positiveCount, int negativeCount) {
        if (score >= 4) {
            return "正面";
        }
        if (score <= 2) {
            return "负面";
        }
        if (positiveCount > 0 && negativeCount == 0) {
            return "偏正面";
        }
        if (negativeCount > 0 && positiveCount == 0) {
            return "偏负面";
        }
        return "中性";
    }

    private double calculateConfidence(String text, int positiveCount, int negativeCount) {
        int totalWords = text.split("\\s+").length;
        if (totalWords == 0) {
            return 0.5;
        }

        int relevantWords = positiveCount + negativeCount;
        double baseConfidence = Math.min(1.0, relevantWords / (double) totalWords * 3);

        // 根据情感强度调整置信度
        if (relevantWords >= 3) {
            baseConfidence = Math.min(1.0, baseConfidence * 1.2);
        }

        return Math.max(0.1, Math.min(1.0, baseConfidence));
    }

    private int countWords(String text, Set<String> words) {
        int count = 0;
        for (String word : words) {
            if (text.contains(word)) {
                count++;
            }
        }
        return count;
    }

    private SentimentResult buildSentimentResult(CommentRecord record, SentimentAnalysisResult analysis,
                                                 int hourOfDay, int dayOfWeek) {
        return new SentimentResult(
                record.getDataId(),
                record.getUserId(),
                record.getProductId(),
                new Timestamp(record.getCommentTimestamp()),
                record.getCommentTitle(),
                record.getCommentContent(),
                record.getRating(),
                analysis.score,
                analysis.score,
                analysis.hasPositive,
                analysis.hasNegative,
                analysis.category,
                analysis.confidence,
                new Timestamp(System.currentTimeMillis()),
                extractProductCategory(record.getProductId()),
                hourOfDay,
                dayOfWeek,
                Math.abs(record.getRating() - analysis.score) <= 1
        );
    }

    private String extractProductCategory(String productId) {
        if (productId == null) {
            return "未知";
        }

        productId = productId.toLowerCase();
        if (productId.contains("book") || productId.contains("书")) {
            return "图书";
        }
        if (productId.contains("phone") || productId.contains("手机")) {
            return "手机";
        }
        if (productId.contains("cloth") || productId.contains("服装")) {
            return "服装";
        }
        if (productId.contains("food") || productId.contains("食品")) {
            return "食品";
        }
        if (productId.contains("elec") || productId.contains("电器")) {
            return "电器";
        }
        if (productId.contains("computer") || productId.contains("电脑")) {
            return "电脑";
        }
        if (productId.contains("shoe") || productId.contains("鞋")) {
            return "鞋靴";
        }

        return "其他";
    }

    private void updateMetrics(String category) {
        switch (category) {
            case "正面": positiveCounter.inc(); break;
            case "负面": negativeCounter.inc(); break;
            default: neutralCounter.inc(); break;
        }
    }

    private SentimentResult createErrorResult(CommentRecord record, Exception e) {
        return new SentimentResult(
                record.getDataId(),
                record.getUserId(),
                record.getProductId(),
                new Timestamp(record.getCommentTimestamp()),
                record.getCommentTitle(),
                "情感分析失败: " + e.getMessage(),
                record.getRating(),
                3,
                0,
                false,
                false,
                "分析错误",
                0.1,
                new Timestamp(System.currentTimeMillis()),
                "未知",
                0,
                0,
                false
        );
    }

    @Override
    public void close() throws Exception {
        // 清理资源
    }

    // 情感分析结果内部类
    private static class SentimentAnalysisResult {
        final int score;
        final String category;
        final double confidence;
        final boolean hasPositive;
        final boolean hasNegative;

        SentimentAnalysisResult(int score, String category, double confidence,
                                boolean hasPositive, boolean hasNegative) {
            this.score = score;
            this.category = category;
            this.confidence = confidence;
            this.hasPositive = hasPositive;
            this.hasNegative = hasNegative;
        }
    }
}