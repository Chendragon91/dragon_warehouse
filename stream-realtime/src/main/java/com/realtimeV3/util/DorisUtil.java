package com.realtimeV3.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Doris数据工具类
 * 负责数据序列化和格式转换
 */
public class DorisUtil {
    private static final ObjectMapper objectMapper;
    private static final AtomicInteger errorCount = new AtomicInteger(0);
    private static final long ERROR_RESET_TIME = 5 * 60 * 1000; // 5分钟重置错误计数
    private static long lastErrorTime = 0;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    /**
     * 对象转换为JSON字符串
     */
    public static String convertToDorisJson(Object obj) {
        return convertToDorisJsonWithRetry(obj, 3);
    }

    /**
     * 带重试的对象转换
     */
    public static String convertToDorisJsonWithRetry(Object obj, int maxRetries) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                String json = objectMapper.writeValueAsString(obj);
                if (attempt > 1) {
                    System.out.println("JSON转换成功（第" + attempt + "次重试）");
                }
                return json;
            } catch (JsonProcessingException e) {
                if (attempt == maxRetries) {
                    return handleConversionError(obj, e, maxRetries);
                }

                // 等待后重试
                try {
                    Thread.sleep(50 * attempt); // 指数退避
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return createErrorJson("conversion_interrupted", ie.getMessage());
                }
            } catch (Exception e) {
                return handleConversionError(obj, e, maxRetries);
            }
        }
        return createErrorJson("unknown_error", "未知转换错误");
    }

    /**
     * 处理转换错误
     */
    private static String handleConversionError(Object obj, Exception e, int maxRetries) {
        int currentErrors = errorCount.incrementAndGet();
        long currentTime = System.currentTimeMillis();

        // 定期重置错误计数
        if (currentTime - lastErrorTime > ERROR_RESET_TIME) {
            errorCount.set(0);
            lastErrorTime = currentTime;
        }

        System.err.println("JSON转换失败（重试" + maxRetries + "次）: " + e.getMessage());
        System.err.println("错误对象: " + (obj != null ? obj.getClass().getSimpleName() : "null"));
        System.err.println("累计错误次数: " + currentErrors);

        if (currentErrors > 100) {
            System.err.println("警告：错误次数超过100，可能需要检查数据质量");
        }

        return createErrorJson("conversion_failed", e.getMessage());
    }

    /**
     * 创建错误JSON
     */
    private static String createErrorJson(String errorType, String message) {
        return String.format(
                "{\"error_type\": \"%s\", \"error_message\": \"%s\", \"timestamp\": %d, \"data_type\": \"error_record\"}",
                errorType,
                escapeJsonString(message),
                System.currentTimeMillis()
        );
    }

    /**
     * 转义JSON字符串中的特殊字符
     */
    private static String escapeJsonString(String input) {
        if (input == null) {
            return "null";
        }

        return input.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * 验证JSON格式
     */
    public static boolean isValidJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return false;
        }

        try {
            objectMapper.readTree(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 美化JSON输出
     */
    public static String prettyPrint(String json) {
        try {
            Object jsonObject = objectMapper.readValue(json, Object.class);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
        } catch (Exception e) {
            return json; // 返回原始JSON
        }
    }

    /**
     * 获取错误统计
     */
    public static int getErrorCount() {
        return errorCount.get();
    }

    /**
     * 重置错误统计
     */
    public static void resetErrorCount() {
        errorCount.set(0);
        lastErrorTime = System.currentTimeMillis();
    }

    /**
     * 获取ObjectMapper实例
     */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}