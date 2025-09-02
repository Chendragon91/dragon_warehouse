package com.realtimeV3;

import com.realtimeV3.model.CommentRecord;
import com.realtimeV3.model.SentimentResult;
import com.realtimeV3.process.CommentPreprocess;
import com.realtimeV3.process.SentimentAnalyzer;
import com.realtimeV3.process.DorisSinkBuilder;
import com.realtimeV3.source.KafkaCommentSource;
import com.realtimeV3.util.ConfigLoader;
import com.realtimeV3.util.DorisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.doris.flink.sink.DorisSink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Flink实时评论情感分析主程序
 * 负责整个处理管道的编排和执行
 */
public class FlinkJobMain {

    private static final String JOB_NAME = "JD-Comment-Sentiment-Analysis";
    private static final String VERSION = "3.0.0";

    public static void main(String[] args) throws Exception {
        // 打印启动信息
        printBanner();

        // 初始化Flink执行环境
        final StreamExecutionEnvironment env = createExecutionEnvironment();

        try {
            // 构建数据处理管道
            buildProcessingPipeline(env);

            // 执行任务
            System.out.println("开始执行Flink作业...");
            env.execute(JOB_NAME + "-V" + VERSION);

        } catch (Exception e) {
            System.err.println("Flink作业执行失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 创建和配置Flink执行环境
     */
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 加载配置
        Properties flinkProps = ConfigLoader.loadFlinkConfig();

        // 基础配置
        env.enableCheckpointing(Long.parseLong(flinkProps.getProperty("checkpoint.interval", "5000")));
        env.setParallelism(Integer.parseInt(flinkProps.getProperty("parallelism.default", "2")));
        env.setMaxParallelism(Integer.parseInt(flinkProps.getProperty("parallelism.max", "4")));

        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                Integer.parseInt(flinkProps.getProperty("restart.attempts", "3")),
                Time.of(Integer.parseInt(flinkProps.getProperty("restart.delay", "10")), TimeUnit.SECONDS)
        ));

        // 状态后端配置（生产环境建议配置）
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoints");

        System.out.println("Flink环境配置完成");
        System.out.println("并行度: " + env.getParallelism());
        System.out.println("检查点间隔: " + env.getCheckpointConfig().getCheckpointInterval() + "ms");

        return env;
    }

    /**
     * 构建数据处理管道
     */
    private static void buildProcessingPipeline(StreamExecutionEnvironment env) {
        System.out.println("开始构建数据处理管道...");

        // 创建Kafka数据源
        KafkaSource<CommentRecord> kafkaSource = KafkaCommentSource.createKafkaSource();
        System.out.println("Kafka数据源创建完成");

        // 构建数据流
        DataStream<CommentRecord> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<CommentRecord>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getCommentTimestamp()),
                "Kafka评论数据源"
        ).name("kafka-source").uid("kafka-source-1");

        // 数据预处理
        DataStream<CommentRecord> processedStream = sourceStream
                .map(new CommentPreprocess())
                .name("数据预处理")
                .uid("preprocess-1")
                .filter(record -> {
                    // 过滤掉错误记录和无效数据
                    boolean isValid = record.isValid() && !record.getDataId().startsWith("err_");
                    if (!isValid) {
                        System.err.println("过滤无效记录: " + record.getDataId());
                    }
                    return isValid;
                })
                .name("数据过滤")
                .uid("filter-1");

        // 情感分析处理
        DataStream<SentimentResult> sentimentStream = processedStream
                .map(new SentimentAnalyzer())
                .name("情感分析")
                .uid("sentiment-1")
                .filter(result -> {
                    // 过滤分析错误的结果
                    boolean isValid = !"分析错误".equals(result.getSentimentCategory());
                    if (!isValid) {
                        System.err.println("过滤分析错误记录: " + result.getDataId());
                    }
                    return isValid;
                })
                .name("结果过滤")
                .uid("result-filter-1");

        // 构建Doris Sink
        DorisSink<String> odsSink = DorisSinkBuilder.buildOdsSink();
        DorisSink<String> dwdSink = DorisSinkBuilder.buildDwdSink();

        System.out.println("Doris Sink构建完成");

        // 写入ODS层
        processedStream
                .map(record -> {
                    String json = DorisUtil.convertToDorisJson(record);
                    if (DorisUtil.isValidJson(json)) {
                        return json;
                    } else {
                        System.err.println("无效的JSON数据: " + record.getDataId());
                        return DorisUtil.createErrorJson("invalid_data", "数据转换失败");
                    }
                })
                .name("ODS数据转换")
                .uid("ods-converter-1")
                .sinkTo(odsSink)
                .name("ODS层存储")
                .uid("ods-sink-1");

        // 写入DWD层
        sentimentStream
                .map(result -> {
                    String json = DorisUtil.convertToDorisJson(result);
                    if (DorisUtil.isValidJson(json)) {
                        return json;
                    } else {
                        System.err.println("无效的情感分析JSON: " + result.getDataId());
                        return DorisUtil.createErrorJson("invalid_sentiment", "情感分析结果转换失败");
                    }
                })
                .name("DWD数据转换")
                .uid("dwd-converter-1")
                .sinkTo(dwdSink)
                .name("DWD层存储")
                .uid("dwd-sink-1");

        // 实时监控（可选）
        sentimentStream
                .map(result -> {
                    System.out.printf("处理完成: ID=%s, 评分=%d, 情感=%s, 置信度=%.2f%n",
                            result.getDataId(), result.getSentimentScore(),
                            result.getSentimentCategory(), result.getSentimentConfidence());
                    return result;
                })
                .name("处理监控")
                .uid("monitor-1");

        System.out.println("数据处理管道构建完成");
    }

    /**
     * 打印启动横幅
     */
    private static void printBanner() {
        System.out.println("================================================");
        System.out.println("      京东评论情感分析实时处理系统");
        System.out.println("      版本: " + VERSION);
        System.out.println("      作者: 实时计算团队");
        System.out.println("================================================");
        System.out.println("开始初始化系统...");
    }

    /**
     * 优雅关闭钩子
     */
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("正在关闭系统...");
            System.out.println("累计JSON转换错误: " + DorisUtil.getErrorCount());
            System.out.println("系统关闭完成");
        }));
    }
}