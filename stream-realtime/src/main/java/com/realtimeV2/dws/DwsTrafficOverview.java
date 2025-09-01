package com.realtimeV2.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;   // 你项目当前使用的老API（已弃用提示无碍编译）
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;  // 同上
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Properties;

/**
 * DWS - 流量总览：从 Kafka 读 V2_DWD 页面日志 -> 计算 UV/PV/跳失率 -> 写入 V2_DWS
 */
public class DwsTrafficOverview {

    // TODO: 替换为你的 Kafka 地址
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // 结合你的截图，主题名使用 V2_ 前缀
    private static final String SRC_TOPIC  = "V2_dwd_page_log";
    private static final String SINK_TOPIC = "V2_dws_traffic_overview";
    private static final String GROUP_ID   = "V2_dws_traffic_overview";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 1) Kafka Source（老API，与你DWD一致）
        Properties srcProps = new Properties();
        srcProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        srcProps.setProperty("group.id", GROUP_ID);
        // 如果你DWD里还有别的属性（如auto.offset.reset等），照抄加到这里即可
        FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>(
                SRC_TOPIC, new SimpleStringSchema(), srcProps
        );
        source.setStartFromLatest(); // 与你现有习惯一致，如需重跑可改为 setStartFromEarliest()

        DataStreamSource<String> pageLogDS = env.addSource(source);

        // 2) String -> JSON
        SingleOutputStreamOperator<JSONObject> jsonDS = pageLogDS.map(
                (MapFunction<String, JSONObject>) JSON::parseObject
        );

        // 3) 1分钟滚动窗口：UV、PV、跳失率（简化：last_page_id为空视作一次进入会话且跳出）
        SingleOutputStreamOperator<TrafficOverviewBean> trafficDS = jsonDS
                .keyBy(obj -> "all") // 全局单 key
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new TrafficAgg(), new TrafficWindowFunc());

        // 4) 写 Kafka Sink（老API，与你DWD一致）
        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);

        FlinkKafkaProducer<String> sink = new FlinkKafkaProducer<>(
                SINK_TOPIC, new SimpleStringSchema(), sinkProps
        );

        trafficDS
                .map(JSON::toJSONString)
                .addSink(sink);

        env.execute("DwsTrafficOverview");
    }

    // --- Bean ---
    public static class TrafficOverviewBean {
        public String stt;   // 窗口起始
        public String edt;   // 窗口结束
        public Long uvCt;    // 访客数
        public Long pvCt;    // 浏览量
        public Double bounceRate; // 跳失率(粗略)
        public Long payUserCt;    // 支付人数(预留)
        public Double payAmount;  // 支付金额(预留)
        public Double conversionRate; // 转化率(预留)

        public TrafficOverviewBean() {}

        public TrafficOverviewBean(String stt, String edt, Long uvCt, Long pvCt,
                                   Double bounceRate, Long payUserCt,
                                   Double payAmount, Double conversionRate) {
            this.stt = stt;
            this.edt = edt;
            this.uvCt = uvCt;
            this.pvCt = pvCt;
            this.bounceRate = bounceRate;
            this.payUserCt = payUserCt;
            this.payAmount = payAmount;
            this.conversionRate = conversionRate;
        }
    }

    /**
     * 累加器: (uvSet, pv, sessionCount, bounceCount)
     */
    public static class TrafficAgg implements AggregateFunction<JSONObject,
            Tuple4<HashSet<String>, Long, Long, Long>,
            Tuple4<Long, Long, Long, Long>> {

        @Override
        public Tuple4<HashSet<String>, Long, Long, Long> createAccumulator() {
            return Tuple4.of(new HashSet<>(), 0L, 0L, 0L);
        }

        @Override
        public Tuple4<HashSet<String>, Long, Long, Long> add(JSONObject v,
                                                             Tuple4<HashSet<String>, Long, Long, Long> acc) {
            if (v == null) {
                return acc;
            }

            // UV
            String uid = v.getString("user_id");
            if (uid != null) {
                acc.f0.add(uid);
            }

            // PV
            acc.f1 += 1;

            // 简化跳失：last_page_id 为空 -> 进入会话 + 跳出
            JSONObject page = v.getJSONObject("page");
            if (page != null) {
                String lastPage = page.getString("last_page_id");
                if (lastPage == null || lastPage.isEmpty()) {
                    acc.f2 += 1; // session++
                    acc.f3 += 1; // bounce++
                }
            }
            return acc;
        }

        @Override
        public Tuple4<HashSet<String>, Long, Long, Long> merge(
                Tuple4<HashSet<String>, Long, Long, Long> a,
                Tuple4<HashSet<String>, Long, Long, Long> b) {
            a.f0.addAll(b.f0);
            return Tuple4.of(a.f0, a.f1 + b.f1, a.f2 + b.f2, a.f3 + b.f3);
        }

        @Override
        public Tuple4<Long, Long, Long, Long> getResult(
                Tuple4<HashSet<String>, Long, Long, Long> acc) {
            return Tuple4.of((long) acc.f0.size(), acc.f1, acc.f2, acc.f3);
        }
    }

    // --- 窗口收尾 ---
    public static class TrafficWindowFunc extends ProcessWindowFunction<
            Tuple4<Long, Long, Long, Long>, TrafficOverviewBean, String, TimeWindow> {

        @Override
        public void process(String key,
                            Context ctx,
                            Iterable<Tuple4<Long, Long, Long, Long>> it,
                            Collector<TrafficOverviewBean> out) {

            Tuple4<Long, Long, Long, Long> r = it.iterator().next();
            long uv = r.f0;
            long pv = r.f1;
            long session = r.f2;
            long bounce = r.f3;

            double bounceRate = session == 0 ? 0.0 : (bounce * 1.0 / session);

            // 支付指标留口，后续和 V2_dwd_payment_info 关联
            long payUserCt = 0L;
            double payAmount = 0.0;
            double conversionRate = uv == 0 ? 0.0 : (payUserCt * 1.0 / uv);

            String stt = tsFormat(ctx.window().getStart());
            String edt = tsFormat(ctx.window().getEnd());

            out.collect(new TrafficOverviewBean(stt, edt, uv, pv, bounceRate, payUserCt, payAmount, conversionRate));
        }
    }

    private static String tsFormat(long ts) {
        return Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
