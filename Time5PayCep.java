package cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class Time5PayCep {
    public static void main(String[] args) throws Exception {
        //找出那些 5 秒钟内连续登录失败的账号，然后禁止用户再次尝试登录需要等待 1 分钟
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LogInEvent> source = env.fromElements(
                new LogInEvent(1L, "fail", 1597905234000L),
                new LogInEvent(1L, "success", 1597905235000L),
                new LogInEvent(2L, "fail", 1597905236000L),
                new LogInEvent(2L, "fail", 1597905237000L),
                new LogInEvent(2L, "fail", 1597905238000L),
                new LogInEvent(3L, "fail", 1597905239000L),
                new LogInEvent(3L, "success", 1597905240000L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogInEvent>(Time.milliseconds(500L)) {
            @Override
            public long extractTimestamp(LogInEvent logInEvent) {
                return logInEvent.getTimestamp();
            }
        }).keyBy(new KeySelector<LogInEvent, Object>() {
            @Override
            public Object getKey(LogInEvent value) throws Exception {
                return value.getuId();
            }
        });

        // 关键逻辑代码
        Pattern pattern = Pattern.<LogInEvent>begin("start").where(new IterativeCondition<LogInEvent>() {

            @Override
            public boolean filter(LogInEvent logInEvent, Context<LogInEvent> context) throws Exception {
                return logInEvent.getAction().equals("fail");
            }

        }).next("next").where(new IterativeCondition<LogInEvent>() {

            @Override
            public boolean filter(LogInEvent logInEvent, Context<LogInEvent> context) throws Exception {
                return logInEvent.getAction().equals("fail");
            }

        }).within(Time.seconds(5));


        PatternStream<LogInEvent> patternStream = CEP.pattern(source, pattern);
        SingleOutputStreamOperator<AlertEvent> process = patternStream.process(new PatternProcessFunction<LogInEvent, AlertEvent>() {
            @Override
            public void processMatch(Map<String, List<LogInEvent>> match, Context ctx, Collector<AlertEvent> out) throws Exception {
                List<LogInEvent> start = match.get("start");
                List<LogInEvent> next = match.get("next");
                long timestamp = ctx.timestamp();
                System.out.println("时间有什么:"+timestamp);
                System.out.println("start:" + start + ",next:" + next);

                out.collect(new AlertEvent(String.valueOf(start.get(0).getuId()), "Login fail ~ "));
            }
        });

        process.print();

        env.execute("execute cep");
    }
}
