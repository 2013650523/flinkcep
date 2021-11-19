package cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.util.List;
import java.util.Map;
import java.util.Random;


public class FlinkCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Event> input = env.fromElements(
                new Event(42L, "start", 42L),
                new Event(2L, "fail", 1597905236000L),
                new Event(2L, "end", 1597905237000L),
                new Event(2L, "fail", 1597905238000L),
                new Event(3L, "fail", 1597905239000L),
                new Event(3L, "success", 1597905240000L)
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.milliseconds(500L)) {
            @Override
            public long extractTimestamp(Event logInEvent) {
                return logInEvent.getVolume();
            }
        });
       /*         .keyBy(new KeySelector<Event, Object>() {
            @Override
            public Object getKey(Event value) throws Exception {
                return value.getId();
            }
        });
*/

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        System.out.println("start: "+event.toString());
                        return event.getId() == 42;
                    }
                }
        ).next("middle").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event subEvent) {
                        System.out.println("middle:"+subEvent.toString());
                        return subEvent.getVolume() >= 10.0;
                    }
                }
        ).followedBy("end").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        System.out.println("end: "+event.toString());
                        return event.getName().equals("end");
                    }
                }
        );
       // System.out.println("---------------");
        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        DataStream<AlertEvent> result = patternStream.process(
                new PatternProcessFunction<Event, AlertEvent>() {
                    @Override
                    public void processMatch(
                            Map<String, List<Event>> pattern,
                            Context ctx,
                            Collector<AlertEvent> out) throws Exception {

                        System.err.println(pattern.get("middle").get(0));
                        for (Map.Entry<String, List<Event>> s : pattern.entrySet()) {
                            System.out.println(s.getKey());
                            System.out.println("-------11111111-----");
                            System.out.println(s.getValue().toString());

                        }
                        out.collect(new AlertEvent("liu","name"));
                    }
                });
        result.print();
        env.execute("执行");
    }
}
