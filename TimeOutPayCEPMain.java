package cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class TimeOutPayCEPMain {
    //找出那些下单后 10 分钟内没有支付的订单
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<PayEvent> source = env.fromElements(
                new PayEvent(1L, "create", 1597905234000L),
                new PayEvent(4L, "create", 1597905239000L),
                new PayEvent(1L, "pay", 1597906434000L),
                new PayEvent(2L, "create", 1597905236000L),
                new PayEvent(2L, "pay", 1597905237000L),
                new PayEvent(3L, "create", 1597905239000L)

        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<PayEvent>(Time.milliseconds(500L)) {
            @Override
            public long extractTimestamp(PayEvent payEvent) {
                return payEvent.getVolume();
            }
        }).keyBy(new KeySelector<PayEvent, Object>() {
            @Override
            public Object getKey(PayEvent value) throws Exception {
                return value.getId();
            }
        });

        // 逻辑处理代码
        OutputTag<PayEvent> orderTimeoutOutput = new OutputTag<PayEvent>("orderTimeout") {};
        Pattern<PayEvent, PayEvent> pattern = Pattern.<PayEvent>
                begin("begin")
                .where(new IterativeCondition<PayEvent>() {
                    @Override
                    public boolean filter(PayEvent payEvent, Context context) throws Exception {
                        return payEvent.getName().equals("create");
                    }
                })
                .followedBy("pay")
                .where(new IterativeCondition<PayEvent>() {
                    @Override
                    public boolean filter(PayEvent payEvent, Context context) throws Exception {
                        return payEvent.getName().equals("pay");
                    }
                })
                .within(Time.seconds(600));
                        //.timesOrMore(5);
        //第三种场景 希望找到24小时内,至少消费5次以上的订单;

        PatternStream<PayEvent> patternStream = CEP.pattern(source, pattern);
        SingleOutputStreamOperator<PayEvent> result = patternStream.select(orderTimeoutOutput, new PatternTimeoutFunction<PayEvent, PayEvent>() {
            @Override
            public PayEvent timeout(Map<String, List<PayEvent>> map, long l) throws Exception {
                System.out.println("timeout: "+map.get("begin").get(0));
                return map.get("begin").get(0);
            }
        }, new PatternSelectFunction<PayEvent, PayEvent>() {
            @Override
            public PayEvent select(Map<String, List<PayEvent>> map) throws Exception {
                for (Map.Entry<String, List<PayEvent>> stringListEntry : map.entrySet()) {
                    System.out.println("select ======key"+stringListEntry.getKey()+" value:"+stringListEntry.getValue());
                }
                System.out.println("select:"+map.get("pay").get(0));
                return map.get("pay").get(0);
            }
        });

        //result.print();
        DataStream<PayEvent> sideOutput = result.getSideOutput(orderTimeoutOutput);
        sideOutput.print();
        env.execute("execute cep");
    }
}
