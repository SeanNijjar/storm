package org.apache.storm.starter.thesis_perf_tests;

import java.util.*;

import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;



/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class SyntheticTopology extends ConfigurableTopology {
    static int num_tuples;
    static int num_fields_per_tuple;

    static ArrayList<ValueGeneratorBase> value_generators;
    static BaseRichBolt test_bolt;

    Random random;

    public SyntheticTopology(Random _random) {
        random = _random;
    }

    private static List<Range> ReadRanges(String ranges_string) {
        ArrayList<Range> ranges_list = new ArrayList<>();
        String[] ranges_arg_partitioned = ranges_string.split("=");

        assert(ranges_arg_partitioned[0].compareTo("ranges") == 0);
        String[] ranges = ranges_arg_partitioned[1].split(",");

        for (String range: ranges) {
            String[] range_bounds = range.split("-");
            assert(range_bounds.length == 2);

            Long hi = Long.getLong(range_bounds[1]);
            Long lo = Long.getLong(range_bounds[0]);
            assert(lo <= hi);
            ranges_list.add(new Range(lo, hi));
        }

        return ranges_list;
    }

    private static ValueGeneratorBase CreateValueGenerator(Random random, String generator_type, List<Range> ranges) {
        ValueGeneratorBase value_generator = null;

        generator_type.toLowerCase();
        switch(generator_type) {
            case "byte": {
                value_generator = new ByteGenerator(random, ranges, 1000000);
            } break;

            case "short": {
                value_generator = new ShortGenerator(random, ranges, 1000000);
            } break;

            case "int":
            case "integer": {
                value_generator = new IntegerGenerator(random, ranges, 1000000);
            } break;

            case "long": {
                value_generator = new LongGenerator(random, ranges, 1000000);
            } break;

            case "float": {
                value_generator = new FloatGenerator(random, ranges, 1000000);
            } break;

            case "double": {
                value_generator = new DoubleGenerator(random, ranges, 1000000);
            } break;

            case "string": {
                value_generator = new StringGenerator(random, ranges, 1000000);
            } break;
        };

        return value_generator;
    }

    private static void ProcessArgs(String[] args, Random random) {
        for (String arg: args) {
            String[] arg_segments = arg.split(":", 2);

            String arg_name = arg_segments[0];
            switch (arg_name) {
                case "n_tuples": {
                    num_tuples = Integer.getInteger(arg_segments[1]);
                }

                case "n_fields": {
                    num_fields_per_tuple = Integer.getInteger(arg_segments[1]);
                    value_generators.add(null);

                    for (int i = 0; i < num_fields_per_tuple; i++) {
                        value_generators.add(null);
                    }
                } break;

                case "default_type": {
                    assert(value_generators.size() > 0);
                    String[] default_type_arg_parts = arg_segments[1].split(":");

                    List<Range> ranges = ReadRanges(arg_segments[1]);
                    ValueGeneratorBase value_generator = CreateValueGenerator(random, default_type_arg_parts[0], ranges);

                    for (int i = 0; i < num_fields_per_tuple; i++) {
                        value_generators.set(i, value_generator);
                    }

                } break;

                case "type_override": {
                    assert(value_generators.size() > 0);
                    String[] default_type_arg_parts = arg_segments[1].split(":");
                    String[] field_index_strings = default_type_arg_parts[1].split(",");
                    List<Range> ranges = ReadRanges(default_type_arg_parts[2]);
                    ValueGeneratorBase value_generator = CreateValueGenerator(random, default_type_arg_parts[0], ranges);

                    for (String field_index_string: field_index_strings) {
                        value_generators.set(Integer.getInteger(field_index_string), value_generator);
                    }
                } break;
            };

        }
    }

    public static void main(String[] args) throws Exception {
        Random random = new Random();
        ProcessArgs(args, random);

        SyntheticTopology topology = new SyntheticTopology(random);
        ConfigurableTopology.start(topology, args);
    }



    protected int run(String[] args) throws Exception {



        TopologyBuilder builder = new TopologyBuilder();

        SyntheticBenchmarkSpout spout = new SyntheticBenchmarkSpout(value_generators, random);
        builder.setSpout("spout", spout, 5);

        builder.setBolt("split", new org.apache.storm.starter.WordCountTopology.SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new org.apache.storm.starter.WordCountTopology.WordCount(), 12).fieldsGrouping("split", new Fields("word"));

        conf.setDebug(true);

        String topologyName = "word-count";

        conf.setNumWorkers(3);

        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        return submit(topologyName, conf, builder);
    }

    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }


    //@HeteroBolt
    public static class PassthroughBolt extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        OutputCollector collector;

        //@Override
        void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector _collector) {
            collector = _collector;
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            List<Object> tuple_fields = tuple.getValues();
            collector.emit(new Values(tuple_fields));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //declarer.declare(new Fields("word", "count"));
        }
    }
}

