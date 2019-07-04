package org.apache.storm.starter.thesis_perf_tests;

import java.util.List;
import java.util.Random;


public class LongGenerator extends ValueGeneratorBase {

    public LongGenerator(Random _random, List<Range> _ranges, int _num_values_to_generate) {
        super(_random, _ranges, _num_values_to_generate);
    }

    protected Object NextValueInRange(Integer range_index) {
        Range range = super.ranges.get(range_index);
        long range_size = range.high - range.low;
        Long val = (super.random.nextLong() % range_size) + range.low;
        return val;
    }
}

