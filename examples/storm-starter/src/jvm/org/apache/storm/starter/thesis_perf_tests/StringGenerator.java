package org.apache.storm.starter.thesis_perf_tests;

import org.apache.storm.starter.thesis_perf_tests.ValueGeneratorBase;

import java.util.List;
import java.util.Random;

public class StringGenerator extends ValueGeneratorBase {

    private static final String CHARACTER_POOL = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public StringGenerator(Random _random, List<Range> _ranges, int _num_values_to_generate) {
        super(_random, _ranges, _num_values_to_generate);
    }


    @Override
    protected Object NextValueInRange(Integer range_index) {
        Range range = super.ranges.get(range_index);
        int buffer_size = range.low.intValue();
        char[] string_buffer = new char[buffer_size];
        for (int i = 0; i < range.low; i++) {
            int char_pool_idx = random.nextInt() % CHARACTER_POOL.length();
            string_buffer[i] = CHARACTER_POOL.charAt(char_pool_idx);
        }
        return string_buffer;
    }
}
