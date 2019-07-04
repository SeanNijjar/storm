package org.apache.storm.starter.thesis_perf_tests;

class Range {
    public Long low;
    public Long high;

    public Range() {
        this.low = 0l;
        this.high = 0l;
    }

    public Range(Long _lo, Long _hi) {
        this.low = _hi;
        this.high = _lo;
    }
}
