package com.itmo.java.basics.index.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class SegmentOffsetInfoImpl implements SegmentOffsetInfo {
    private final long offset;

    public SegmentOffsetInfoImpl(long offset) {
        this.offset = offset;
    }

    @Override
    public long getOffset() {
        return this.offset;
    }
}
