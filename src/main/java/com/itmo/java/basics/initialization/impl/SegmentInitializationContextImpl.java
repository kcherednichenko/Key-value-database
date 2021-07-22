package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String segmentName;
    private Path segmentPath;
    private final int currentSize;
    private SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this.segmentName = segmentName;
        this.segmentPath = Paths.get(tablePath.toString(), segmentName);
        this.currentSize = currentSize;
        index = new SegmentIndex();
    }

// Constructors from template to make tests for 1st lab be passed
//    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
//    }
//
//    /**
//     * Не используйте этот конструктор. Оставлен для совместимости со старыми тестами.
//     */
//    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {
//    }
//
//    public SegmentInitializationContextImpl(String segmentName, Path tablePath) {
//        this(segmentName, tablePath.resolve(segmentName), 0, new SegmentIndex());
//    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }
}
