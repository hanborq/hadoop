package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;

public class UnsortedKVIterator<K extends Object, V extends Object> implements RawKeyValueIterator
{

    private static final Log LOG = LogFactory.getLog(UnsortedKVIterator.class);
    // fields
    private Configuration conf;
    private FileSystem fs;
    private CompressionCodec codec;
    private List<Segment<K, V>> segments = null;
    private Progressable reporter;
    private Progress mergeProgress = new Progress();

    // context
    private long totalBytesProcessed = 0;
    private float progPerByte;

    private DataInputBuffer key;
    private DataInputBuffer value;

    private Segment<K, V> currentSegment = null;
    private int current = 0;
    private Counters.Counter readsCounter = null;


    public UnsortedKVIterator(Configuration conf, FileSystem fs, List<Segment<K, V>> segments,
            Progressable reporter, boolean sortSegments)
    {
        this.conf = conf;
        this.fs = fs;
        this.segments = segments;
        this.reporter = reporter;
        if (sortSegments)
        {
            Collections.sort(segments);
        }
    }

    public UnsortedKVIterator(Configuration conf, FileSystem fs, List<Segment<K, V>> segments,
            Progressable reporter, boolean sortSegments, CompressionCodec codec)
    {
        this(conf, fs, segments, reporter, sortSegments);
        this.codec = codec;
    }

    public RawKeyValueIterator merge(Counters.Counter readsCounter, Counters.Counter writesCounter,
            Progress mergePhase) throws IOException
    {

        LOG.info("Feeding  " + segments.size() + " unsorted segments to reduce");
        if (mergePhase != null)
        {
            mergeProgress = mergePhase;
        }
        this.readsCounter = readsCounter;
        long totalBytes = computeBytesInMerges();
        if (totalBytes != 0)
        {
            progPerByte = 1.0f / (float) totalBytes;
        }
        return this;

    }

    private long computeBytesInMerges()
    {
        long result = 0;
        for (Segment<K, V> seg : segments)
        {
            result += seg.getLength();
        }
        return result;
    }

    @Override
    public void close() throws IOException
    {
        // do nothing.
    }

    @Override
    public DataInputBuffer getKey() throws IOException
    {
        return key;
    }

    @Override
    public DataInputBuffer getValue() throws IOException
    {
        return value;
    }

    @Override
    public Progress getProgress()
    {
        return mergeProgress;
    }

    @Override
    public boolean next() throws IOException
    {
        if (current >= segments.size())
            return false;

        if (currentSegment == null)
        {
            currentSegment = segments.get(current);
            currentSegment.init(readsCounter);
        }

        long startPos = currentSegment.getPosition();
        boolean hasNext = currentSegment.nextRawKey();
        long endPos = currentSegment.getPosition();
        totalBytesProcessed += endPos - startPos;

        if (!hasNext)
        {
            currentSegment.close();
            current++;
            currentSegment = null;
            return next();
        }

        startPos = currentSegment.getPosition();
        key = currentSegment.getKey();
        value = currentSegment.getValue();
        endPos = currentSegment.getPosition();
        totalBytesProcessed += endPos - startPos;
        mergeProgress.set(totalBytesProcessed * progPerByte);

        return true;
    }

}
