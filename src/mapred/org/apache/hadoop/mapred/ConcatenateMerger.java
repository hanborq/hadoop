package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.IFile.IStreamWriter;
import org.apache.hadoop.util.Progressable;

public class ConcatenateMerger<K, V>
{
    private static final Log LOG = LogFactory.getLog(ConcatenateMerger.class);

    public static <K extends Object, V extends Object> void writeFile(IStreamWriter writer,
            List<Segment<K, V>> segments, Progressable progressable, Counters.Counter readsCounter,
            Configuration conf) throws IOException
    {
        if (LOG.isDebugEnabled())
        {
            StringBuilder sb = new StringBuilder();
            sb.append(" Merging " + segments.size() + " files :[");
            for (Segment<K, V> segment : segments)
            {
                sb.append(" Compressed Length: " + segment.getLength() + ","
                        + "Uncompressed Lenghth :" + segment.getRawLen() + ";");
            }
            sb.append("]");
            LOG.debug(sb.toString());
        }
        for (int i = 0; i < segments.size(); i++)
        {
            Segment<K, V> segment = segments.get(i);
            segment.init(readsCounter);
            segment.writeTo(writer, progressable, conf);
            segment.close();
        }
    }
}
