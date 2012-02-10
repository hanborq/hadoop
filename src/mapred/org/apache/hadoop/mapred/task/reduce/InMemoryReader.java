/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.task.reduce;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.IFile.IStreamWriter;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.util.Progressable;

/**
 * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
 */

public class InMemoryReader<K, V> extends Reader<K, V>
{
    private final TaskAttemptID taskAttemptId;
    private final MergeManager<K, V> merger;
    // DataInputBuffer memDataIn = new DataInputBuffer();
    private int start;
    private int length;

    public InMemoryReader(MergeManager<K, V> merger, TaskAttemptID taskAttemptId, byte[] data,
            int start, int length) throws IOException
    {
        super(null, null, length - start, null, null);
        this.merger = merger;
        this.taskAttemptId = taskAttemptId;

        buffer = data;
        bufferSize = (int) fileLength;
        dataIn.reset(buffer, start, length);
        this.start = start;
        this.length = length;
    }

    @Override
    public void reset(int offset)
    {
        dataIn.reset(buffer, start + offset, length);
        bytesRead = offset;
        eof = false;
    }

    @Override
    public long getPosition() throws IOException
    {
        // InMemoryReader does not initialize streams like Reader, so
        // in.getPos()
        // would not work. Instead, return the number of uncompressed bytes
        // read,
        // which will be correct since in-memory data is not compressed.
        return bytesRead;
    }

    @Override
    public long getLength()
    {
        return fileLength;
    }

    private void dumpOnError()
    {
        File dumpFile = new File("../output/" + taskAttemptId + ".dump");
        System.err.println("Dumping corrupt map-output of " + taskAttemptId + " to "
                + dumpFile.getAbsolutePath());
        try
        {
            FileOutputStream fos = new FileOutputStream(dumpFile);
            fos.write(buffer, 0, bufferSize);
            fos.close();
        }
        catch (IOException ioe)
        {
            System.err.println("Failed to dump map-output of " + taskAttemptId);
        }
    }

    @Override
    public boolean nextRawKey(DataInputBuffer key) throws IOException
    {
        try
        {
            return super.nextRawKey(key);
        }
        catch (IOException ioe)
        {
            dumpOnError();
            throw ioe;
        }
    }

    @Override
    public void nextRawValue(DataInputBuffer value) throws IOException
    {

        try
        {
            super.nextRawValue(value);
        }
        catch (IOException ioe)
        {
            dumpOnError();
            throw ioe;
        }
    }

    @Override
    public void close()
    {
        // Release
        dataIn = null;
        buffer = null;
        // Inform the MergeManager
        if (merger != null)
        {
            merger.unreserve(bufferSize);
        }
    }

    /**
     * writing (dumping) byte-array data to output stream. 
     */
    @Override
    public void dumpTo(IStreamWriter writer, Progressable progressable, Configuration conf)
            throws IOException
    {
        writer.write(buffer, start, length - IFile.LEN_OF_EOF);

        DataInputBuffer din = new DataInputBuffer();
        din.reset(buffer, start + length - IFile.LEN_OF_EOF, IFile.LEN_OF_EOF);
        verifyEOF(din);
    }
}
