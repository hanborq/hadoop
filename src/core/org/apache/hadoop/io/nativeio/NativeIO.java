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
package org.apache.hadoop.io.nativeio;

import java.io.FileDescriptor;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * JNI wrappers for various native IO-related calls not available in Java.
 * These functions should generally be used alongside a fallback to another
 * more portable mechanism.
 */
public class NativeIO {
  // Flags for open() call from bits/fcntl.h
  public static final int O_RDONLY   =    00;
  public static final int O_WRONLY   =    01;
  public static final int O_RDWR     =    02;
  public static final int O_CREAT    =  0100;
  public static final int O_EXCL     =  0200;
  public static final int O_NOCTTY   =  0400;
  public static final int O_TRUNC    = 01000;
  public static final int O_APPEND   = 02000;
  public static final int O_NONBLOCK = 04000;
  public static final int O_SYNC   =  010000;
  public static final int O_ASYNC  =  020000;
  public static final int O_FSYNC = O_SYNC;
  public static final int O_NDELAY = O_NONBLOCK;

  private static final Log LOG = LogFactory.getLog(NativeIO.class);

  private static boolean nativeLoaded = false;
  private static boolean workaroundNonThreadSafePasswdCalls = false;

  static final String WORKAROUND_NON_THREADSAFE_CALLS_KEY =
    "hadoop.workaround.non.threadsafe.getpwuid";
  static final boolean WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        Configuration conf = new Configuration();
        workaroundNonThreadSafePasswdCalls = conf.getBoolean(
          WORKAROUND_NON_THREADSAFE_CALLS_KEY,
          WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT);

        initNative();
        nativeLoaded = true;
      } catch (Throwable t) {
        // This can happen if the user has an older version of libhadoop.so
        // installed - in this case we can continue without native IO
        // after warning
        LOG.error("Unable to initialize NativeIO libraries", t);
      }
    }
  }

  /**
   * Return true if the JNI-based native IO extensions are available.
   */
  public static boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
  }

  /** Wrapper around open(2) */
  public static native FileDescriptor open(String path, int flags, int mode) throws IOException;

  /** Wrapper around chmod(2) */
  public static native void chmod(String path, int mode) throws IOException;

  private static native long getUIDForFDOwner(FileDescriptor fd) throws IOException;
  private static native String getUserName(long uid) throws IOException;

  /** Initialize the JNI method ID and class ID cache */
  private static native void initNative();
  
  private static class CachedUid {
    final long timestamp;
    final String username;
    public CachedUid(String username, long timestamp) {
      this.timestamp = timestamp;
      this.username = username;
    }
  }
  private static final Map<Long, CachedUid> uidCache = 
    new ConcurrentHashMap<Long, CachedUid>();
  private static long cacheTimeout;
  private static boolean initialized = false;
  
  public static String getOwner(FileDescriptor fd) throws IOException {
    ensureInitialized();
    long uid = getUIDForFDOwner(fd);
    CachedUid cUid = uidCache.get(uid);
    long now = System.currentTimeMillis();
    if (cUid != null && (cUid.timestamp + cacheTimeout) > now) {
      return cUid.username;
    }
    String user = getUserName(uid);
    LOG.debug("Got UserName " + user + " for UID " + uid + 
        " from the native implementation");
    cUid = new CachedUid(user, now);
    uidCache.put(uid, cUid);
    return user;
  }
    
  private synchronized static void ensureInitialized() {
    if (!initialized) {
      cacheTimeout = 
        new Configuration().getLong("hadoop.security.uid.cache.secs", 
                                     4*60*60) * 1000;
      LOG.debug("Initialized cache for UID to User mapping with a cache" +
      		" timeout of " + cacheTimeout/1000 + " seconds.");
      initialized = true;
    }
  }
}
