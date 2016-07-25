/*
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.remote.artery.compress;

import akka.actor.ActorRef;
import scala.util.hashing.MurmurHash3;

import java.util.Random;

/**
 * INTERNAL API: Count-Min Sketch datastructure.
 * <p>
 * Not thread-safe.
 * <p>
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 * <p>
 * This implementation is mostly taken and adjusted from the Apache V2 licensed project `stream-lib`, located here:
 * https://github.com/clearspring/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/frequency/CountMinSketch.java
 */
public class CountMinSketch {

  public static final long PRIME_MODULUS = (1L << 31) - 1;

  private int depth;
  private int width;
  private long[][] table;
  private long size;
  private double eps;
  private double confidence;

  private int[] recyclableCMSHashBuckets;


  public CountMinSketch(int depth, int width, int seed) {
    this.depth = depth;
    this.width = width;
    this.eps = 2.0 / width;
    this.confidence = 1 - 1 / Math.pow(2, depth);
    recyclableCMSHashBuckets = preallocateHashBucketsArray(depth);
    initTablesWith(depth, width, seed);
  }


  private void initTablesWith(int depth, int width, int seed) {
    this.table = new long[depth][width];
  }

  /**
   * Referred to as {@code epsilon} in the whitepaper
   */
  public double relativeError() {
    return eps;
  }

  public double confidence() {
    return confidence;
  }

  /**
   * Similar to {@code add}, however we reuse the fact that the hask buckets have to be calculated for {@code add}
   * already, and a separate {@code estimateCount} operation would have to calculate them again, so we do it all in one go.
   */
  public long addObjectAndEstimateCount(Object item, long count) {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented");
    }
    Murmur3Hash.hashBuckets(item, recyclableCMSHashBuckets, width);
    for (int i = 0; i < depth; ++i) {
      table[i][recyclableCMSHashBuckets[i]] += count;
    }
    size += count;
    return estimateCount(recyclableCMSHashBuckets);
  }

  public long size() {
    return size;
  }

  /**
   * The estimate is correct within {@code 'epsilon' * (total item count)},
   * with probability {@code confidence}.
   *
   * @param buckets the "indexes" of buckets from which we want to calculate the count
   */
  private long estimateCount(int[] buckets) {
    long res = Long.MAX_VALUE;
    for (int i = 0; i < depth; ++i) {
      res = Math.min(res, table[i][buckets[i]]);
    }
    return res;
  }


  private static class Murmur3Hash {

    /**
     * Force all bits of the hash to avalanche. Used for finalizing the hash.
     */
    private static int avalanche(int hash) {
      int h = hash;

      h ^= h >>> 16;
      h *= 0x85ebca6b;
      h ^= h >>> 13;
      h *= 0xc2b2ae35;
      h ^= h >>> 16;

      return h;
    }

    private static int mixLast(int hash, int data) {
      int k = data;

      k *= 0xcc9e2d51; //c1
      k = Integer.rotateLeft(k, 15);
      k *= 0x1b873593; //c2

      return hash ^ k;
    }


    private static int mix(int hash, int data) {
      int h = mixLast(hash, data);
      h = Integer.rotateLeft(h, 13);
      return h * 5 + 0xe6546b64;
    }

    public static int hash(Object o) {
      if (o == null) {
        return 0;
      }
      if (o instanceof ActorRef) { // TODO possibly scary optimisation
        // ActorRef hashcode is the ActorPath#uid, which is a random number assigned at its creation,
        // thus no hashing happens here - the value is already cached.
        // TODO it should be thought over if this preciseness (just a random number, and not hashing) is good enough here?
        return o.hashCode();
      }
      if (o instanceof String) {
        return hash(((String) o).getBytes());
      }
      if (o instanceof Long) {
        return hashLong((Long) o, 0);
      }
      if (o instanceof Integer) {
        return hashLong((Integer) o, 0);
      }
      if (o instanceof Double) {
        return hashLong(Double.doubleToRawLongBits((Double) o), 0);
      }
      if (o instanceof Float) {
        return hashLong(Float.floatToRawIntBits((Float) o), 0);
      }
      if (o instanceof byte[]) {
        return bytesHash((byte[]) o, 0);
      }
      return hash(o.toString());
    }

    private static final long highLongMask = 0xFFFFFFFF00000000L;

    static int hashLong(long value, int seed) {
      int h = seed;
      h = mix(h, (int) (value));
      h = mixLast(h, (int) ((value & highLongMask) >> 32));
      return avalanche(h ^ 2);
    }

    public static int bytesHash(byte[] data, int seed) {
      int len = data.length;
      int h = seed;

      // Body
      int i = 0;
      while (len >= 4) {
        int k = data[i] & 0xFF;
        k |= (data[i + 1] & 0xFF) << 8;
        k |= (data[i + 2] & 0xFF) << 16;
        k |= (data[i + 3] & 0xFF) << 24;

        h = mix(h, k);

        i += 4;
        len -= 4;
      }

      // Tail
      int k = 0;
      if (len == 3) k ^= (data[i + 2] & 0xFF) << 16;
      if (len >= 2) k ^= (data[i + 1] & 0xFF) << 8;
      if (len >= 1) {
        k ^= (data[i] & 0xFF);
        h = mixLast(h, k);
      }

      // Finalization
      return avalanche(h ^ data.length);
    }

    static void hashBuckets(Object item, int[] hashBuckets, int max) {
      int hash1 = hash(item); // specialized hash for ActorRef and Strings
      int hash2 = hashLong(hash1, hash1);
      final int depth = hashBuckets.length;
      for (int i = 0; i < depth; i++)
        hashBuckets[i] = Math.abs((hash1 + i * hash2) % max);
    }

  }

  /**
   * This is copied from https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/hash/MurmurHash.java
   * Which is Apache V2 licensed.
   * <p></p>
   * This is a very fast, non-cryptographic hash suitable for general hash-based
   * lookup. See http://murmurhash.googlepages.com/ for more details.
   * <p/>
   * <p>
   * The C version of MurmurHash 2.0 found at that site was ported to Java by
   * Andrzej Bialecki (ab at getopt org).
   * </p>
   */
  // TODO replace with Scala's Murmur3, it's much faster
  private static class MurmurHash {

    public static int hash(Object o) {
      if (o == null) {
        return 0;
      }
      if (o instanceof ActorRef) { // TODO possibly scary optimisation
        // ActorRef hashcode is the ActorPath#uid, which is a random number assigned at its creation, 
        // thus no hashing happens here - the value is already cached.
        // TODO it should be thought over if this preciseness (just a random number, and not hashing) is good enough here?
        return o.hashCode();
      }
      if (o instanceof String) {
        return hash(((String) o).getBytes());
      }
      if (o instanceof Long) {
        return hashLong((Long) o);
      }
      if (o instanceof Integer) {
        return hashLong((Integer) o);
      }
      if (o instanceof Double) {
        return hashLong(Double.doubleToRawLongBits((Double) o));
      }
      if (o instanceof Float) {
        return hashLong(Float.floatToRawIntBits((Float) o));
      }
      if (o instanceof byte[]) {
        return hash((byte[]) o);
      }
      return hash(o.toString());
    }

    public static int hash(byte[] data) {
      return hash(data, data.length, -1);
    }

    public static int hash(byte[] data, int seed) {
      return hash(data, data.length, seed);
    }

    public static int hash(byte[] data, int length, int seed) {
      int m = 0x5bd1e995;
      int r = 24;

      int h = seed ^ length;

      int len_4 = length >> 2;

      for (int i = 0; i < len_4; i++) {
        int i_4 = i << 2;
        int k = data[i_4 + 3];
        k = k << 8;
        k = k | (data[i_4 + 2] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 1] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 0] & 0xff);
        k *= m;
        k ^= k >>> r;
        k *= m;
        h *= m;
        h ^= k;
      }

      // avoid calculating modulo
      int len_m = len_4 << 2;
      int left = length - len_m;

      if (left != 0) {
        if (left >= 3) {
          h ^= (int) data[length - 3] << 16;
        }
        if (left >= 2) {
          h ^= (int) data[length - 2] << 8;
        }
        if (left >= 1) {
          h ^= (int) data[length - 1];
        }

        h *= m;
      }

      h ^= h >>> 13;
      h *= m;
      h ^= h >>> 15;

      return h;
    }

    public static int hashLong(long data) {
      return hashLong(data, 0);
    }

    public static int hashLong(long data, int seed) {
      int m = 0x5bd1e995;
      int r = 24;

      int h = seed;
      // int h = seed ^ length;

      int k = (int) data * m;
      k ^= k >>> r;
      h ^= k * m;

      k = (int) (data >> 32) * m;
      k ^= k >>> r;
      h *= m;
      h ^= k * m;

      h ^= h >>> 13;
      h *= m;
      h ^= h >>> 15;

      return h;
    }

    /**
     * Mutates passed in {@code hashBuckets}
     */
    static void hashBuckets(Object item, int[] hashBuckets, int max) {
      int hash1 = hash(item); // specialized hash for ActorRef and Strings
      int hash2 = hashLong(hash1, hash1);
      final int depth = hashBuckets.length;
      for (int i = 0; i < depth; i++)
        hashBuckets[i] = Math.abs((hash1 + i * hash2) % max);
    }
  }

  public int[] preallocateHashBucketsArray(int depth) {
    return new int[depth];
  }

  @Override
  public String toString() {
    return "CountMinSketch{" +
      "confidence=" + confidence +
      ", size=" + size +
      ", depth=" + depth +
      ", width=" + width +
      '}';
  }
}
