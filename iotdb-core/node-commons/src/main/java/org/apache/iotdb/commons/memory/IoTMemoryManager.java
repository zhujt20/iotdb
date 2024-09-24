/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.memory;

import org.apache.iotdb.commons.exception.memory.SystemRuntimeOutOfMemoryCriticalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongUnaryOperator;

public class IoTMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTMemoryManager.class);

  /** whether enable memory management */
  private final boolean MEMORY_MANAGEMENT_ENABLED;

  /** max retry time of allocating memory */
  private final int MEMORY_ALLOCATE_MAX_RETRIES;

  /** retry interval of allocating memory, unit: ms */
  private final long MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS;

  /** total size of memory manager, unit: bytes */
  private final long TOTAL_MEMORY_SIZE_IN_BYTES;

  /** minimum size of allocating memory, unit: bytes */
  private final long MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES;

  private long usedMemorySizeInBytes;

  private final Set<IoTMemoryBlock> allocatedBlocks = new HashSet<>();

  public IoTMemoryManager(
      boolean is_enable_memory_management,
      int memory_size_in_bytes,
      int min_allocate_memory_size_in_bytes,
      int max_retry_times,
      long retry_interval_in_ms) {
    this.MEMORY_MANAGEMENT_ENABLED = is_enable_memory_management;
    this.TOTAL_MEMORY_SIZE_IN_BYTES = memory_size_in_bytes;
    this.MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES = min_allocate_memory_size_in_bytes;
    this.MEMORY_ALLOCATE_MAX_RETRIES = max_retry_times;
    this.MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS = retry_interval_in_ms;
    // TODO @spricoder register period job to expand all
  }

  // region Allocate Memory Block

  public synchronized IoTMemoryBlock tryAllocate(long sizeInBytes) {
    return tryAllocate(sizeInBytes, currentSize -> currentSize * 2 / 3);
  }

  public synchronized IoTMemoryBlock tryAllocate(
      long sizeInBytes, LongUnaryOperator customAllocateStrategy) {
    if (!MEMORY_MANAGEMENT_ENABLED) {
      return new IoTMemoryBlock(sizeInBytes);
    }

    if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
      return registerMemoryBlock(sizeInBytes);
    }

    long sizeToAllocateInBytes = sizeInBytes;
    while (sizeToAllocateInBytes > MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeToAllocateInBytes) {
        LOGGER.info(
            "tryAllocate: allocated memory, "
                + "total memory size {} bytes, used memory size {} bytes, "
                + "original requested memory size {} bytes,"
                + "actual requested memory size {} bytes",
            TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes,
            sizeInBytes,
            sizeToAllocateInBytes);
        return registerMemoryBlock(sizeToAllocateInBytes);
      }

      sizeToAllocateInBytes =
          Math.max(
              customAllocateStrategy.applyAsLong(sizeToAllocateInBytes),
              MEMORY_ALLOCATE_MIN_SIZE_IN_BYTES);
    }

    if (tryShrink4Allocate(sizeToAllocateInBytes)) {
      LOGGER.info(
          "tryAllocate: allocated memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "original requested memory size {} bytes,"
              + "actual requested memory size {} bytes",
          TOTAL_MEMORY_SIZE_IN_BYTES,
          usedMemorySizeInBytes,
          sizeInBytes,
          sizeToAllocateInBytes);
      return registerMemoryBlock(sizeToAllocateInBytes);
    } else {
      LOGGER.warn(
          "tryAllocate: failed to allocate memory, "
              + "total memory size {} bytes, used memory size {} bytes, "
              + "requested memory size {} bytes",
          TOTAL_MEMORY_SIZE_IN_BYTES,
          usedMemorySizeInBytes,
          sizeInBytes);
      return registerMemoryBlock(0);
    }
  }

  public synchronized boolean tryAllocate(
      IoTMemoryBlock block, long memoryInBytesNeededToBeAllocated) {
    if (!MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= memoryInBytesNeededToBeAllocated) {
      usedMemorySizeInBytes += memoryInBytesNeededToBeAllocated;
      block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() + memoryInBytesNeededToBeAllocated);
      return true;
    }

    return false;
  }

  public synchronized IoTMemoryBlock forceAllocate(long sizeInBytes)
      throws SystemRuntimeOutOfMemoryCriticalException {
    if (!MEMORY_MANAGEMENT_ENABLED) {
      return new IoTMemoryBlock(sizeInBytes);
    }

    for (int i = 1; i <= MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
        return registerMemoryBlock(sizeInBytes);
      }

      try {
        tryShrink4Allocate(sizeInBytes);
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceAllocate: interrupted while waiting for available memory", e);
      }
    }

    throw new SystemRuntimeOutOfMemoryCriticalException(
        String.format(
            "forceAllocate: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  /**
   * Allocate a {@link IoTMemoryBlock} only if memory already used is less than the specified
   * threshold.
   *
   * @param sizeInBytes size of memory needed to allocate
   * @param usedThreshold proportion of memory used, ranged from 0.0 to 1.0
   * @return {@code null} if the proportion of memory already used exceeds {@code usedThreshold}.
   *     Will return a memory block otherwise.
   */
  public synchronized IoTMemoryBlock forceAllocateIfSufficient(
      long sizeInBytes, float usedThreshold) {
    if (usedThreshold < 0.0f || usedThreshold > 1.0f) {
      return null;
    }

    if (!MEMORY_MANAGEMENT_ENABLED) {
      return new IoTMemoryBlock(sizeInBytes);
    }

    if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes
        && (float) usedMemorySizeInBytes / TOTAL_MEMORY_SIZE_IN_BYTES < usedThreshold) {
      return forceAllocate(sizeInBytes);
    } else {
      long memoryToShrink =
          Math.max(
              usedMemorySizeInBytes - (long) (TOTAL_MEMORY_SIZE_IN_BYTES * usedThreshold),
              sizeInBytes);
      if (tryShrink4Allocate(memoryToShrink)) {
        return forceAllocate(sizeInBytes);
      }
    }
    return null;
  }

  private IoTMemoryBlock registerMemoryBlock(long sizeInBytes) {
    usedMemorySizeInBytes += sizeInBytes;

    final IoTMemoryBlock returnedMemoryBlock = new IoTMemoryBlock(sizeInBytes);
    allocatedBlocks.add(returnedMemoryBlock);
    return returnedMemoryBlock;
  }

  // endregion

  // region Manage Memory Block

  /**
   * Force resize memory block to target size
   *
   * @param block target memory block
   * @param targetSize target memory size
   */
  public synchronized void forceResize(IoTMemoryBlock block, long targetSize) {
    if (block == null || block.isReleased()) {
      LOGGER.warn("forceResize: cannot resize a null or released memory block");
      return;
    }

    if (!MEMORY_MANAGEMENT_ENABLED) {
      block.setMemoryUsageInBytes(targetSize);
      return;
    }

    final long oldSize = block.getMemoryUsageInBytes();

    if (oldSize >= targetSize) {
      usedMemorySizeInBytes -= oldSize - targetSize;
      block.setMemoryUsageInBytes(targetSize);
      return;
    }

    long sizeInBytes = targetSize - oldSize;
    for (int i = 1; i <= MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
        usedMemorySizeInBytes += sizeInBytes;
        block.setMemoryUsageInBytes(targetSize);
        return;
      }

      try {
        tryShrink4Allocate(sizeInBytes);
        this.wait(MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("forceResize: interrupted while waiting for available memory", e);
      }
    }

    throw new SystemRuntimeOutOfMemoryCriticalException(
        String.format(
            "forceResize: failed to allocate memory after %d retries, "
                + "total memory size %d bytes, used memory size %d bytes, "
                + "requested memory size %d bytes",
            MEMORY_ALLOCATE_MAX_RETRIES,
            TOTAL_MEMORY_SIZE_IN_BYTES,
            usedMemorySizeInBytes,
            sizeInBytes));
  }

  private boolean tryShrink4Allocate(long sizeInBytes) {
    final List<IoTMemoryBlock> shuffledBlocks = new ArrayList<>(allocatedBlocks);
    Collections.shuffle(shuffledBlocks);

    while (true) {
      boolean hasAtLeastOneBlockShrinkable = false;
      for (final IoTMemoryBlock block : shuffledBlocks) {
        if (block.shrink()) {
          hasAtLeastOneBlockShrinkable = true;
          if (TOTAL_MEMORY_SIZE_IN_BYTES - usedMemorySizeInBytes >= sizeInBytes) {
            return true;
          }
        }
      }
      if (!hasAtLeastOneBlockShrinkable) {
        return false;
      }
    }
  }

  public synchronized void tryExpandAllAndCheckConsistency() {
    allocatedBlocks.forEach(IoTMemoryBlock::expand);

    long blockSum = allocatedBlocks.stream().mapToLong(IoTMemoryBlock::getMemoryUsageInBytes).sum();
    if (blockSum != usedMemorySizeInBytes) {
      LOGGER.warn(
          "tryExpandAllAndCheckConsistency: memory usage is not consistent with allocated blocks,"
              + " usedMemorySizeInBytes is {} but sum of all blocks is {}",
          usedMemorySizeInBytes,
          blockSum);
    }
  }

  public synchronized void release(IoTMemoryBlock block) {
    if (!MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return;
    }

    allocatedBlocks.remove(block);
    usedMemorySizeInBytes -= block.getMemoryUsageInBytes();
    block.markAsReleased();

    this.notifyAll();
  }

  public synchronized boolean release(IoTMemoryBlock block, long sizeInBytes) {
    if (!MEMORY_MANAGEMENT_ENABLED || block == null || block.isReleased()) {
      return false;
    }

    usedMemorySizeInBytes -= sizeInBytes;
    block.setMemoryUsageInBytes(block.getMemoryUsageInBytes() - sizeInBytes);

    this.notifyAll();

    return true;
  }

  // endregion

  public long getUsedMemorySizeInBytes() {
    return usedMemorySizeInBytes;
  }

  public long getTotalMemorySizeInBytes() {
    return TOTAL_MEMORY_SIZE_IN_BYTES;
  }
}
