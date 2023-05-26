/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.utils;

/**
 * 主从日志复制流控类
 */
public class Quota {

    /**
     * 每秒最大追加日志速度，默认 20 MB
     */
    private final int max;

    /**
     * 采样值
     */
    private final int[] samples;
    /**
     * 每个采样值对应的时间
     */
    private final long[] timeVec;

    /**
     * 时间窗口大小，每秒一个窗口
     */
    private final int window;

    public Quota(int max) {
        this(5, max);
    }
    public Quota(int window, int max) {
        if (window < 5) {
            window = 5;
        }
        this.max = max;
        this.window = window;
        this.samples = new int[window];
        this.timeVec = new long[window];
    }

    /**
     * 基于当前时间戳，计算在 window 中的索引
     *
     * @param currTimeMs
     * @return
     */
    private int index(long currTimeMs) {
        return  (int) (second(currTimeMs) % window);
    }

    private long second(long currTimeMs) {
        return currTimeMs / 1000;
    }

    /**
     * 统计当前速率
     *
     * @param value 当前速率
     */
    public void sample(int value) {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] != second) {
            timeVec[index] = second;
            samples[index] = value;
        } else {
            samples[index] += value;
        }

    }

    /**
     * 验证当前秒的采样值是否超过阈值
     *
     * @return
     */
    public boolean validateNow() {
        long timeMs = System.currentTimeMillis();
        int index = index(timeMs);
        long second = second(timeMs);
        if (timeVec[index] == second) {
            return samples[index] >= max;
        }
        return false;
    }

    /**
     * 当前秒的采样值如果已经超过阈值，返回当前秒剩下的时间
     *
     * @return 当前秒剩下的毫秒数
     */
    public int leftNow() {
        long timeMs = System.currentTimeMillis();
        return (int) ((second(timeMs) + 1) * 1000 - timeMs);
    }
}
