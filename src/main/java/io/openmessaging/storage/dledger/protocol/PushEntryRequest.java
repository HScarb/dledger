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

package io.openmessaging.storage.dledger.protocol;

import java.util.ArrayList;
import java.util.List;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.utils.PreConditions;

public class PushEntryRequest extends RequestOrResponse {
    private long commitIndex = -1;
    private Type type = Type.APPEND;
    private DLedgerEntry entry;

    //for batch append push
    private List<DLedgerEntry> batchEntry = new ArrayList<>();
    private int totalSize;

    public DLedgerEntry getEntry() {
        return entry;
    }

    public void setEntry(DLedgerEntry entry) {
        this.entry = entry;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public void addEntry(DLedgerEntry entry) {
        if (!batchEntry.isEmpty()) {
            PreConditions.check(batchEntry.get(0).getIndex() + batchEntry.size() == entry.getIndex(),
                DLedgerResponseCode.UNKNOWN, "batch push wrong order");
        }
        batchEntry.add(entry);
        totalSize += entry.getSize();
    }

    public long getFirstEntryIndex() {
        if (!batchEntry.isEmpty()) {
            return batchEntry.get(0).getIndex();
        } else if (entry != null) {
            return entry.getIndex();
        } else {
            return -1;
        }
    }

    public long getLastEntryIndex() {
        if (!batchEntry.isEmpty()) {
            return batchEntry.get(batchEntry.size() - 1).getIndex();
        } else if (entry != null) {
            return entry.getIndex();
        } else {
            return -1;
        }
    }

    public int getCount() {
        if (!batchEntry.isEmpty()) {
            return batchEntry.size();
        } else if (entry != null) {
            return 1;
        } else {
            return 0;
        }
    }

    public long getTotalSize() {
        return totalSize;
    }

    public List<DLedgerEntry> getBatchEntry() {
        return batchEntry;
    }

    public void clear() {
        batchEntry.clear();
        totalSize = 0;
    }

    public boolean isBatch() {
        return !batchEntry.isEmpty();
    }

    public enum Type {
        /**
         * 将日志条目追加到 Follower
         */
        APPEND,
        /**
         * 通常 Leader 会将提交的索引附加在 APPEND 请求上，
         * 如果 APPEND 请求少且分散，Leader 将发送一个单独的 COMMIT 请求来通知 Follower 提交索引
         */
        COMMIT,
        /**
         * Leader 变化时，新 Leader 需要与 Follower 日志条目进行比较，截断 Follower 多余的数据，确保主从节点数据一致
         */
        COMPARE,
        /**
         * Leader 通过索引完成比较后，发现 Follower 存在多余的数据（未提交的），需要发送 TRUNCATE 到 Follower，
         * 删除多余的数据（Follower 未提交但在 Leader 上不存在的数据），保证数据一致性
         */
        TRUNCATE
    }
}
