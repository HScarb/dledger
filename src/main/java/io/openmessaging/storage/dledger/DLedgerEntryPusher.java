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

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;

/**
 * DLedger 条目推送器，负责在 Leader 节点上将条目推送给 Follower
 */
public class DLedgerEntryPusher {

    private static Logger logger = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    private DLedgerConfig dLedgerConfig;
    private DLedgerStore dLedgerStore;

    private final MemberState memberState;

    /**
     * RPC 实现类，用于集群内网络通信
     */
    private DLedgerRpcService dLedgerRpcService;

    /**
     * 每个投票轮次中，复制组每个节点当前已存储的最大日志序号（水位）
     * 用于判断已提交条目序号，仲裁一个条目是否已被超过半数节点存储
     */
    private Map<Long /* 投票轮次 */, ConcurrentMap<String /* 节点编号 */, Long /* 日志序号 */>> peerWaterMarksByTerm = new ConcurrentHashMap<>();
    /**
     * 每一个投票轮次中，等待响应挂起的 Append 请求
     */
    private Map<Long /* 投票轮次 */, ConcurrentMap<Long /* 日志序号 */, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();

    /**
     * 条目接收处理线程，仅在 Follower 激活
     */
    private EntryHandler entryHandler;

    /**
     * 追加条目 ACK 仲裁线程，仅在 Leader 激活
     */
    private QuorumAckChecker quorumAckChecker;

    /**
     * 数据复制线程，在 Leader 节点会为每一个 Follower 节点创建一个 {@link EntryDispatcher}
     */
    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

    private Optional<StateMachineCaller> fsmCaller;

    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;
        // 为每个 Follower 节点创建一个 EntryDispatcher 日志复制线程
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, logger));
            }
        }
        this.entryHandler = new EntryHandler(logger);
        this.quorumAckChecker = new QuorumAckChecker(logger);
        this.fsmCaller = Optional.empty();
    }

    public void startup() {
        entryHandler.start();
        quorumAckChecker.start();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public void registerStateMachine(final Optional<StateMachineCaller> fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    /**
     * Follower 收到 Leader 的 push 请求处理入口
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            logger.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    /**
     * 检查当前投票轮次是否已经在挂起请求表中存在，不存在则新建这个投票轮次的挂起请求 Map
     *
     * @param term 投票轮次
     * @param env 调用该方法的函数
     */
    private void checkTermForPendingMap(long term, String env) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            logger.info("Initialize the pending append map in {} for term={}", env, term);
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    /**
     * Leader 更新 Peer 水位线
     *
     * @param term
     * @param peerId
     * @param index
     */
    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "updatePeerWaterMark");
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    public long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    /**
     * 判断 Push 等待队列是否已满，单个投票轮次的挂起的 Append 请求数超过 1w 则返回满。
     *
     * @param currTerm 投票轮次
     * @return 等待队列是否已满
     */
    public boolean isPendingFull(long currTerm) {
        // 检查当前投票轮次的挂起请求 Key 在 Map 中是否存在
        checkTermForPendingMap(currTerm, "isPendingFull");
        // 如果当前投票轮次挂起的请求数量大于 10000，返回满
        return pendingAppendResponsesByTerm.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    /**
     * 把写入的条目 Push 到所有 Follower，Leader 等待 Follower 节点 ACK
     *
     * @param entry 追加的条目
     * @param isBatchWait
     * @return
     */
    public CompletableFuture<AppendEntryResponse> waitAck(DLedgerEntry entry, boolean isBatchWait) {
        // Leader 节点更新自身水位线
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        if (memberState.getPeerMap().size() == 1) {
            // 如果复制组内只有一个节点，直接返回成功
            AppendEntryResponse response = new AppendEntryResponse();
            response.setGroup(memberState.getGroup());
            response.setLeaderId(memberState.getSelfId());
            response.setIndex(entry.getIndex());
            response.setTerm(entry.getTerm());
            response.setPos(entry.getPos());
            if (isBatchWait) {
                return BatchAppendFuture.newCompletedFuture(entry.getPos(), response);
            }
            return AppendFuture.newCompletedFuture(entry.getPos(), response);
        } else {
            // Leader 等待 Follower 复制完数据，返回给客户端一个 Future
            checkTermForPendingMap(entry.getTerm(), "waitAck");
            AppendFuture<AppendEntryResponse> future;
            if (isBatchWait) {
                future = new BatchAppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            } else {
                future = new AppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
            }
            future.setPos(entry.getPos());
            // 将 Future 放入 pendingAppendResponsesByTerm 中，等待 Follower 复制完数据后，填充结果
            CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
            if (old != null) {
                logger.warn("[MONITOR] get old wait at index={}", entry.getIndex());
            }
            return future;
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     *
     * Complete the TimeoutFuture in pendingAppendResponsesByTerm (CurrentTerm, index).
     * Called by statemachineCaller when a committed entry (CurrentTerm, index) was applying to statemachine done.
     *
     * @return true if complete success
     */
    public boolean completeResponseFuture(final long index) {
        final long term = this.memberState.currTerm();
        final Map<Long, TimeoutFuture<AppendEntryResponse>> responses = this.pendingAppendResponsesByTerm.get(term);
        if (responses != null) {
            CompletableFuture<AppendEntryResponse> future = responses.remove(index);
            if (future != null && !future.isDone()) {
                logger.info("Complete future, term {}, index {}", term, index);
                AppendEntryResponse response = new AppendEntryResponse();
                response.setGroup(this.memberState.getGroup());
                response.setTerm(term);
                response.setIndex(index);
                response.setLeaderId(this.memberState.getSelfId());
                response.setPos(((AppendFuture) future).getPos());
                future.complete(response);
                return true;
            }
        }
        return false;
    }

    /**
     * Check responseFutures timeout from {beginIndex} in currentTerm
     */
    public void checkResponseFuturesTimeout(final long beginIndex) {
        final long term = this.memberState.currTerm();
        final Map<Long, TimeoutFuture<AppendEntryResponse>> responses = this.pendingAppendResponsesByTerm.get(term);
        if (responses != null) {
            for (long i = beginIndex; i < Integer.MAX_VALUE; i++) {
                TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                if (future == null) {
                    break;
                } else if (future.isTimeOut()) {
                    AppendEntryResponse response = new AppendEntryResponse();
                    response.setGroup(memberState.getGroup());
                    response.setCode(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                    response.setTerm(term);
                    response.setIndex(i);
                    response.setLeaderId(memberState.getSelfId());
                    future.complete(response);
                } else {
                    break;
                }
            }
        }
    }

    /**
     * 检查挂起的日志追加请求是否泄漏
     * Check responseFutures elapsed before {endIndex} in currentTerm
     */
    private void checkResponseFuturesElapsed(final long endIndex) {
        final long currTerm = this.memberState.currTerm();
        final Map<Long, TimeoutFuture<AppendEntryResponse>> responses = this.pendingAppendResponsesByTerm.get(currTerm);
        // 遍历挂起的请求
        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : responses.entrySet()) {
            // 如果日志序号小于 quorumIndex，说明该日志已经被提交，向客户端返回成功，然后将请求移出挂起队列
            if (futureEntry.getKey() < endIndex) {
                AppendEntryResponse response = new AppendEntryResponse();
                response.setGroup(memberState.getGroup());
                response.setTerm(currTerm);
                response.setIndex(futureEntry.getKey());
                response.setLeaderId(memberState.getSelfId());
                response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                futureEntry.getValue().complete(response);
                responses.remove(futureEntry.getKey());
            }
        }
    }

    /**
     * 将 Leader 节点的轮次和已提交指针，更新到 Follower 节点
     *
     * @param term 轮次
     * @param committedIndex 已提交指针
     */
    private void updateCommittedIndex(final long term, final long committedIndex) {
        dLedgerStore.updateCommittedIndex(term, committedIndex);
        this.fsmCaller.ifPresent(caller -> caller.onCommitted(committedIndex));
    }

    /**
     * 日志追加 ACK 投票仲裁线程，Leader 节点激活
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        /**
         * 上次打印水位线日志的时间戳，用于日志打印
         */
        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        /**
         * 上次检测泄漏的时间戳
         */
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        /**
         * 已投票仲裁的日志序号
         */
        private long lastQuorumIndex = -1;

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker-" + memberState.getSelfId(), logger);
        }

        /**
         * 追加日志仲裁主逻辑循环
         */
        @Override
        public void doWork() {
            try {
                // 打印日志，如果距上次打印时间超过 3s，则输出当前状态日志
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    if (DLedgerEntryPusher.this.fsmCaller.isPresent()) {
                        final long lastAppliedIndex = DLedgerEntryPusher.this.fsmCaller.get().getLastAppliedIndex();
                        logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={} appliedIndex={}",
                            memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm), lastAppliedIndex);
                    } else {
                        logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={}",
                            memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                    }
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                // 不是 Leader 直接返回
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                // 清除过期请求
                if (pendingAppendResponsesByTerm.size() > 1) {
                    for (Long term : pendingAppendResponsesByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        // 清除投票轮次与当前轮次不同的挂起请求，向客户端返回错误码 TERM_CHANGED
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : pendingAppendResponsesByTerm.get(term).entrySet()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setIndex(futureEntry.getKey());
                            response.setCode(DLedgerResponseCode.TERM_CHANGED.getCode());
                            response.setLeaderId(memberState.getLeaderId());
                            logger.info("[TermChange] Will clear the pending response index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().complete(response);
                        }
                        pendingAppendResponsesByTerm.remove(term);
                    }
                }
                // 清除已过期的日志复制水位线，即投票 term 与当前 term 不同的水位线
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        // 清除投票轮次与当前轮次不同的水位线（复制组中每个节点当前存储的最大日志序列号），避免内存泄漏
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        peerWaterMarksByTerm.remove(term);
                    }
                }

                // 追加日志仲裁
                // 获取当前投票轮次已经存储的日志表
                Map<String /* 节点编号 */, Long /*日志序号*/> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                // 按日志序号降序排序
                List<Long> sortedWaterMarks = peerWaterMarks.values()
                    .stream()
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
                // 获取日志表中间的日志序号，即为完成仲裁的日志序号
                long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);
                final Optional<StateMachineCaller> fsmCaller = DLedgerEntryPusher.this.fsmCaller;
                if (fsmCaller.isPresent()) {
                    // If there exist statemachine
                    DLedgerEntryPusher.this.dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);
                    final StateMachineCaller caller = fsmCaller.get();
                    caller.onCommitted(quorumIndex);

                    // Check elapsed
                    if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                        updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                        checkResponseFuturesElapsed(caller.getLastAppliedIndex());
                        lastCheckLeakTimeMs = System.currentTimeMillis();
                    }

                    if (quorumIndex == this.lastQuorumIndex) {
                        waitForRunning(1);
                    }
                } else {
                    // 更新 committedIndex 索引，方便 DLedgerStore 定时将 committedIndex 写入 checkPoint
                    dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);
                    // 处理 quorumIndex（已提交指针）之前的挂起的客户端追加日志请求，返回成功
                    ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                    boolean needCheck = false;
                    int ackNum = 0;
                    // 从 quorumIndex 开始倒序遍历
                    for (Long i = quorumIndex; i > lastQuorumIndex; i--) {
                        try {
                            // 移除对应的挂起请求
                            CompletableFuture<AppendEntryResponse> future = responses.remove(i);
                            if (future == null) {
                                // 如果未找到对应挂起的请求，说明前面挂起的请求已经全部处理完毕，结束遍历。
                                // 标记需要进行泄漏检测
                                needCheck = true;
                                break;
                            } else if (!future.isDone()) {
                                // 找到对应的挂起请求，向客户端返回写入成功
                                AppendEntryResponse response = new AppendEntryResponse();
                                response.setGroup(memberState.getGroup());
                                response.setTerm(currTerm);
                                response.setIndex(i);
                                response.setLeaderId(memberState.getSelfId());
                                response.setPos(((AppendFuture) future).getPos());
                                // 向客户端返回响应结果
                                future.complete(response);
                            }
                            // ackNum + 1，表示本次仲裁向客户端返回响应结果的数量
                            ackNum++;
                        } catch (Throwable t) {
                            logger.error("Error in ack to index={} term={}", i, currTerm, t);
                        }
                    }

                    // 如果本次仲裁没有日志被成功追加，检查被挂起的追加请求
                    // 判断其大于 quorumIndex 的日志序号是否超时，如果超时，向客户端返回 WAIT_QUORUM_ACK_TIMEOUT
                    if (ackNum == 0) {
                        checkResponseFuturesTimeout(quorumIndex + 1);
                        waitForRunning(1);
                    }

                    if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                        // 检查挂起的日志追加请求是否泄漏
                        updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                        checkResponseFuturesElapsed(quorumIndex);
                        lastCheckLeakTimeMs = System.currentTimeMillis();
                    }
                }
                lastQuorumIndex = quorumIndex;
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }

    /**
     * 数据转发线程，负责将 Leader 节点的条目推送给 Follower 节点
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     *   APPEND : append the entries to the follower
     *   COMPARE : if the leader changes, the new leader should compare its entries to follower's
     *   TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     *   COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     *           the leader will send a pure request to inform the follower of committed index.
     *
     *   The common transferring between these types are as following:
     *
     *   COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     *   ^                             |
     *   |---<-----<------<-------<----|
     *
     */
    private class EntryDispatcher extends ShutdownAbleThread {

        /**
         * 向 Follower 发送命令的类型
         */
        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);
        /**
         * 上一次发送 commit 请求的时间戳
         */
        private long lastPushCommitTimeMs = -1;
        /**
         * 目标节点 ID
         */
        private String peerId;
        /**
         * Leader 已完成 COMPARE 的日志序号
         */
        private long compareIndex = -1;
        /**
         * 已向 Follower 发送 APPEND 请求的日志序号（异步操作，不保证 Follower 响应）
         */
        private long writeIndex = -1;
        /**
         * 允许的最大挂起条目数量
         */
        private int maxPendingSize = 1000;
        /**
         * Leader 节点当前投票轮次
         */
        private long term = -1;
        /**
         * Leader 节点 ID
         */
        private String leaderId = null;
        /**
         * 上次检测泄露的时间，即挂起的日志数量超过 maxPendingSize 的时间
         */
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        /**
         * 挂起的 APPEND 请求列表（Leader 将日志转发到 Follower 的请求）
         */
        private ConcurrentMap<Long /* 日志序号 */, Long /* 挂起的时间戳 */> pendingMap = new ConcurrentHashMap<>();
        private ConcurrentMap<Long, Pair<Long, Integer>> batchPendingMap = new ConcurrentHashMap<>();
        private PushEntryRequest batchAppendEntryRequest = new PushEntryRequest();
        /**
         * 每秒转发到 Follower 的日志带宽配额，默认 20M
         */
        private Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        /**
         * 检查节点是否为 Leader，并更新日志转发器的状态
         * @return
         */
        private boolean checkAndFreshState() {
            // 当前节点不是 Leader，直接返回 false，暂停 EntryDispatcher 的复制执行
            if (!memberState.isLeader()) {
                return false;
            }
            // 如果集群触发了重新选举，当前节点刚被选举成 Leader，
            // 日志转发器投票轮次与状态机投票轮次不相等，或 LeaderId 为空，或 LeaderId 与状态机 LeaderId 不相等
            // 将 EntryDispatcher 的 term、leaderId 与 MemberState 同步，然后发送 COMPARE 请求
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return false;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    // 更新投票轮次和 LeaderId 为状态机的
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    // 改变日志转发器的状态为 COMPARE
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return true;
        }

        private PushEntryRequest buildPushRequest(DLedgerEntry entry, PushEntryRequest.Type target) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setTerm(term);
            request.setEntry(entry);
            request.setType(target);
            request.setCommitIndex(dLedgerStore.getCommittedIndex());
            return request;
        }

        private void resetBatchAppendEntryRequest() {
            batchAppendEntryRequest.setGroup(memberState.getGroup());
            batchAppendEntryRequest.setRemoteId(peerId);
            batchAppendEntryRequest.setLeaderId(leaderId);
            batchAppendEntryRequest.setTerm(term);
            batchAppendEntryRequest.setType(PushEntryRequest.Type.APPEND);
            batchAppendEntryRequest.clear();
        }

        /**
         * 检查是否超出配额，超出会触发流控
         * 1. 挂起的 APPEND 请求数，阈值为 1000
         * 2. 主从同步差异，阈值为 300 MB
         * 3. 每秒追加日志速率，阈值为 20 MB
         *
         * @param entry
         */
        private void checkQuotaAndWait(DLedgerEntry entry) {
            // 检查挂起的 APPEND 请求数是否超过阈值，未超过则返回
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            // 内存存储，直接返回
            if (dLedgerStore instanceof DLedgerMemoryStore) {
                return;
            }
            // MMAP 存储，检查主从同步差异，未超过则返回
            DLedgerMmapFileStore mmapFileStore = (DLedgerMmapFileStore) dLedgerStore;
            if (mmapFileStore.getDataFileList().getMaxWrotePosition() - entry.getPos() < dLedgerConfig.getPeerPushThrottlePoint()) {
                return;
            }
            // 统计当前秒的速率，累加
            quota.sample(entry.getSize());
            // 如果触发流控，等待，直到当前这一秒结束
            if (quota.validateNow()) {
                long leftNow = quota.leftNow();
                logger.warn("[Push-{}]Quota exhaust, will sleep {}ms", peerId, leftNow);
                DLedgerUtils.sleep(leftNow);
            }
        }

        /**
         * APPEND 日志实现，Leader 将日志转发到 Follower
         * @param index 日志序号
         * @throws Exception
         */
        private void doAppendInner(long index) throws Exception {
            // 根据日志序号查询日志内容
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
            }
            // 检查是否超出配额，如果超出则流控，sleep 一段时间
            checkQuotaAndWait(entry);
            // 构造 APPEND 请求
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
            // 异步发送 APPEND 请求
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
            // 保存发送 APPEND 请求，Key：APPEND 的日志序号，Value：APPEND 的时间戳
            pendingMap.put(index, System.currentTimeMillis());
            // APPEND 成功回调
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            // 移除 APPEND 请求等待列表中的日志条目
                            pendingMap.remove(x.getIndex());
                            // 更新 Peer（对端节点）水位线（追加成功的日志序号）
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            // 唤醒 ACK 仲裁线程，用于仲裁 APPEND 结果
                            quorumAckChecker.wakeup();
                            break;
                        case INCONSISTENT_STATE:
                            // APPEND 请求状态不一致，Leader 将发送 COMPARE 请求，对比数据是否一致
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }

        /**
         * 根据日志序号查询内容
         *
         * @param index 日志序号
         * @return 日志条目
         */
        private DLedgerEntry getDLedgerEntryForAppend(long index) {
            DLedgerEntry entry;
            try {
                entry = dLedgerStore.get(index);
            } catch (DLedgerException e) {
                //  Do compare, in case the ledgerBeginIndex get refreshed.
                if (DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN.equals(e.getCode())) {
                    logger.info("[Push-{}]Get INDEX_LESS_THAN_LOCAL_BEGIN when requested index is {}, try to compare", peerId, index);
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                    return null;
                }
                throw e;
            }
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            return entry;
        }

        private void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                //Ignore the results
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        /**
         * APPEND 请求重推
         * 判断最新一个 APPEND 请求是否已经超时，如果超时则重新发送请求
         *
         * @throws Exception
         */
        private void doCheckAppendResponse() throws Exception {
            // 获取 Follower 节点水位（已复制日志序号）
            long peerWaterMark = getPeerWaterMark(term, peerId);
            // 尝试查找最老的（已复制序号 + 1）等待的 APPEND 请求
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            // 如果下一个等待的 APPEND 请求已超时（1s），重试推送
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLedgerConfig.getMaxPushTimeOutMs()) {
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1);
            }
        }

        /**
         * 追加日志条目到 Follower
         *
         * @throws Exception
         */
        private void doAppend() throws Exception {
            // 无限循环
            while (true) {
                // 检查节点状态，确保是 Leader，否则直接跳出
                if (!checkAndFreshState()) {
                    break;
                }
                // 检查日志转发器内部状态，确保为 APPEND，否则直接跳出
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                // 如果准备 APPEND 的日志超过了当前 Leader 的最大日志序号
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    // 单独发送 COMMIT 请求
                    doCommit();
                    // 检查最老的日志 APPEND 的请求是否超时，如果超时，重新发送 APPEND 请求，跳出
                    doCheckAppendResponse();
                    break;
                }
                // 检查挂起的 APPEND 请求数量是否超过阈值（1000）
                if (pendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    // 获取 Follower 节点的水位线（成功 APPEND 的日志序号）
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    // 如果挂起请求日志序号小于水位线，说明已经 APPEND 成功，丢弃该请求
                    for (Long index : pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                    }
                    // 记录最新一次检查的时间戳
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                // 如果挂起的 APPEND 请求数量大于阈值
                if (pendingMap.size() >= maxPendingSize) {
                    // 检查最老的日志 APPEND 的请求是否超时，如果超时，重新发送 APPEND 请求，跳出
                    doCheckAppendResponse();
                    break;
                }
                // 将日志转发到 Follower，异步操作，不等待 Follower 响应
                doAppendInner(writeIndex);
                // 准备 APPEND 下一条日志
                writeIndex++;
            }
        }

        private void sendBatchAppendEntryRequest() throws Exception {
            batchAppendEntryRequest.setCommitIndex(dLedgerStore.getCommittedIndex());
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
            batchPendingMap.put(batchAppendEntryRequest.getFirstEntryIndex(), new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            batchPendingMap.remove(x.getIndex());
                            if (x.getCount() == 0) {
                                updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            } else {
                                updatePeerWaterMark(x.getTerm(), peerId, x.getIndex() + x.getCount() - 1);
                            }
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when batch push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
            batchAppendEntryRequest.clear();
        }

        private void doBatchAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
            }
            batchAppendEntryRequest.addEntry(entry);
            if (batchAppendEntryRequest.getTotalSize() >= dLedgerConfig.getMaxBatchPushSize()) {
                sendBatchAppendEntryRequest();
            }
        }

        private void doCheckBatchAppendResponse() throws Exception {
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Pair<Long, Integer> pair = batchPendingMap.get(peerWaterMark + 1);
            if (pair != null && System.currentTimeMillis() - pair.getKey() > dLedgerConfig.getMaxPushTimeOutMs()) {
                long firstIndex = peerWaterMark + 1;
                long lastIndex = firstIndex + pair.getValue() - 1;
                logger.warn("[Push-{}]Retry to push entry from {} to {}", peerId, firstIndex, lastIndex);
                batchAppendEntryRequest.clear();
                for (long i = firstIndex; i <= lastIndex; i++) {
                    DLedgerEntry entry = dLedgerStore.get(i);
                    batchAppendEntryRequest.addEntry(entry);
                }
                sendBatchAppendEntryRequest();
            }
        }

        private void doBatchAppend() throws Exception {
            while (true) {
                if (!checkAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    if (batchAppendEntryRequest.getCount() > 0) {
                        sendBatchAppendEntryRequest();
                    }
                    doCommit();
                    doCheckBatchAppendResponse();
                    break;
                }
                if (batchPendingMap.size() >= maxPendingSize || (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000)) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Map.Entry<Long, Pair<Long, Integer>> entry : batchPendingMap.entrySet()) {
                        if (entry.getKey() + entry.getValue().getValue() - 1 <= peerWaterMark) {
                            batchPendingMap.remove(entry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (batchPendingMap.size() >= maxPendingSize) {
                    doCheckBatchAppendResponse();
                    break;
                }
                doBatchAppendInner(writeIndex);
                writeIndex++;
            }
        }

        /**
         * 在发送 COMPARE 请求后，发现 Follower 数据存在差异
         * Leader 向 Follower 发送 TRUNCATE 请求，截断 Follower 的数据
         *
         * @param truncateIndex Follower 需要开始截断的日志序号
         * @throws Exception
         */
        private void doTruncate(long truncateIndex) throws Exception {
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            DLedgerEntry truncateEntry = dLedgerStore.get(truncateIndex);
            PreConditions.check(truncateEntry != null, DLedgerResponseCode.UNKNOWN);
            logger.info("[Push-{}]Will push data to truncate truncateIndex={} pos={}", peerId, truncateIndex, truncateEntry.getPos());
            // 构造 TRUNCATE 请求，发送到 Follower，等待 Follower 响应
            PushEntryRequest truncateRequest = buildPushRequest(truncateEntry, PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            // Follower 清理完多余的数据，Leader 将状态变为 APPEND
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);
        }

        /**
         * 改变日志转发器的状态
         *
         * @param index 已写入日志序号
         * @param target 要修改的目标状态
         */
        private synchronized void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                case APPEND:
                    // 重置 compareIndex 指针
                    compareIndex = -1;
                    // 更新当前节点已追加日志序号为 index
                    updatePeerWaterMark(term, peerId, index);
                    // 唤醒 ACK 仲裁线程，对 APPEND 结果进行仲裁
                    quorumAckChecker.wakeup();
                    // 更新待追加日志序号
                    writeIndex = index + 1;
                    if (dLedgerConfig.isEnableBatchPush()) {
                        resetBatchAppendEntryRequest();
                    }
                    break;
                case COMPARE:
                    // 从 APPEND 切换到 COMPARE
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        // 重置 compareIndex 指针为 -1
                        compareIndex = -1;
                        // 清空挂起的日志转发请求
                        if (dLedgerConfig.isEnableBatchPush()) {
                            batchPendingMap.clear();
                        } else {
                            pendingMap.clear();
                        }
                    }
                    break;
                case TRUNCATE:
                    // 重置 compareIndex 为 -1
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            // 更新日志转发器状态
            type.set(target);
        }

        /**
         * 向 Follower 发送 COMPARE 请求
         * @throws Exception
         */
        private void doCompare() throws Exception {
            // 无限循环
            while (true) {
                // 验证当前状态下是否可以发送 COMPARE 请求，不是 Leader 则跳出循环
                if (!checkAndFreshState()) {
                    break;
                }
                // 请求类型不是 COMPARE 或 TRUNCATE，跳出
                if (type.get() != PushEntryRequest.Type.COMPARE
                    && type.get() != PushEntryRequest.Type.TRUNCATE) {
                    break;
                }
                // compareIndex 和 ledgerEndIndex 都为 -1，表示时新的集群，没有存储任何数据，无需比较主从数据是否一致，跳出
                if (compareIndex == -1 && dLedgerStore.getLedgerEndIndex() == -1) {
                    break;
                }
                // 重置 compareIndex，如果为 -1 或不在有效范围内，重置 compareIndex 为 Leader 当前存储的最大日志序号
                //revise the compareIndex
                if (compareIndex == -1) {
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                    logger.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > dLedgerStore.getLedgerEndIndex() || compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    logger.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex, dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex());
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                }

                // 根据待比较日志序号查询 Leader 日志
                DLedgerEntry entry = dLedgerStore.get(compareIndex);
                PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                // 构造 COMPARE 请求
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);
                // 向 Follower 发起 COMPARE 请求
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
                // 等待获取请求结果，默认超时时间为 3s
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);
                // Follower 需要开始截断的日志序号
                long truncateIndex = -1;

                // 根据 Follower 响应计算 truncateIndex（Follower 需要截断的日志序号，即多余的数据）
                // 如果响应码为 SUCCESS，表示 compareIndex 对应的日志条目在 Follower 上存在
                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    /*
                     * The comparison is successful:
                     * 1.Just change to append state, if the follower's end index is equal the compared index.
                     * 2.Truncate the follower, if the follower has some dirty entries.
                     */
                    if (compareIndex == response.getEndIndex()) {
                        // 如果 Leader 已经完成比较的日志序号与 Follower 存储的最大日志序号相同，无需截断，切换日志转发器状态为 APPEND
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        // 设置 truncateIndex 为 compareIndex，将向从节点发送 TRUNCATE
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < dLedgerStore.getLedgerBeginIndex()
                    || response.getBeginIndex() > dLedgerStore.getLedgerEndIndex()) {
                    /*
                     * Leader 和 Follower 日志不相交
                     * 设置 truncateIndex 为 Leader 目前最小的偏移量。
                     * 这就意味着会删除 Follower 的所有数据，然后从 truncateIndex 开始向从节点重新转发日志
                     * 这种情况通常发生在 Follower 崩溃很长一段时间，而 Leader 删除过期的日志时
                     */
                    /*
                     The follower's entries does not intersect with the leader.
                     This usually happened when the follower has crashed for a long time while the leader has deleted the expired entries.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex < response.getBeginIndex()) {
                    /*
                     * Leader 的 compareIndex 小于 Follower 节点的起始日志序号，从 Leader 最小日志序号开始同步
                     */
                    /*
                     The compared index is smaller than the follower's begin index.
                     This happened rarely, usually means some disk damage.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex > response.getEndIndex()) {
                    /*
                     * Leader 的 compareIndex 大于 Follower 节点的最大日志序号
                     * 将 compareIndex 设置为 Follower 最大日志序号，继续发起 COMPARE 请求
                     */
                    /*
                     The compared index is bigger than the follower's end index.
                     This happened frequently. For the compared index is usually starting from the end index of the leader.
                     */
                    compareIndex = response.getEndIndex();
                } else {
                    /*
                     * compareIndex 大于 Follower 节点的开始日志序号，但小于 最大日志序号
                     * 表示有相交，将 compareIndex 减 1，继续比较，直到找到需要截断的日志序号
                     */
                    /*
                      Compare failed and the compared index is in the range of follower's entries.
                     */
                    compareIndex--;
                }
                // compareIndex 小于 Leader 的最小日志序号，将 truncateIndex 设置为 Leader 的最小日志序号
                /*
                 The compared index is smaller than the leader's begin index, truncate the follower.
                 */
                if (compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /*
                 * 如果 truncateIndex 不等于 -1，则日志转发器状态为 TRUNCATE，然后立即向 Follower 发送 TRUNCATE 请求
                 */
                /*
                 If get value for truncateIndex, do it right now.
                 */
                if (truncateIndex != -1) {
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                    doTruncate(truncateIndex);
                    break;
                }
            }
        }

        /**
         * 线程主循环，不断循环执行该方法
         */
        @Override
        public void doWork() {
            try {
                // 检查节点状态，并更新日志转发器状态
                if (!checkAndFreshState()) {
                    waitForRunning(1);
                    return;
                }

                // 根据日志转发器状态向 Follower 发送 APPEND 或 COMPARE 请求
                if (type.get() == PushEntryRequest.Type.APPEND) {
                    if (dLedgerConfig.isEnableBatchPush()) {
                        doBatchAppend();
                    } else {
                        doAppend();
                    }
                } else {
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("[Push-{}]Error in {} writeIndex={} compareIndex={}", peerId, getName(), writeIndex, compareIndex, t);
                changeState(-1, PushEntryRequest.Type.COMPARE);
                DLedgerUtils.sleep(500);
            }
        }
    }

    /**
     * 数据接收处理线程，节点为 Follower 时激活
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     *
     */
    private class EntryHandler extends ShutdownAbleThread {

        /**
         * 上次检查 Leader 是否有推送消息的时间戳
         */
        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        /**
         * APPEND 请求处理队列
         */
        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        /**
         * COMMIT、COMPARE、TRUNCATE 请求处理队列
         */
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>(100);

        public EntryHandler(Logger logger) {
            super("EntryHandler-" + memberState.getSelfId(), logger);
        }

        /**
         * Follower 收到 Leader 请求处理入口，将请求放入待处理队列
         * {@link #writeRequestMap} 和 {@link #compareOrTruncateRequests}，由 {@link #doWork()} 方法从队列拉取任务进行处理
         *
         * @param request Leader 请求
         * @return
         * @throws Exception
         */
        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            //The timeout should smaller than the remoting layer's request timeout
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                case APPEND:
                    if (request.isBatch()) {
                        PreConditions.check(request.getBatchEntry() != null && request.getCount() > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    } else {
                        PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    }
                    long index = request.getFirstEntryIndex();
                    // 放入 APPEND 请求处理队列
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    // 如果该序号的日志条目已存在，返回 REPEATED_PUSH
                    if (old != null) {
                        logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                        future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                case COMMIT:
                    // 放入处理队列
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                case COMPARE:
                case TRUNCATE:
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    // 将 APPEND 等待队列清空
                    writeRequestMap.clear();
                    // 放入处理队列
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            wakeup();
            return future;
        }

        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getFirstEntryIndex());
                response.setCount(request.getCount());
            }
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        /**
         * 处理 Leader 发来的 APPEND 请求
         *
         * @param writeIndex
         * @param request
         * @param future
         */
        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                // Follower 日志追加
                DLedgerEntry entry = dLedgerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                // 使用 Leader 节点的已提交指针更新 Follower 节点的已提交指针
                updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        /**
         * Follower 处理 Leader 发来的 COMPARE 请求
         * 判断 Leader 传来的日志序号在 Follower 是否存在
         *
         * @param compareIndex Leader 已经完成比较的日志序号
         * @param request LEADER 发来的 COMPARE 请求
         * @param future
         * @return 日志序号存在：SUCCESS，不存在：INCONSISTENT_STATE
         */
        private CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(compareIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                // 从 Follower 中获取日志序号为 compareIndex 的日志条目
                DLedgerEntry local = dLedgerStore.get(compareIndex);
                // 检查日志条目是否存在
                PreConditions.check(request.getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                // 存在，构造返回体返回
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                // 日志条目在 Follower 不存在
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        /**
         * Follower 处理 Leader 发来的 COMMIT 请求
         * @param committedIndex Leader 中已提交的日志序号
         * @param request COMMIT 请求
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                // 将 Leader 节点的轮次和已提交指针，更新到 Follower 节点
                updateCommittedIndex(request.getTerm(), committedIndex);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        /**
         * 处理 Leader 发来的 TRUNCATE 请求
         * 删除节点上 truncateIndex 之后的所有日志
         *
         * @param truncateIndex
         * @param request
         * @param future
         * @return
         */
        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={} pos={}", truncateIndex, request.getEntry().getPos());
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                // 删除节点上 truncateIndex 之后的所有日志
                long index = dLedgerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                // 使用 Leader 节点的已提交指针更新 Follower 节点的已提交指针
                updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private void handleDoBatchAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getFirstEntryIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                for (DLedgerEntry entry : request.getBatchEntry()) {
                    dLedgerStore.appendAsFollower(entry, request.getTerm(), request.getLeaderId());
                }
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoBatchAppend]", t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }

        }

        /**
         * @param endIndex Follower 存储的最大日志序号
         */
        private void checkAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            // 遍历所有挂起的 APPEND 请求
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long firstEntryIndex = pair.getKey().getFirstEntryIndex();
                long lastEntryIndex = pair.getKey().getLastEntryIndex();
                // 待追加日志序号小于等于 Follower 存储的最大日志序号
                //Fall behind
                if (lastEntryIndex <= endIndex) {
                    try {
                        // 如果 Follower 存储的该条日志与 Leader 推送请求中的不一样，返回 INCONSISTENT_STATE
                        // 表示从该日志序号开始，Leader 与 Follower 数据不一致，需要发送 COMPARE 和 TRUNCATE 请求修正数据
                        if (pair.getKey().isBatch()) {
                            for (DLedgerEntry dLedgerEntry : pair.getKey().getBatchEntry()) {
                                PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                            }
                        } else {
                            DLedgerEntry dLedgerEntry = pair.getKey().getEntry();
                            PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                        }
                        // Follower 存储的该条日志内容与 Leader 推送的日志内容相同，说明从节点已经存储该条日志，返回 SUCCESS
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(pair.getKey().getFirstEntryIndex());
                    continue;
                }
                // 待追加日志序号等于 endIndex + 1，表示 Follower 下一条期望追加的日志已经被 Leader 推送过来，是正常情况，直接返回
                if (firstEntryIndex == endIndex + 1) {
                    return;
                }
                // 处理待追加日志序号大于 endIndex + 1 的情况
                // 挂起时间未超时，继续检查下一条待追加日志
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                // 快速失败机制
                // 挂起时间超时，说明该日志没有正常写入 Follower。记录其日志序号，最终向主节点反馈最小的超时日志序号。
                if (firstEntryIndex < minFastForwardIndex) {
                    minFastForwardIndex = firstEntryIndex;
                }
            }
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            // 如果有记录超时的待追加日志序号，向 Leader 返回 INCONSISTENT_STATE，让主节点发送 COMPARE 进行数据比对，保证主从一致性
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }
        /**
         * 异常检测，Follower 从 writeRequestMap 中查找最大日志序号 + 1 的日志序号对应的请求，如果不存在，可能发生推送丢失
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         *   * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         *   * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         * @param endIndex Follower 存储的最大日志序号
         */
        private void checkAbnormalFuture(long endIndex) {
            // 距离上一次异常检查不到 1s，直接返回
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            lastCheckFastForwardTimeMs  = System.currentTimeMillis();
            // 没有积压的 APPEND 请求，Leader 没有推送新日志，直接返回
            if (writeRequestMap.isEmpty()) {
                return;
            }

            checkAppendFuture(endIndex);
        }

        @Override
        public void doWork() {
            try {
                // 不是从节点，跳出
                if (!memberState.isFollower()) {
                    waitForRunning(1);
                    return;
                }
                if (compareOrTruncateRequests.peek() != null) {
                    // 如果 COMMIT、COMPARE、TRUNCATE 请求队列不为空，优先处理
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    // 处理 APPEND 请求
                    // 下一条待写日志序号 = 已存储的最大日志序号 + 1
                    long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                    // 根据序号获取 APPEND 请求
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair == null) {
                        // 在待写队列中没有找到对应的 APPEND 请求，调用 checkAbnormalFuture 检查请求是否丢失
                        checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                        waitForRunning(1);
                        return;
                    }
                    // 能找到 APPEND 请求，处理请求
                    PushEntryRequest request = pair.getKey();
                    if (request.isBatch()) {
                        handleDoBatchAppend(nextIndex, request, pair.getValue());
                    } else {
                        handleDoAppend(nextIndex, request, pair.getValue());
                    }
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.logger.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
