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
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft Leader 选举类
 */
public class DLedgerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLedgerLeaderElector.class);

    private Random random = new Random();
    private DLedgerConfig dLedgerConfig;
    private final MemberState memberState;
    /**
     * RPC 服务，向其他节点发送心跳，选举请求等
     */
    private DLedgerRpcService dLedgerRpcService;

    //as a server handler
    //record the last leader state
    /**
     * 上次收到心跳包的时间戳
     */
    private volatile long lastLeaderHeartBeatTime = -1;
    /**
     * 上次发送心跳包的时间戳
     */
    private volatile long lastSendHeartBeatTime = -1;
    /**
     * 上次成功收到心跳包的时间戳
     */
    private volatile long lastSuccHeartBeatTime = -1;
    /**
     * 心跳周期
     */
    private int heartBeatTimeIntervalMs = 2000;
    /**
     * 允许最多 N 个周期内没有收到心跳包，Follower 只有超过 N 个周期未收到心跳才会变为 Candidate，进行下一轮选举
     */
    private int maxHeartBeatLeak = 3;
    //as a client
    /**
     * 发送下一个拉票请求的时间戳
     */
    private long nextTimeToRequestVote = -1;
    /**
     * 是否立即开始新的投票轮次。true 则忽略计时器，立即发起新的投票轮次。<br/>
     * 作用是在 Follower 收到 Leader 心跳，并且 Follower 轮次大于 Leader 轮次时，立即发起新的投票请求。
     */
    private volatile boolean needIncreaseTermImmediately = false;
    /**
     * 最小的发送投票间隔时间
     */
    private int minVoteIntervalMs = 300;
    /**
     * 最大的发送投票间隔时间
     */
    private int maxVoteIntervalMs = 1000;

    /**
     * 节点状态变更监听器
     */
    private List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();

    /**
     * 上一次投票结果
     */
    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
    /**
     * 上次投票的开销
     */
    private long lastVoteCost = 0L;

    /**
     * 状态机管理器
     */
    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);

    private final TakeLeadershipTask takeLeadershipTask = new TakeLeadershipTask();

    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;
        refreshIntervals(dLedgerConfig);
    }

    public void startup() {
        // 启动状态管理器
        stateMaintainer.start();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            // 启动角色转换监听器，监听状态机状态变更
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    /**
     * Follower 收到 Leader 心跳请求，响应
     *
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {

        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        if (request.getTerm() < memberState.currTerm()) {
            // Leader 节点轮次小于 Follower，返回 EXPIRED_TERM，告诉对方轮次已经过期，需要重新选举
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.currTerm()) {
            // 轮次相同，判断请求的节点是否是当前的 Leader
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                // 如果请求的节点是当前的 Leader，更新上次收到 Leader 心跳的时间戳，返回成功
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        // 处理异常情况，通常情况下上面已经返回。需要加锁以保证线程安全
        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            if (request.getTerm() < memberState.currTerm()) {
                // Leader 节点轮次小于 Follower，返回 EXPIRED_TERM，告诉对方轮次已经过期，需要重新选举
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) {
                // 轮次相同，判断请求的节点是否是当前的 Leader
                if (memberState.getLeaderId() == null) {
                    // 如果当前节点还没有 Leader，切换角色成为 Follower，设置 Leader 节点 Id，返回成功
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    // 如果请求的节点是当前的 Leader，更新上次收到 Leader 心跳的时间戳，返回成功
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    // 如果请求的节点不是当前的 Leader，返回 INCONSISTENT_LEADER，告诉对方 Leader 节点不一致，对方会切换成 Candidate，重新选举
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else {
                // 如果请求的轮次大于当前轮次，认为本节点未准备好，进入 Candidate 状态，立即发起一轮投票，返回 TERM_NOT_READY
                //To make it simple, for larger term, do not change to follower immediately
                //first change to candidate, and notify the state-maintainer thread
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
        }
    }

    /**
     * 切换角色成为 Leader，Candidate 状态调用
     * @param term
     */
    public void changeRoleToLeader(long term) {
        synchronized (memberState) {
            if (memberState.currTerm() == term) {
                // 修改状态机角色
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;
                // 触发状态转换事件，执行扩展点逻辑代码
                handleRoleChange(term, MemberState.Role.LEADER);
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    /**
     * 处理收到的拉票请求
     *
     * @param request 拉票请求
     * @param self 是否自身发送的投票请求
     * @return
     */
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        // 由于一个节点可能同时收到多个节点的拉票请求，所以需要加锁。锁定状态机对象。
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) {
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }

            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEDGER_TERM));
            } else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.currTerm()) {
                // 拉票请求的 term 小于当前 term，拒绝投票
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.currVoteFor() == null) {
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //repeat just let it go
                } else {
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_HAS_LEADER));
                    } else {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                //stepped down by larger term
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }

            if (request.getTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEDGER));
            }

            if (!self && isTakingLeadership() && request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && memberState.getLedgerEndIndex() >= request.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TAKING_LEADERSHIP));
            }

            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }

    /**
     * Leader 节点向 Follower 节点发送心跳包
     *
     * @param term
     * @param leaderId
     * @throws Exception
     */
    private void sendHeartbeats(long term, String leaderId) throws Exception {
        // 集群内节点个数
        final AtomicInteger allNum = new AtomicInteger(1);
        // 收到成功响应的节点个数
        final AtomicInteger succNum = new AtomicInteger(1);
        // 收到对端未准备好的响应的节点个数
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        // 当前集群节点维护的最大投票轮次
        final AtomicLong maxTerm = new AtomicLong(-1);
        // 是否存在 Leader 节点不一致
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        // latch，用于等待异步请求结果
        final CountDownLatch beatLatch = new CountDownLatch(1);
        // 本次心跳包开始发送的时间戳
        long startHeartbeatTimeMs = System.currentTimeMillis();
        // 遍历集群中所有节点，异步发送心跳
        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            // 构造心跳包
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            // 异步发送心跳
            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                // 心跳响应回调，统计响应结果
                try {
                    if (ex != null) {
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                        throw ex;
                    }
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        // 成功响应
                        case SUCCESS:
                            succNum.incrementAndGet();
                            break;
                        // Leader 节点投票轮次小于 Follower 节点
                        case EXPIRED_TERM:
                            maxTerm.set(x.getTerm());
                            break;
                        // Follower 节点已经有新的 Leader
                        case INCONSISTENT_LEADER:
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        // Follower 未准备好
                        case TERM_NOT_READY:
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }

                    if (x.getCode() == DLedgerResponseCode.NETWORK_ERROR.getCode())
                        memberState.getPeersLiveTable().put(id, Boolean.FALSE);
                    else
                        memberState.getPeersLiveTable().put(id, Boolean.TRUE);

                    // 成功响应的 Follower 节点超过半数，唤醒心跳线程
                    if (memberState.isQuorum(succNum.get())
                        || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("heartbeat response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    // 所有 Follower 都响应（但成功的没有超过半数），也唤醒心跳线程
                    if (allNum.get() == memberState.peerSize()) {
                        beatLatch.countDown();
                    }
                }
            });
        }
        // 等待心跳结果，超过半数成功则继续
        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        // 仲裁心跳响应结果
        if (memberState.isQuorum(succNum.get())) {
            // 半数以上 Follower 节点成功响应，更新上次成功心跳时间戳
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else {
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                // （成功 + 未准备）超过半数，立即发送下一次心跳
                lastSendHeartBeatTime = -1;
            } else if (maxTerm.get() > term) {
                // Follower 投票轮次比 Leader 大，Leader 切换成 Candidate 并使用从节点的轮次
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) {
                // Follower 有其他 Leader，该 Leader 节点切换成 Candidate
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                // 超过 maxHeartBeatLeak 个心跳周期（6s）没有收到心跳包，切换成 Candidate
                changeRoleToCandidate(term);
            }
        }
    }

    private void maintainAsLeader() throws Exception {
        // 距离上次发送心跳超过 1 个心跳周期（2s），发送心跳请求
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();
                // 记录本次发送心跳时间戳
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            // 向所有从节点发送心跳
            sendHeartbeats(term, leaderId);
        }
    }

    private void maintainAsFollower() {
        // 距离上次收到心跳是否超过 2 个心跳周期。在不加锁的情况下判断，减少加锁次数
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                // 超过 maxHeartBeatLeak(3) 个心跳周期没有收到心跳包，则切换成 Candidate
                if (memberState.isFollower() && (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    /**
     * 向其他节点发起投票请求（拉票），等待各个节点响应
     *
     * @param term 当前轮次
     * @param ledgerEndTerm 最大投票轮次
     * @param ledgerEndIndex 最大日志条目 index
     * @return
     * @throws Exception
     */
    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long ledgerEndTerm,
        long ledgerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        // 遍历集群内所有节点
        for (String id : memberState.getPeerMap().keySet()) {
            // 构建投票请求
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                // 投票给自己
                voteResponse = handleVote(voteRequest, true);
            } else {
                // 发送投票请求到其他节点
                //async
                voteResponse = dLedgerRpcService.vote(voteRequest);
            }
            responses.add(voteResponse);

        }
        return responses;
    }

    private boolean isTakingLeadership() {
        if (dLedgerConfig.getPreferredLeaderIds() != null && memberState.getTermToTakeLeadership() == memberState.currTerm()) {
            List<String> preferredLeaderIds = Arrays.asList(dLedgerConfig.getPreferredLeaderIds().split(";"));
            return preferredLeaderIds.contains(memberState.getSelfId());
        }
        return false;
    }

    /**
     * 设置下一次拉票时间
     * @return
     */
    private long getNextTimeToRequestVote() {
        if (isTakingLeadership()) {
            return System.currentTimeMillis() + dLedgerConfig.getMinTakeLeadershipVoteIntervalMs() +
                random.nextInt(dLedgerConfig.getMaxTakeLeadershipVoteIntervalMs() - dLedgerConfig.getMinTakeLeadershipVoteIntervalMs());
        }
        // 300 + [0, 700) ms
        return System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    /**
     * 节点状态为 Candidate 时执行
     * @throws Exception
     */
    private void maintainAsCandidate() throws Exception {
        //for candidate
        // 未到下一拉票轮次，直接返回
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        // 投票轮次
        long term;
        // Leader 当前的投票轮次
        long ledgerEndTerm;
        // 当前 Log 最大 index，即下一条 Log 的起始 index
        long ledgerEndIndex;
        synchronized (memberState) {
            if (!memberState.isCandidate()) {
                return;
            }
            // 如果上一次投票结果是等待下一轮投票，或应立即发起投票
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                // 获取新投票轮次，准备发起新一轮投票
                long prevTerm = memberState.currTerm();
                term = memberState.nextTerm();
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {
                // 维持当前投票轮次
                term = memberState.currTerm();
            }
            ledgerEndIndex = memberState.getLedgerEndIndex();
            ledgerEndTerm = memberState.getLedgerEndTerm();
        }
        // 如果需要立即发起拉票
        if (needIncreaseTermImmediately) {
            // 设置下一次拉票时间
            nextTimeToRequestVote = getNextTimeToRequestVote();
            // 设置该标志位为 false
            needIncreaseTermImmediately = false;
            return;
        }

        // 向其他节点发起拉票请求
        long startVoteTimeMs = System.currentTimeMillis();
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);
        // 已知的最大投票轮次
        final AtomicLong knownMaxTermInGroup = new AtomicLong(term);
        // 所有投票数
        final AtomicInteger allNum = new AtomicInteger(0);
        // 有效投票数
        final AtomicInteger validNum = new AtomicInteger(0);
        // 赞成投票数
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        // 未准备好投票。如果对端节点的投票轮次小于发起投票的轮次，则认为我对端未准备好，对端节点会用本节点的轮次进入 Candidate 状态
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        // 发起投票节点的投票轮次小于对端节点的个数
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0);
        // 是否已经存在 Leader
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);

        // 等待所有响应结果
        CountDownLatch voteLatch = new CountDownLatch(1);
        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        // 有效票 + 1
                        validNum.incrementAndGet();
                    }
                    // 统计投票结果
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                // 赞成票 + 1
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED:
                                // 拒绝，已经投给其他节点
                            case REJECT_TAKING_LEADERSHIP:
                                break;
                            case REJECT_ALREADY_HAS_LEADER:
                                // 拒绝，已经存在 Leader 节点
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                            case REJECT_EXPIRED_VOTE_TERM:
                                // 拒绝，当前投票轮次小于 Leader，更新自身投票轮次
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEDGER_TERM:
                                // 拒绝，当前 ledgerTerm 小于对端 ledgerTerm
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                // 拒绝，当前 ledgerEndIndex 小于对端 ledgerEndIndex
                                biggerLedgerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                // 拒绝，对端投票轮次小于当前轮次，即对端未准备好。此时对端节点使用当前投票轮次进入 Candidate 状态
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;

                        }
                    }
                    // 已经有 Leader / 收到过半赞成，结束等待
                    if (alreadyHasLeader.get()
                        || memberState.isQuorum(acceptedNum.get())
                        || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("vote response failed", t);
                } finally {
                    // 如果所有对端节点都返回响应，结束等待
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            });

        }

        // 同步等待所有节点响应
        try {
            voteLatch.await(2000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }

        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);
        VoteResponse.ParseResult parseResult;
        // 根据投票结果进行仲裁，驱动状态机
        if (knownMaxTermInGroup.get() > term) {
            // 对端 term 大于自身 term，使用对端 term 进入 Candidate 状态
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
            changeRoleToCandidate(knownMaxTermInGroup.get());
        } else if (alreadyHasLeader.get()) {
            // 已有 Leader，保持 Candidate 状态，待收到 Leader 心跳请求后变为 Follower
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            // 重置下次拉票计时器，添加心跳最大超时时间。表示：等待在指定时间内收到 Leader 的心跳，降为 Follower。
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs * maxHeartBeatLeak;
        } else if (!memberState.isQuorum(validNum.get())) {
            // 有效票未超过半数，重置下次拉票计时器，等待重新投票。不增加投票轮次
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (!memberState.isQuorum(validNum.get() - biggerLedgerNum.get())) {
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote() + maxVoteIntervalMs;
        } else if (memberState.isQuorum(acceptedNum.get())) {
            // 有效票超过半数，成为 Leader 节点
            parseResult = VoteResponse.ParseResult.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
            // 有效票 + 未准备好投票 超过半数，立即重新拉票
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;
        } else {
            // 赞成票 + 对端 ledgerEndIndex 超过半数，重置计时器，继续本轮选举
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        lastParseResult = parseResult;
        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
            memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);

        if (parseResult == VoteResponse.ParseResult.PASSED) {
        // 有效票超过半数的情况，切换成 Leader
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            changeRoleToLeader(term);
        }
    }

    /**
     * The core method of maintainer. Run the specified logic according to the current role: candidate => propose a
     * vote. leader => send heartbeats to followers, and step down to candidate when quorum followers do not respond.
     * follower => accept heartbeats, and change to candidate when no heartbeat from leader.
     *
     * @throws Exception
     */
    private void maintainState() throws Exception {
        // 按状态执行方法，
        if (memberState.isLeader()) {
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            maintainAsFollower();
        } else {
            maintainAsCandidate();
        }
    }

    /**
     * 触发角色状态转换事件，执行扩展点的逻辑代码
     * @param term
     * @param role
     */
    private void handleRoleChange(long term, MemberState.Role role) {
        try {
            takeLeadershipTask.check(term, role);
        } catch (Throwable t) {
            logger.error("takeLeadershipTask.check failed. ter={}, role={}", term, role, t);
        }

        // 执行角色状态转换监听器（扩展点）的逻辑代码
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }

    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
        LeadershipTransferRequest request) throws Exception {
        logger.info("handleLeadershipTransfer: {}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [HandleLeaderTransfer] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            if (!memberState.isLeader()) {
                logger.warn("[BUG] [HandleLeaderTransfer] selfId={} is not leader", request.getLeaderId());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.NOT_LEADER.getCode()));
            }

            if (memberState.getTransferee() != null) {
                logger.warn("[BUG] [HandleLeaderTransfer] transferee={} is already set", memberState.getTransferee());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.LEADER_TRANSFERRING.getCode()));
            }

            memberState.setTransferee(request.getTransfereeId());
        }
        LeadershipTransferRequest takeLeadershipRequest = new LeadershipTransferRequest();
        takeLeadershipRequest.setGroup(memberState.getGroup());
        takeLeadershipRequest.setLeaderId(memberState.getLeaderId());
        takeLeadershipRequest.setLocalId(memberState.getSelfId());
        takeLeadershipRequest.setRemoteId(request.getTransfereeId());
        takeLeadershipRequest.setTerm(request.getTerm());
        takeLeadershipRequest.setTakeLeadershipLedgerIndex(memberState.getLedgerEndIndex());
        takeLeadershipRequest.setTransferId(memberState.getSelfId());
        takeLeadershipRequest.setTransfereeId(request.getTransfereeId());
        if (memberState.currTerm() != request.getTerm()) {
            logger.warn("[HandleLeaderTransfer] term changed, cur={} , request={}", memberState.currTerm(), request.getTerm());
            return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.EXPIRED_TERM.getCode()));
        }

        return dLedgerRpcService.leadershipTransfer(takeLeadershipRequest).thenApply(response -> {
            synchronized (memberState) {
                if (response.getCode() != DLedgerResponseCode.SUCCESS.getCode() ||
                    (memberState.currTerm() == request.getTerm() && memberState.getTransferee() != null)) {
                    logger.warn("leadershipTransfer failed, set transferee to null");
                    memberState.setTransferee(null);
                }
            }
            return response;
        });
    }

    public CompletableFuture<LeadershipTransferResponse> handleTakeLeadership(
        LeadershipTransferRequest request) throws Exception {
        logger.debug("handleTakeLeadership.request={}", request);
        synchronized (memberState) {
            if (memberState.currTerm() != request.getTerm()) {
                logger.warn("[BUG] [handleTakeLeadership] currTerm={} != request.term={}", memberState.currTerm(), request.getTerm());
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.INCONSISTENT_TERM.getCode()));
            }

            long targetTerm = request.getTerm() + 1;
            memberState.setTermToTakeLeadership(targetTerm);
            CompletableFuture<LeadershipTransferResponse> response = new CompletableFuture<>();
            takeLeadershipTask.update(request, response);
            changeRoleToCandidate(targetTerm);
            needIncreaseTermImmediately = true;
            return response;
        }
    }

    private class TakeLeadershipTask {
        private LeadershipTransferRequest request;
        private CompletableFuture<LeadershipTransferResponse> responseFuture;

        public synchronized void update(LeadershipTransferRequest request,
            CompletableFuture<LeadershipTransferResponse> responseFuture) {
            this.request = request;
            this.responseFuture = responseFuture;
        }

        public synchronized void check(long term, MemberState.Role role) {
            logger.trace("TakeLeadershipTask called, term={}, role={}", term, role);
            if (memberState.getTermToTakeLeadership() == -1 || responseFuture == null) {
                return;
            }
            LeadershipTransferResponse response = null;
            if (term > memberState.getTermToTakeLeadership()) {
                response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.EXPIRED_TERM.getCode());
            } else if (term == memberState.getTermToTakeLeadership()) {
                switch (role) {
                    case LEADER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.SUCCESS.getCode());
                        break;
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    default:
                        return;
                }
            } else {
                switch (role) {
                    /*
                     * The node may receive heartbeat before term increase as a candidate,
                     * then it will be follower and term < TermToTakeLeadership
                     */
                    case FOLLOWER:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED.getCode());
                        break;
                    default:
                        response = new LeadershipTransferResponse().term(term).code(DLedgerResponseCode.INTERNAL_ERROR.getCode());
                }
            }

            responseFuture.complete(response);
            logger.info("TakeLeadershipTask finished. request={}, response={}, term={}, role={}", request, response, term, role);
            memberState.setTermToTakeLeadership(-1);
            responseFuture = null;
            request = null;
        }
    }

    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    /**
     * 节点状态管理器，维护节点状态
     */
    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
            try {
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) {
                    // 更新心跳相关配置
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);
                    // 驱动状态机，做状态转移
                    DLedgerLeaderElector.this.maintainState();
                }
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }
}
