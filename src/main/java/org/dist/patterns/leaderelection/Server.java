package org.dist.patterns.leaderelection;

import org.dist.patterns.heartbeat.HeartBeatScheduler;

public class Server {
    enum ServerState {
        LOOKING,
        FOLLOWING,
        LEADING;
    }

    private volatile boolean heartBeatReceived = false;
    private volatile ServerState state = ServerState.FOLLOWING;

    private HeartBeatScheduler electionTimeoutChecker = new HeartBeatScheduler(()->{
        heartBeatCheck();

    }, 100);

    private void heartBeatCheck() {
        if(!heartBeatReceived) {
            handleHeartBeatTimeout();
        } else {
            heartBeatReceived = false; //reset
        }
    }

    private void handleHeartBeatTimeout() {
        System.out.println("handling election timeout");
        state = ServerState.LOOKING; //equivalent to CANDIDATE in RAFT
    }

    public void startup() {
        electionTimeoutChecker.start();

        while(true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (state == ServerState.LOOKING) {
                leaderElection();
            } else if (state == ServerState.LEADING) {
                setupHeartBeatScheduler();
            }
            //continue in some state.
        }
        //lookforleader
        //voteforself and requestvote
        //from the votes received, set self vote to server with highest log and term and highest number

        //waitinfollowerstate and expect requestvote/heartbeat
        //timeout and request vote
        //sendvote if term > current term, index > self index
        //if no vote, wait in candidate state with varying timeout.


    }

    private void setupHeartBeatScheduler() {
        HeartBeatScheduler electionTimeoutChecker = new HeartBeatScheduler(()->{
            sendHeartBeats();

        }, 100);
    }

    private void sendHeartBeats() {

    }

    int term = 0;
    int logIndex = 0;
    private void leaderElection() {
        term = term + 1;

        RequestVoteResponse selfResponse = new RequestVoteResponse(true, term, logIndex);
        RequestVoteResponse requestVoteResponse1 = requestVote(term, logIndex);
        RequestVoteResponse requestVoteResponse2 = requestVote(term, logIndex);
        if (requestVoteResponse1.success) { //we have majority in the cluster of 3
            state = ServerState.LEADING;
        }
    }

    static class RequestVoteResponse {
        private boolean success;
        private int term;
        private int logIndex;

        public RequestVoteResponse(boolean success, int term, int logIndex) {
            this.success = success;
            this.term = term;
            this.logIndex = logIndex;
        }
    }

    private RequestVoteResponse requestVote(int term, int logIndex) {
        return new RequestVoteResponse(true, term, logIndex);
    }
}
