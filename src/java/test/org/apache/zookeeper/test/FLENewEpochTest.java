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

package org.apache.zookeeper.test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.FastLeaderElection;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.Vote;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.junit.Test;

public class FLENewEpochTest extends TestCase {
    protected static final Logger LOG = Logger.getLogger(FLENewEpochTest.class);

    int count;
    int baseport;
    int baseLEport;
    HashMap<Long,QuorumServer> peers;
    ArrayList<LEThread> threads;
    File tmpdir[];
    int port[];
    int[] round;
    
    @Override
    public void setUp() throws Exception {
        count = 3;
        baseport= 33303;
        baseLEport = 43303;

        peers = new HashMap<Long,QuorumServer>(count);
        threads = new ArrayList<LEThread>(count);
        tmpdir = new File[count];
        port = new int[count];

        round = new int[3];
        round[0] = 0;
        round[1] = 0;
        round[2] = 0;
        LOG.info("SetUp " + getName());
    }

    @Override
    public void tearDown() throws Exception {
        for(int i = 0; i < threads.size(); i++) {
            ((FastLeaderElection) threads.get(i).peer.getElectionAlg()).shutdown();
        }
        LOG.info("FINISHED " + getName());
    }


    class LEThread extends Thread {
        FastLeaderElection le;
        int i;
        QuorumPeer peer;

        LEThread(QuorumPeer peer, int i) {
            this.i = i;
            this.peer = peer;
            LOG.info("Constructor: " + getName());
            
        }

        public void run(){
        	boolean flag = true;
            try{
            	while(flag){
            		Vote v = null;
            		peer.setPeerState(ServerState.LOOKING);
            		LOG.info("Going to call leader election again: " + i);
            		v = peer.getElectionAlg().lookForLeader();

            		if(v == null){
            			fail("Thread " + i + " got a null vote");
            		}

            		/*
            		 * A real zookeeper would take care of setting the current vote. Here
            		 * we do it manually.
            		 */
            		peer.setCurrentVote(v);

            		LOG.info("Finished election: " + i + ", " + v.id);
            		//votes[i] = v;

            		switch(i){
            		case 0:
            			LOG.info("First peer, do nothing, just join");
            			flag = false;
            			break;
            		case 1:
            			LOG.info("Second entering case");
            			if(round[1] != 0) flag = false;
            			else{
            				while(round[2] == 0){
            					Thread.sleep(200);
            				}
            			}
            			LOG.info("Second is going to start second round");
            			round[1]++;
            			break;
            		case 2:
            			LOG.info("Third peer, shutting it down");
            			((FastLeaderElection) peer.getElectionAlg()).shutdown();
            			peer.shutdown();
            			flag = false;
            			round[2] = 1;
            			LOG.info("Third leaving");
            			break;
            		}
            	}
            } catch (Exception e){
            	e.printStackTrace();
            }    
        }
    }


      @Test
      public void testLENewEpoch() throws Exception {

          FastLeaderElection le[] = new FastLeaderElection[count];

          LOG.info("TestLE: " + getName()+ ", " + count);
          for(int i = 0; i < count; i++) {
              peers.put(Long.valueOf(i), new QuorumServer(i, new InetSocketAddress(baseport+100+i),
                      new InetSocketAddress(baseLEport+100+i)));
              tmpdir[i] = File.createTempFile("letest", "test");
              port[i] = baseport+i;
          }

          for(int i = 1; i < le.length; i++) {
              QuorumPeer peer = new QuorumPeer(peers, tmpdir[i], tmpdir[i], port[i], 3, i, 2, 2, 2);
              peer.startLeaderElection();
              LEThread thread = new LEThread(peer, i);
              thread.start();
              threads.add(thread);
          }
          Thread.sleep(2000);
          QuorumPeer peer = new QuorumPeer(peers, tmpdir[0], tmpdir[0], port[0], 3, 0, 2, 2, 2);
          peer.startLeaderElection();
          LEThread thread = new LEThread(peer, 0);
          thread.start();
          threads.add(thread);
          
          LOG.info("Started threads " + getName());

          for(int i = 0; i < threads.size(); i++) {
              threads.get(i).join(10000);
              if (threads.get(i).isAlive()) {
                  fail("Threads didn't join");
              }

          }
      }
  }
