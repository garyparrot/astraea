/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.balancer.executor;

import org.astraea.app.admin.Replica;
import org.astraea.app.admin.TopicPartition;

/** Monitoring the migration progress of specific replica log */
public interface SyncingProgress {

  static SyncingProgress of(TopicPartition topicPartition, Replica leaderReplica, Replica replica) {
    return new SyncingProgress() {
      @Override
      public TopicPartition topicPartition() {
        return topicPartition;
      }

      @Override
      public int brokerId() {
        return replica.broker();
      }

      @Override
      public boolean synced() {
        return replica.inSync();
      }

      @Override
      public double percentage() {
        // attempts to bypass the divided by zero issue
        if (replica.size() == leaderReplica.size()) {
          return 1;
        } else {
          return (double) replica.size() / leaderReplica.size();
        }
      }

      @Override
      public long logSize() {
        return replica.size();
      }

      @Override
      public long leaderLogSize() {
        return leaderReplica.size();
      }
    };
  }

  /** Current tracking target */
  TopicPartition topicPartition();

  /** Broker of the tracking log */
  int brokerId();

  /** Is the target replica log synced */
  boolean synced();

  /** The ratio between current log size and leader log size */
  double percentage();

  /** The size of current migration log */
  long logSize();

  /** The size of leader log */
  long leaderLogSize();
}
