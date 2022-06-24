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
package org.astraea.app.balancer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.executor.StraightPlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.generator.ShufflePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.NodeInfo;
import org.astraea.app.cost.ReplicaInfo;
import org.astraea.app.cost.broker.CpuCost;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.partitioner.Configuration;

class BalancerUtils {

  public static ClusterInfo mockClusterInfoAllocation(
      ClusterInfo clusterInfo, ClusterLogAllocation allocation) {
    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return clusterInfo.nodes();
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        return clusterInfo.dataDirectories(brokerId);
      }

      @Override
      public Set<String> topics() {
        return clusterInfo.topics();
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return clusterInfo.beans(brokerId);
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return clusterInfo.allBeans();
      }

      @Override
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return replicas(topic).stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        // there is no offline sense for a fake cluster info, so everything is online.
        return replicas(topic);
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
        Map<Integer, NodeInfo> nodeIdMap =
            nodes().stream()
                .collect(Collectors.toUnmodifiableMap(NodeInfo::id, Function.identity()));
        var result =
            allocation
                .topicPartitionStream()
                .filter(tp -> tp.topic().equals(topic))
                .map(tp -> Map.entry(tp, allocation.logPlacements(tp)))
                .flatMap(
                    entry -> {
                      var tp = entry.getKey();
                      var logs = entry.getValue();

                      return IntStream.range(0, logs.size())
                          .mapToObj(
                              i ->
                                  ReplicaInfo.of(
                                      tp.topic(),
                                      tp.partition(),
                                      nodeIdMap.get(logs.get(i).broker()),
                                      i == 0,
                                      true,
                                      false,
                                      logs.get(i).logDirectory().orElse(null)));
                    })
                .collect(Collectors.toUnmodifiableList());

        if (result.isEmpty()) throw new NoSuchElementException();

        return result;
      }
    };
  }

  public static CostFunction constructCostFunction(
      Class<? extends CostFunction> aClass, Configuration configuration) {
    // TODO: make it possible to construct specific object from Config
    // TODO: add test for this after above TODO is done
    if (!aClass.equals(CpuCost.class)) throw new UnsupportedOperationException();
    return new CpuCost();
  }

  public static RebalancePlanGenerator constructGenerator(
      Class<? extends RebalancePlanGenerator> aClass, Configuration configuration) {
    // TODO: make it possible to construct specific object from Config
    // TODO: add test for this after above TODO is done
    if (!aClass.equals(ShufflePlanGenerator.class)) throw new UnsupportedOperationException();
    var shuffleCount =
        configuration
            .string("shuffle.plan.generator.shuffle.count")
            .map(Integer::parseInt)
            .orElse(5);
    return new ShufflePlanGenerator(() -> shuffleCount);
  }

  public static RebalancePlanExecutor constructExecutor(
      Class<? extends RebalancePlanExecutor> aClass, Configuration configuration) {
    // TODO: make it possible to construct specific object from Config
    // TODO: add test for this after above TODO is done
    if (!aClass.equals(StraightPlanExecutor.class)) throw new UnsupportedOperationException();
    return new StraightPlanExecutor();
  }
}
