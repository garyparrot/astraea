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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.ClusterInfoProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BalancerUtilsTest {

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4})
  void testMockClusterInfo(int gotoBroker) {
    // arrange
    var nodeCount = 5;
    var topicCount = 3;
    var partitionCount = 5;
    var replicaCount = 1;
    var logCount = topicCount * partitionCount * replicaCount;
    var dir = "/path/to/somewhere";
    var topicNames =
        IntStream.range(0, topicCount)
            .mapToObj(i -> "Topic_" + i)
            .collect(Collectors.toUnmodifiableSet());
    var clusterInfo =
        ClusterInfoProvider.fakeClusterInfo(
            nodeCount, topicCount, partitionCount, replicaCount, (ignore) -> topicNames);
    var mockedAllocationMap =
        topicNames.stream()
            .flatMap(
                topicName ->
                    IntStream.range(0, partitionCount)
                        .mapToObj(p -> new TopicPartition(topicName, p)))
            .collect(
                Collectors.toUnmodifiableMap(
                    i -> i, i -> List.of(LogPlacement.of(gotoBroker, dir))));
    var mockedAllocation = LayeredClusterLogAllocation.of(mockedAllocationMap);

    // act, declare every log should locate at specific broker -> gotoBroker
    var mockedClusterInfo = BalancerUtils.mockClusterInfoAllocation(clusterInfo, mockedAllocation);

    // assert, expected every log in the mocked ClusterInfo locate at that broker
    Assertions.assertEquals(
        Collections.nCopies(logCount, gotoBroker),
        topicNames.stream()
            .flatMap(name -> mockedClusterInfo.replicas(name).stream())
            .map(replica -> replica.nodeInfo().id())
            .collect(Collectors.toUnmodifiableList()));

    // assert, expected every log in the mocked ClusterInfo locate at that dir
    Assertions.assertEquals(
        Collections.nCopies(logCount, dir),
        topicNames.stream()
            .flatMap(name -> mockedClusterInfo.replicas(name).stream())
            .map(replica -> replica.dataFolder().orElseThrow())
            .collect(Collectors.toUnmodifiableList()));
  }
}
