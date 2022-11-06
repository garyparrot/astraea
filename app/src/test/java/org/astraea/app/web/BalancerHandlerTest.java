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
package org.astraea.app.web;

import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.SingleStepBalancer;
import org.astraea.common.balancer.executor.RebalancePlanExecutor;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.balancer.log.ClusterLogAllocation;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.Configuration;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.MoveCost;
import org.astraea.common.producer.Producer;
import org.astraea.it.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class BalancerHandlerTest extends RequireBrokerCluster {

  private static final String defaultDecreasing =
      new Gson()
          .toJson(
              Collections.singleton(
                  new BalancerHandler.CostWeight(DecreasingCost.class.getName(), 1)));

  @Test
  @Timeout(value = 60)
  void testReport() {
    var topics = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      // make sure all replicas have
      admin
          .clusterInfo(Set.copyOf(topics))
          .toCompletableFuture()
          .join()
          .replicaStream()
          .forEach(r -> Assertions.assertNotEquals(0, r.size()));
      var handler = new BalancerHandler(admin);
      var report =
          submitPlanGeneration(
                  handler,
                  Map.of(
                      BalancerHandler.LOOP_KEY,
                      "3000",
                      BalancerHandler.COST_WEIGHT_KEY,
                      defaultDecreasing))
              .report;
      Assertions.assertNotNull(report.id);
      Assertions.assertEquals(3000, report.limit);
      Assertions.assertNotEquals(0, report.changes.size());
      Assertions.assertTrue(report.cost >= report.newCost);
      // "before" should record size
      report.changes.forEach(
          c ->
              c.before.forEach(
                  p -> {
                    // if the topic is generated by this test, it should have data
                    if (topics.contains(c.topic)) Assertions.assertNotEquals(0, p.size);
                    // otherwise, we just check non-null
                    else Assertions.assertNotNull(p.size);
                  }));
      // "after" should NOT record size
      report.changes.stream()
          .flatMap(c -> c.after.stream())
          .forEach(p -> Assertions.assertNull(p.size));
      Assertions.assertTrue(report.cost >= report.newCost);
      var sizeMigration =
          report.migrationCosts.stream().filter(x -> x.function.equals("size")).findFirst().get();
      Assertions.assertTrue(sizeMigration.totalCost >= 0);
      Assertions.assertTrue(sizeMigration.cost.size() > 0);
      Assertions.assertEquals(0, sizeMigration.cost.stream().mapToLong(x -> x.cost).sum());
    }
  }

  @Test
  @Timeout(value = 60)
  void testTopic() {
    var topicNames = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin);
      var report =
          submitPlanGeneration(
                  handler,
                  Map.of(
                      BalancerHandler.LOOP_KEY,
                      "30",
                      BalancerHandler.TOPICS_KEY,
                      topicNames.get(0),
                      BalancerHandler.COST_WEIGHT_KEY,
                      defaultDecreasing))
              .report;
      var actual =
          report.changes.stream().map(r -> r.topic).collect(Collectors.toUnmodifiableSet());
      Assertions.assertEquals(1, actual.size());
      Assertions.assertEquals(topicNames.get(0), actual.iterator().next());
      Assertions.assertTrue(report.cost >= report.newCost);
      var sizeMigration =
          report.migrationCosts.stream().filter(x -> x.function.equals("size")).findFirst().get();
      Assertions.assertTrue(sizeMigration.totalCost >= 0);
      Assertions.assertTrue(sizeMigration.cost.size() > 0);
      Assertions.assertEquals(0, sizeMigration.cost.stream().mapToLong(x -> x.cost).sum());
    }
  }

  @Test
  @Timeout(value = 60)
  void testTopics() {
    var topicNames = createAndProduceTopic(5);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin);
      // For all 5 topics, we only allow the first two topics can be altered.
      // We apply this limitation to test if the BalancerHandler.TOPICS_KEY works correctly.
      var allowedTopics = topicNames.subList(0, 2);
      var report =
          submitPlanGeneration(
                  handler,
                  Map.of(
                      BalancerHandler.LOOP_KEY,
                      "30",
                      BalancerHandler.TOPICS_KEY,
                      String.join(",", allowedTopics),
                      BalancerHandler.COST_WEIGHT_KEY,
                      defaultDecreasing))
              .report;
      Assertions.assertTrue(
          report.changes.stream().map(x -> x.topic).allMatch(allowedTopics::contains),
          "Only allowed topics been altered");
      Assertions.assertTrue(
          report.cost >= report.newCost,
          "The proposed plan should has better score then the current one");
      var sizeMigration =
          report.migrationCosts.stream().filter(x -> x.function.equals("size")).findFirst().get();
      Assertions.assertTrue(sizeMigration.totalCost >= 0);
      Assertions.assertTrue(sizeMigration.cost.size() > 0);
      Assertions.assertEquals(0, sizeMigration.cost.stream().mapToLong(x -> x.cost).sum());
    }
  }

  private static List<String> createAndProduceTopic(int topicCount) {
    try (var admin = Admin.of(bootstrapServers())) {
      var topics =
          IntStream.range(0, topicCount)
              .mapToObj(ignored -> Utils.randomString(10))
              .collect(Collectors.toUnmodifiableList());
      topics.forEach(
          topic -> {
            admin
                .creator()
                .topic(topic)
                .numberOfPartitions(3)
                .numberOfReplicas((short) 1)
                .run()
                .toCompletableFuture()
                .join();
            Utils.sleep(Duration.ofSeconds(1));
            admin
                .moveToBrokers(
                    admin.topicPartitions(Set.of(topic)).toCompletableFuture().join().stream()
                        .collect(Collectors.toMap(tp -> tp, ignored -> List.of(1))))
                .toCompletableFuture()
                .join();
          });
      Utils.sleep(Duration.ofSeconds(3));
      try (var producer = Producer.of(bootstrapServers())) {
        IntStream.range(0, 30)
            .forEach(
                index ->
                    topics.forEach(
                        topic ->
                            producer
                                .sender()
                                .topic(topic)
                                .key(String.valueOf(index).getBytes(StandardCharsets.UTF_8))
                                .run()));
      }
      return topics;
    }
  }

  @Test
  @Timeout(value = 60)
  void testBestPlan() {
    try (var admin = Admin.of(bootstrapServers())) {
      var currentClusterInfo =
          ClusterInfo.of(
              Set.of(NodeInfo.of(10, "host", 22), NodeInfo.of(11, "host", 22)),
              List.of(
                  Replica.builder()
                      .topic("topic")
                      .partition(0)
                      .nodeInfo(NodeInfo.of(10, "host", 22))
                      .lag(0)
                      .size(100)
                      .isLeader(true)
                      .inSync(true)
                      .isFuture(false)
                      .isOffline(false)
                      .isPreferredLeader(true)
                      .path("/tmp/aa")
                      .build()));

      var clusterLogAllocation =
          ClusterLogAllocation.of(
              ClusterInfo.of(
                  List.of(
                      Replica.builder()
                          .topic("topic")
                          .partition(0)
                          .nodeInfo(NodeInfo.of(11, "host", 22))
                          .lag(0)
                          .size(100)
                          .isLeader(true)
                          .inSync(true)
                          .isFuture(false)
                          .isOffline(false)
                          .isPreferredLeader(true)
                          .path("/tmp/aa")
                          .build())));
      HasClusterCost clusterCostFunction =
          (clusterInfo, clusterBean) -> () -> clusterInfo == currentClusterInfo ? 100D : 10D;
      HasMoveCost moveCostFunction =
          (originClusterInfo, newClusterInfo, clusterBean) ->
              MoveCost.builder().totalCost(100).build();

      var balancerHandler = new BalancerHandler(admin);
      var Best =
          Balancer.create(
                  SingleStepBalancer.class,
                  AlgorithmConfig.builder()
                      .clusterCost(clusterCostFunction)
                      .clusterConstraint((before, after) -> after.value() <= before.value())
                      .moveCost(List.of(moveCostFunction))
                      .movementConstraint(moveCosts -> true)
                      .build())
              .offer(
                  admin
                      .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                      .toCompletableFuture()
                      .join(),
                  admin.brokerFolders().toCompletableFuture().join());

      Assertions.assertNotEquals(Optional.empty(), Best);

      // test loop limit
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              Balancer.create(
                      SingleStepBalancer.class,
                      AlgorithmConfig.builder()
                          .clusterCost(clusterCostFunction)
                          .clusterConstraint((before, after) -> true)
                          .moveCost(List.of(moveCostFunction))
                          .movementConstraint(moveCosts -> true)
                          .limit(0)
                          .build())
                  .offer(
                      admin
                          .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                          .toCompletableFuture()
                          .join(),
                      admin.brokerFolders().toCompletableFuture().join()));

      // test cluster cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Balancer.create(
                  SingleStepBalancer.class,
                  AlgorithmConfig.builder()
                      .clusterCost(clusterCostFunction)
                      .clusterConstraint((before, after) -> false)
                      .moveCost(List.of(moveCostFunction))
                      .movementConstraint(moveCosts -> true)
                      .build())
              .offer(
                  admin
                      .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                      .toCompletableFuture()
                      .join(),
                  admin.brokerFolders().toCompletableFuture().join()));

      // test move cost predicate
      Assertions.assertEquals(
          Optional.empty(),
          Balancer.create(
                  SingleStepBalancer.class,
                  AlgorithmConfig.builder()
                      .clusterCost(clusterCostFunction)
                      .clusterConstraint((before, after) -> true)
                      .moveCost(List.of(moveCostFunction))
                      .movementConstraint(moveCosts -> false)
                      .build())
              .offer(
                  admin
                      .clusterInfo(admin.topicNames(false).toCompletableFuture().join())
                      .toCompletableFuture()
                      .join(),
                  admin.brokerFolders().toCompletableFuture().join()));
    }
  }

  @Test
  @Timeout(value = 60)
  void testNoReport() {
    var topic = Utils.randomString(10);
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(1).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(1));
      var handler = new BalancerHandler(admin);
      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler
                  .post(Channel.ofRequest(PostRequest.of(Map.of(BalancerHandler.LOOP_KEY, "0"))))
                  .toCompletableFuture()
                  .join());
      Utils.sleep(Duration.ofSeconds(5));
      var progress =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join());
      Assertions.assertNotNull(post.id);
      Assertions.assertEquals(post.id, progress.id);
      Assertions.assertFalse(progress.generated);
      Assertions.assertNotNull(progress.exception);
    }
  }

  @Test
  @Timeout(value = 60)
  void testPut() {
    // arrange
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor = new NoOpExecutor();
      var handler = new BalancerHandler(admin, theExecutor);
      var progress =
          submitPlanGeneration(
              handler,
              Map.of(
                  BalancerHandler.COST_WEIGHT_KEY,
                  defaultDecreasing,
                  BalancerHandler.LOOP_KEY,
                  "100"));
      var thePlanId = progress.id;

      // act
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", thePlanId))))
                  .toCompletableFuture()
                  .join());
      Utils.sleep(Duration.ofSeconds(1));

      // assert
      Assertions.assertEquals(Response.ACCEPT.code(), response.code());
      Assertions.assertEquals(thePlanId, response.id);
      Assertions.assertEquals(1, theExecutor.count());
    }
  }

  @Test
  @Timeout(value = 60)
  void testBadPut() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new NoOpExecutor());

      // no id offered
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> handler.put(Channel.EMPTY).toCompletableFuture().join(),
          "The 'id' field is required");

      // no such plan id
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", "no such plan"))))
                  .toCompletableFuture()
                  .join(),
          "The requested plan doesn't exists");
    }
  }

  @Test
  @Timeout(value = 360)
  void testSubmitRebalancePlanThreadSafe() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(bootstrapServers())) {
      admin.creator().topic(topic).numberOfPartitions(30).run().toCompletableFuture().join();
      Utils.sleep(Duration.ofSeconds(3));
      admin
          .moveToBrokers(
              admin.topicPartitions(Set.of(topic)).toCompletableFuture().join().stream()
                  .collect(Collectors.toMap(Function.identity(), ignored -> List.of(1))))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(3));
      var theExecutor = new NoOpExecutor();
      var handler = new BalancerHandler(admin, theExecutor);
      var progress = submitPlanGeneration(handler, Map.of());

      // use many threads to increase the chance to trigger a data race
      final int threadCount = Runtime.getRuntime().availableProcessors() * 3;
      final var executor = Executors.newFixedThreadPool(threadCount);
      final var barrier = new CyclicBarrier(threadCount);

      // launch threads
      IntStream.range(0, threadCount)
          .forEach(
              ignore ->
                  executor.submit(
                      () -> {
                        // the plan
                        final var request =
                            Channel.ofRequest(PostRequest.of(Map.of("id", progress.id)));
                        // use cyclic barrier to ensure all threads are ready to work
                        Utils.packException(() -> barrier.await());
                        // send the put request
                        handler.put(request).toCompletableFuture().join();
                      }));

      // await work done
      executor.shutdown();
      Assertions.assertTrue(
          Utils.packException(() -> executor.awaitTermination(threadCount * 3L, TimeUnit.SECONDS)));

      // the rebalance task is triggered in async manner, it may take some time to getting schedule
      Utils.sleep(Duration.ofMillis(500));
      // test if the plan has been executed just once
      Assertions.assertEquals(1, theExecutor.count());
    }
  }

  @Test
  @Timeout(value = 60)
  void testRebalanceOnePlanAtATime() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            @Override
            public CompletionStage<Void> run(
                Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
              return super.run(admin, targetAllocation, Duration.ofSeconds(5))
                  // Use another thread to block this completion to avoid deadlock in
                  // BalancerHandler#put
                  .thenApplyAsync(
                      i -> {
                        Utils.sleep(Duration.ofSeconds(10));
                        return i;
                      });
            }
          };
      var handler = new BalancerHandler(admin, theExecutor);
      var plan0 = submitPlanGeneration(handler, Map.of());
      var plan1 = submitPlanGeneration(handler, Map.of());

      Assertions.assertDoesNotThrow(
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", plan0.id))))
                  .toCompletableFuture()
                  .join());
      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .put(Channel.ofRequest(PostRequest.of(Map.of("id", plan1.id))))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  @Timeout(value = 60)
  void testRebalanceDetectOngoing() {
    try (var admin = Admin.of(bootstrapServers())) {
      var theTopic = Utils.randomString();
      admin.creator().topic(theTopic).numberOfPartitions(1).run().toCompletableFuture().join();
      try (var producer = Producer.of(bootstrapServers())) {
        var dummy = new byte[1024];
        IntStream.range(0, 100000)
            .mapToObj(i -> producer.sender().topic(theTopic).value(dummy).run())
            .collect(Collectors.toUnmodifiableSet())
            .forEach(i -> i.toCompletableFuture().join());
      }

      var handler = new BalancerHandler(admin, new NoOpExecutor());
      var theReport =
          submitPlanGeneration(
              handler,
              Map.of(
                  BalancerHandler.TOPICS_KEY, theTopic,
                  BalancerHandler.COST_WEIGHT_KEY, defaultDecreasing));

      // create an ongoing reassignment
      Assertions.assertEquals(
          1,
          admin.clusterInfo(Set.of(theTopic)).toCompletableFuture().join().replicaStream().count());
      admin
          .moveToBrokers(Map.of(TopicPartition.of(theTopic, 0), List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();

      // debounce wait
      for (int i = 0; i < 2; i++) {
        Utils.waitForNonNull(
            () ->
                admin
                    .clusterInfo(Set.of(theTopic))
                    .toCompletableFuture()
                    .join()
                    .replicaStream()
                    .noneMatch(r -> r.isFuture() || r.isRemoving() || r.isAdding()),
            Duration.ofSeconds(10),
            Duration.ofMillis(10));
      }

      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .put(Channel.ofRequest(PostRequest.of(Map.of("id", theReport.id))))
                          .toCompletableFuture()
                          .join())
              .getCause());
    }
  }

  @Test
  @Timeout(value = 60)
  void testPutSanityCheck() {
    var topic = createAndProduceTopic(1).get(0);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor = new NoOpExecutor();
      var handler = new BalancerHandler(admin, theExecutor);
      var theReport =
          submitPlanGeneration(
                  handler,
                  Map.of(
                      BalancerHandler.COST_WEIGHT_KEY, defaultDecreasing,
                      BalancerHandler.TOPICS_KEY, topic))
              .report;

      // pick a partition and alter its placement
      var theChange = theReport.changes.stream().findAny().orElseThrow();
      admin
          .moveToBrokers(
              Map.of(TopicPartition.of(theChange.topic, theChange.partition), List.of(0, 1, 2)))
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(10));

      // assert
      Assertions.assertInstanceOf(
          IllegalStateException.class,
          Assertions.assertThrows(
                  CompletionException.class,
                  () ->
                      handler
                          .put(Channel.ofRequest(PostRequest.of(Map.of("id", theReport.id))))
                          .toCompletableFuture()
                          .join(),
                  "The cluster state has changed, prevent the plan from execution")
              .getCause());
    }
  }

  @Test
  @Timeout(value = 60)
  void testLookupRebalanceProgress() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public CompletionStage<Void> run(
                Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
              return super.run(admin, targetAllocation, Duration.ofSeconds(5))
                  // Use another thread to block this completion to avoid deadlock in
                  // BalancerHandler#put
                  .thenApplyAsync(
                      i -> {
                        System.out.println("before block");
                        Utils.packException(() -> latch.await());
                        System.out.println("after block");
                        return i;
                      });
            }
          };
      var handler = new BalancerHandler(admin, theExecutor);
      var progress = submitPlanGeneration(handler, Map.of());
      Assertions.assertTrue(progress.generated, "The plan should be generated");

      // not scheduled yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress0 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(progress.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress0.id);
      Assertions.assertTrue(progress0.generated);
      Assertions.assertFalse(progress0.scheduled);
      Assertions.assertFalse(progress0.done);
      Assertions.assertNull(progress0.exception);

      // schedule
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", progress.id))))
                  .toCompletableFuture()
                  .join());
      Assertions.assertNotNull(response.id, "The plan should be executed");

      // not done yet
      Utils.sleep(Duration.ofSeconds(1));
      var progress1 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress1.id);
      Assertions.assertTrue(progress1.generated);
      Assertions.assertTrue(progress1.scheduled);
      Assertions.assertFalse(progress1.done);
      Assertions.assertNull(progress1.exception);

      // it is done
      System.out.println("before countDown");
      theExecutor.latch.countDown();
      System.out.println("after countDown");
      Utils.sleep(Duration.ofSeconds(1));
      var progress2 =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(progress.id, progress2.id);
      Assertions.assertTrue(progress2.generated);
      Assertions.assertTrue(progress2.scheduled);
      Assertions.assertTrue(progress2.done);
      Assertions.assertNull(progress2.exception);
    }
  }

  @Test
  @Timeout(value = 60)
  void testLookupBadExecutionProgress() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var theExecutor =
          new NoOpExecutor() {
            @Override
            public CompletionStage<Void> run(
                Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
              return super.run(admin, targetAllocation, Duration.ofSeconds(5))
                  .thenCompose(
                      ignored -> CompletableFuture.failedFuture(new RuntimeException("Boom")));
            }
          };
      var handler = new BalancerHandler(admin, theExecutor);
      var post =
          Assertions.assertInstanceOf(
              BalancerHandler.PostPlanResponse.class,
              handler.post(Channel.EMPTY).toCompletableFuture().join());
      Utils.waitFor(
          () ->
              ((BalancerHandler.PlanExecutionProgress)
                      handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join())
                  .generated);
      var generated =
          ((BalancerHandler.PlanExecutionProgress)
                  handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join())
              .generated;
      Assertions.assertTrue(generated, "The plan should be generated");

      // schedule
      var response =
          Assertions.assertInstanceOf(
              BalancerHandler.PutPlanResponse.class,
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", post.id))))
                  .toCompletableFuture()
                  .join());
      Assertions.assertNotNull(response.id, "The plan should be executed");

      // exception
      Utils.sleep(Duration.ofSeconds(1));
      var progress =
          Assertions.assertInstanceOf(
              BalancerHandler.PlanExecutionProgress.class,
              handler.get(Channel.ofTarget(response.id)).toCompletableFuture().join());
      Assertions.assertEquals(post.id, progress.id);
      Assertions.assertTrue(progress.generated);
      Assertions.assertTrue(progress.scheduled);
      Assertions.assertTrue(progress.done);
      Assertions.assertNotNull(progress.exception);
      Assertions.assertInstanceOf(String.class, progress.exception);
    }
  }

  @Test
  @Timeout(value = 60)
  void testBadLookupRequest() {
    createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new NoOpExecutor());

      Assertions.assertEquals(
          404, handler.get(Channel.ofTarget("no such plan")).toCompletableFuture().join().code());

      // plan doesn't exists
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", "no such plan"))))
                  .toCompletableFuture()
                  .join(),
          "This plan doesn't exists");
    }
  }

  @Test
  @Timeout(value = 60)
  void testPutIdempotent() {
    var topics = createAndProduceTopic(3);
    try (var admin = Admin.of(bootstrapServers())) {
      var handler = new BalancerHandler(admin, new StraightPlanExecutor());
      var progress =
          submitPlanGeneration(
              handler,
              Map.of(
                  BalancerHandler.COST_WEIGHT_KEY,
                  defaultDecreasing,
                  BalancerHandler.TOPICS_KEY,
                  String.join(",", topics)));

      Assertions.assertDoesNotThrow(
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", progress.id))))
                  .toCompletableFuture()
                  .join(),
          "Schedule the rebalance task");

      // Wait until the migration occurred
      try {
        Utils.waitFor(
            () ->
                admin
                    .clusterInfo(Set.copyOf(topics))
                    .toCompletableFuture()
                    .join()
                    .replicaStream()
                    .anyMatch(replica -> replica.isFuture() || !replica.inSync()));
      } catch (Exception ignore) {
      }

      Assertions.assertDoesNotThrow(
          () ->
              handler
                  .put(Channel.ofRequest(PostRequest.of(Map.of("id", progress.id))))
                  .toCompletableFuture()
                  .join(),
          "Idempotent behavior");
    }
  }

  /** Submit the plan and wait until it generated. */
  private BalancerHandler.PlanExecutionProgress submitPlanGeneration(
      BalancerHandler handler, Map<String, Object> requestBody) {
    var post =
        (BalancerHandler.PostPlanResponse)
            handler
                .post(Channel.ofRequest(PostRequest.of(requestBody)))
                .toCompletableFuture()
                .join();
    Utils.waitFor(
        () -> {
          var progress =
              (BalancerHandler.PlanExecutionProgress)
                  handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
          Assertions.assertNull(progress.exception, progress.exception);
          return progress.generated;
        });
    return (BalancerHandler.PlanExecutionProgress)
        handler.get(Channel.ofTarget(post.id)).toCompletableFuture().join();
  }

  private static class NoOpExecutor implements RebalancePlanExecutor {

    private final LongAdder executionCounter = new LongAdder();

    @Override
    public CompletionStage<Void> run(
        Admin admin, ClusterInfo<Replica> targetAllocation, Duration timeout) {
      executionCounter.increment();
      return CompletableFuture.completedFuture(null);
    }

    int count() {
      return executionCounter.intValue();
    }
  }

  public static class DecreasingCost implements HasClusterCost {

    private ClusterInfo<Replica> original;

    public DecreasingCost(Configuration configuration) {}

    private double value0 = 1.0;

    @Override
    public synchronized ClusterCost clusterCost(
        ClusterInfo<Replica> clusterInfo, ClusterBean clusterBean) {
      if (original == null) original = clusterInfo;
      if (ClusterInfo.findNonFulfilledAllocation(original, clusterInfo).isEmpty()) return () -> 1;
      double theCost = value0;
      value0 = value0 * 0.998;
      return () -> theCost;
    }
  }
}
