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

import com.google.gson.reflect.TypeToken;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.common.DataRate;

public class ThrottleHandler implements Handler {
  private final Admin admin;

  public ThrottleHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Response get(Channel channel) {
    return get();
  }

  private Response get() {
    final var brokers =
        admin.brokers().entrySet().stream()
            .map(
                entry -> {
                  final var egress =
                      entry
                          .getValue()
                          .value("leader.replication.throttled.rate")
                          .map(Long::valueOf)
                          .orElse(null);
                  final var ingress =
                      entry
                          .getValue()
                          .value("follower.replication.throttled.rate")
                          .map(Long::valueOf)
                          .orElse(null);
                  return new BrokerThrottle(entry.getKey(), ingress, egress);
                })
            .collect(Collectors.toUnmodifiableSet());
    final var topicConfigs = admin.topics();
    final var leaderTargets =
        topicConfigs.entrySet().stream()
            .map(
                entry ->
                    toReplicaSet(
                        entry.getKey(),
                        entry.getValue().value("leader.replication.throttled.replicas").orElse("")))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());
    final var followerTargets =
        topicConfigs.entrySet().stream()
            .map(
                entry ->
                    toReplicaSet(
                        entry.getKey(),
                        entry
                            .getValue()
                            .value("follower.replication.throttled.replicas")
                            .orElse("")))
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableSet());

    return new ThrottleSetting(brokers, simplify(leaderTargets, followerTargets));
  }

  @Override
  public Response post(Channel channel) {
    var brokerToUpdate =
        channel
            .request()
            .<Collection<BrokerThrottle>>get(
                "brokers",
                TypeToken.getParameterized(Collection.class, BrokerThrottle.class).getType())
            .orElse(List.of());
    var topics =
        channel
            .request()
            .<Collection<TopicThrottle>>get(
                "topics",
                TypeToken.getParameterized(Collection.class, TopicThrottle.class).getType())
            .orElse(List.of());

    final var throttler = admin.replicationThrottler();
    final var acceptedTopicThrottle = new AtomicInteger();
    // ingress
    throttler.ingress(
        brokerToUpdate.stream()
            .filter(broker -> broker.ingress != null)
            .collect(
                Collectors.toUnmodifiableMap(
                    broker -> broker.id, broker -> DataRate.Byte.of(broker.ingress).perSecond())));
    // egress
    throttler.egress(
        brokerToUpdate.stream()
            .filter(broker -> broker.egress != null)
            .collect(
                Collectors.toUnmodifiableMap(
                    broker -> broker.id, broker -> DataRate.Byte.of(broker.egress).perSecond())));
    // topic
    topics.stream()
        .filter(topic -> topic.partition == null)
        .filter(topic -> topic.broker == null)
        .filter(topic -> topic.type == null)
        .map(topic -> topic.name)
        .peek(x -> acceptedTopicThrottle.incrementAndGet())
        .forEach(throttler::throttle);
    // partition
    topics.stream()
        .filter(topic -> topic.partition != null)
        .filter(topic -> topic.broker == null)
        .filter(topic -> topic.type == null)
        .map(topic -> TopicPartition.of(topic.name, topic.partition))
        .peek(x -> acceptedTopicThrottle.incrementAndGet())
        .forEach(throttler::throttle);
    // replica
    topics.stream()
        .filter(topic -> topic.partition != null)
        .filter(topic -> topic.broker != null)
        .filter(topic -> topic.type == null)
        .map(topic -> TopicPartitionReplica.of(topic.name, topic.partition, topic.broker))
        .peek(x -> acceptedTopicThrottle.incrementAndGet())
        .forEach(throttler::throttle);
    // leader/follower
    topics.stream()
        .filter(topic -> topic.partition != null)
        .filter(topic -> topic.broker != null)
        .filter(topic -> topic.type != null)
        .peek(x -> acceptedTopicThrottle.incrementAndGet())
        .forEach(
            topic -> {
              var replica = TopicPartitionReplica.of(topic.name, topic.partition, topic.broker);
              if (topic.type.equals("leader")) throttler.throttleLeader(replica);
              else if (topic.type.equals("follower")) throttler.throttleFollower(replica);
              else throw new IllegalArgumentException("Unknown throttle type: " + topic.type);
            });

    if (acceptedTopicThrottle.get() != topics.size()) {
      throw new IllegalArgumentException("Some topic throttle combination is not supported");
    }

    var affectedResources = throttler.apply();
    var affectedBrokers =
        Stream.concat(
                affectedResources.ingress().keySet().stream(),
                affectedResources.egress().keySet().stream())
            .distinct()
            .map(
                broker ->
                    BrokerThrottle.of(
                        broker,
                        affectedResources.ingress().get(broker),
                        affectedResources.egress().get(broker)))
            .collect(Collectors.toUnmodifiableList());
    var affectedTopics = simplify(affectedResources.leaders(), affectedResources.followers());
    return new ThrottleSetting(affectedBrokers, affectedTopics);
  }

  @Override
  public Response delete(Channel channel) {
    if (channel.queries().containsKey("topic")) {
      var topic =
          new TopicThrottle(
              channel.queries().get("topic"),
              Optional.ofNullable(channel.queries().get("partition"))
                  .map(Integer::parseInt)
                  .orElse(null),
              Optional.ofNullable(channel.queries().get("replica"))
                  .map(Integer::parseInt)
                  .orElse(null),
              Arrays.stream(LogIdentity.values())
                  .filter(x -> x.name().equals(channel.queries().getOrDefault("type", "")))
                  .findFirst()
                  .orElse(null));

      if (topic.partition == null && topic.broker == null && topic.type == null)
        admin.clearReplicationThrottle(topic.name);
      else if (topic.partition != null && topic.broker == null && topic.type == null)
        admin.clearReplicationThrottle(TopicPartition.of(topic.name, topic.partition));
      else if (topic.partition != null && topic.broker != null && topic.type == null)
        admin.clearReplicationThrottle(
            TopicPartitionReplica.of(topic.name, topic.partition, topic.broker));
      else if (topic.partition != null && topic.broker != null && topic.type.equals("leader"))
        admin.clearLeaderReplicationThrottle(
            TopicPartitionReplica.of(topic.name, topic.partition, topic.broker));
      else if (topic.partition != null && topic.broker != null && topic.type.equals("follower"))
        admin.clearFollowerReplicationThrottle(
            TopicPartitionReplica.of(topic.name, topic.partition, topic.broker));

      return Response.ACCEPT;
    } else if (channel.queries().containsKey("broker")) {
      var broker = Integer.parseInt(channel.queries().get("broker"));
      var bandwidth = channel.queries().get("type").split(" ");
      for (String target : bandwidth) {
        if (target.equals("ingress")) admin.clearIngressReplicationThrottle(Set.of(broker));
        else if (target.equals("egress")) admin.clearEgressReplicationThrottle(Set.of(broker));
        else throw new IllegalArgumentException("Unknown clear target: " + target);
      }
      return Response.ACCEPT;
    } else {
      return Response.BAD_REQUEST;
    }
  }

  /**
   * break apart the {@code throttled.replica} string setting into a set of topic/partition/replicas
   */
  private Set<TopicPartitionReplica> toReplicaSet(String topic, String throttledReplicas) {
    if (throttledReplicas.isEmpty()) return Set.of();

    // TODO: support for wildcard throttle might be implemented in the future, see
    // https://github.com/skiptests/astraea/issues/625
    if (throttledReplicas.equals("*"))
      throw new UnsupportedOperationException("This API doesn't support wildcard throttle");

    return Arrays.stream(throttledReplicas.split(","))
        .map(pair -> pair.split(":"))
        .map(
            pair ->
                TopicPartitionReplica.of(
                    topic, Integer.parseInt(pair[0]), Integer.parseInt(pair[1])))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Given a series of leader/follower throttle config, this method attempts to reduce its size into
   * the simplest form by merging any targets with a common topic/partition/replica scope throttle
   * target.
   */
  private Set<TopicThrottle> simplify(
      Set<TopicPartitionReplica> leaders, Set<TopicPartitionReplica> followers) {
    var commonReplicas =
        leaders.stream().filter(followers::contains).collect(Collectors.toUnmodifiableSet());

    var simplifiedReplicas =
        commonReplicas.stream()
            .map(
                replica ->
                    new TopicThrottle(
                        replica.topic(), replica.partition(), replica.brokerId(), null));
    var leaderReplicas =
        leaders.stream()
            .filter(replica -> !commonReplicas.contains(replica))
            .map(
                replica ->
                    new TopicThrottle(
                        replica.topic(),
                        replica.partition(),
                        replica.brokerId(),
                        LogIdentity.leader));
    var followerReplicas =
        followers.stream()
            .filter(replica -> !commonReplicas.contains(replica))
            .map(
                replica ->
                    new TopicThrottle(
                        replica.topic(),
                        replica.partition(),
                        replica.brokerId(),
                        LogIdentity.follower));

    return Stream.concat(Stream.concat(simplifiedReplicas, leaderReplicas), followerReplicas)
        .collect(Collectors.toUnmodifiableSet());
  }

  static class ThrottleSetting implements Response {

    final Collection<BrokerThrottle> brokers;
    final Collection<TopicThrottle> topics;

    ThrottleSetting(Collection<BrokerThrottle> brokers, Collection<TopicThrottle> topics) {
      this.brokers = brokers;
      this.topics = topics;
    }
  }

  static class BrokerThrottle {
    final int id;
    final Long ingress;
    final Long egress;

    static BrokerThrottle of(int id, DataRate ingress, DataRate egress) {
      return new BrokerThrottle(
          id,
          Optional.ofNullable(ingress).map(i -> (long) i.byteRate()).orElse(null),
          Optional.ofNullable(egress).map(i -> (long) i.byteRate()).orElse(null));
    }

    BrokerThrottle(int id, Long ingress, Long egress) {
      this.id = id;
      this.ingress = ingress;
      this.egress = egress;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BrokerThrottle that = (BrokerThrottle) o;
      return id == that.id
          && Objects.equals(ingress, that.ingress)
          && Objects.equals(egress, that.egress);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, ingress, egress);
    }

    @Override
    public String toString() {
      return "BrokerThrottle{"
          + "broker="
          + id
          + ", ingress="
          + ingress
          + ", egress="
          + egress
          + '}';
    }
  }

  static class TopicThrottle {
    final String name;
    final Integer partition;
    final Integer broker;
    final String type;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TopicThrottle that = (TopicThrottle) o;
      return Objects.equals(name, that.name)
          && Objects.equals(partition, that.partition)
          && Objects.equals(broker, that.broker)
          && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, partition, broker, type);
    }

    TopicThrottle(String name, Integer partition, Integer broker, LogIdentity identity) {
      this.name = Objects.requireNonNull(name);
      this.partition = partition;
      this.broker = broker;
      this.type = (identity == null) ? null : identity.name();
    }

    @Override
    public String toString() {
      return "ThrottleTarget{"
          + "name='"
          + name
          + '\''
          + ", partition="
          + partition
          + ", broker="
          + broker
          + ", type="
          + type
          + '}';
    }
  }

  enum LogIdentity {
    leader,
    follower
  }
}
