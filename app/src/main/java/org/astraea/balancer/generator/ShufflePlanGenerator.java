package org.astraea.balancer.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.balancer.alpha.ClusterLogAllocation;
import org.astraea.balancer.alpha.LogPlacement;
import org.astraea.balancer.alpha.RebalancePlanProposal;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.NodeInfo;

/**
 * The {@link ShufflePlanGenerator} proposes a new log placement based on the current log placement,
 * but with a few random placement changes. Noted that this {@link RebalancePlanGenerator} making
 * changes to replica allocation on brokers. It doesn't consider which data directory at the
 * destination broker the new placement will be. <br>
 * <br>
 * The following operations are considered as a valid shuffle action:
 *
 * <ol>
 *   <li>Remove a replica from the replica set, then add another broker(must not be part of the
 *       replica set before this action) into the replica set.
 *   <li>Change the leader of a partition by a member of this replica set, the original leader
 *       becomes a follower.
 * </ol>
 */
public class ShufflePlanGenerator implements RebalancePlanGenerator {

  private final Supplier<Integer> numberOfShuffle;

  public ShufflePlanGenerator(int origin, int bound) {
    this(() -> ThreadLocalRandom.current().nextInt(origin, bound));
  }

  public ShufflePlanGenerator(Supplier<Integer> numberOfShuffle) {
    this.numberOfShuffle = numberOfShuffle;
  }

  @Override
  public Stream<RebalancePlanProposal> generate(ClusterInfo clusterInfo) {
    return Stream.generate(
        () -> {
          final var rebalancePlanBuilder = RebalancePlanProposal.builder();
          final var brokerIds =
              clusterInfo.nodes().stream()
                  .map(NodeInfo::id)
                  .collect(Collectors.toUnmodifiableSet());

          if (brokerIds.size() == 0)
            return rebalancePlanBuilder
                .addWarning("Why there is no broker?")
                .noRebalancePlan()
                .build();

          if (brokerIds.size() == 1)
            return rebalancePlanBuilder
                .addWarning("Only one broker exists. There is no reason to rebalance.")
                .noRebalancePlan()
                .build();

          final var shuffleCount = numberOfShuffle.get();
          final var currentAllocation =
              new HashMap<>(ClusterLogAllocation.of(clusterInfo).allocation());
          final var pickingList =
              currentAllocation.keySet().stream().collect(Collectors.toUnmodifiableList());

          rebalancePlanBuilder.addInfo("About to make " + shuffleCount + " shuffle.");
          for (int i = 0; i < shuffleCount; i++) {
            final var sourceTopicPartitionIndex =
                ThreadLocalRandom.current().nextInt(0, pickingList.size());
            final var sourceTopicPartition = pickingList.get(sourceTopicPartitionIndex);
            final var sourceLogPlacements = currentAllocation.get(sourceTopicPartition);
            final var sourceLogPlacementIndex =
                ThreadLocalRandom.current().nextInt(0, sourceLogPlacements.size());
            final var sourceLogPlacement = sourceLogPlacements.get(sourceLogPlacementIndex);
            final var sourceIsLeader = sourceLogPlacementIndex == 0;

            Consumer<Integer> replicaSetMigration =
                (targetBroker) -> {
                  currentAllocation.put(
                      sourceTopicPartition,
                      sourceLogPlacements.stream()
                          .map(
                              logPlacement ->
                                  logPlacement == sourceLogPlacement
                                      ? LogPlacement.of(targetBroker)
                                      : logPlacement)
                          .collect(Collectors.toUnmodifiableList()));
                  rebalancePlanBuilder.addInfo(
                      String.format(
                          "Change replica set of topic %s partition %d, from %d to %d.",
                          sourceTopicPartition.topic(),
                          sourceTopicPartition.partition(),
                          sourceLogPlacement.broker(),
                          targetBroker));
                };
            Consumer<Integer> leaderFollowerMigration =
                (targetBroker) -> {
                  currentAllocation.put(
                      sourceTopicPartition,
                      sourceLogPlacements.stream()
                          .map(
                              log -> {
                                if (log.broker() == sourceLogPlacement.broker())
                                  return LogPlacement.of(targetBroker);
                                else if (log.broker() == targetBroker)
                                  return LogPlacement.of(sourceLogPlacement.broker());
                                else return log;
                              })
                          .collect(Collectors.toUnmodifiableList()));
                  rebalancePlanBuilder.addInfo(
                      String.format(
                          "Change the log identity of topic %s partition %d replica %d, from %s to %s",
                          sourceTopicPartition.topic(),
                          sourceTopicPartition.partition(),
                          sourceLogPlacement.broker(),
                          sourceIsLeader ? "leader" : "follower",
                          sourceIsLeader ? "follower" : "leader"));
                };

            // generate a set of valid migration broker for given placement.
            // [Valid movement 1] add all brokers and remove all broker in current replica set
            final var replicaSetMigrationSet =
                brokerIds.stream()
                    .filter(
                        broker ->
                            sourceLogPlacements.stream().anyMatch(log -> log.broker() == broker))
                    .map(
                        targetBroker ->
                            Movement.of(
                                targetBroker, () -> replicaSetMigration.accept(targetBroker)))
                    .collect(Collectors.toUnmodifiableSet());
            final var validMigrationCandidates = new ArrayList<>(replicaSetMigrationSet);
            // [Valid movement 2] add all leader/follower change candidate
            if (sourceLogPlacements.size() > 1) {
              if (sourceIsLeader) {
                // leader can migrate its identity to follower.
                validMigrationCandidates.addAll(
                    sourceLogPlacements.stream()
                        .skip(1)
                        .map(LogPlacement::broker)
                        .map(
                            targetBroker ->
                                Movement.of(
                                    targetBroker,
                                    () -> leaderFollowerMigration.accept(targetBroker)))
                        .collect(Collectors.toUnmodifiableList()));
              } else {
                // follower can migrate its identity to leader.
                final var leaderLogBroker = sourceLogPlacements.get(0).broker();
                validMigrationCandidates.add(
                    Movement.of(
                        leaderLogBroker, () -> leaderFollowerMigration.accept(leaderLogBroker)));
              }
            }

            // pick a migration and execute
            final var selectedMigration =
                validMigrationCandidates.get(
                    ThreadLocalRandom.current().nextInt(0, validMigrationCandidates.size()));
            selectedMigration.execution.run();
          }

          return rebalancePlanBuilder.build();
        });
  }

  private static class Movement {
    int targetBroker;

    Runnable execution;

    private static Movement of(int targetBroker) {
      return of(
          targetBroker,
          () -> {
            throw new AssertionError();
          });
    }

    private static Movement of(int targetBroker, Runnable execution) {
      return new Movement(targetBroker, execution);
    }

    private Movement(int targetBroker, Runnable execution) {
      this.targetBroker = targetBroker;
      this.execution = execution;
    }
  }
}
