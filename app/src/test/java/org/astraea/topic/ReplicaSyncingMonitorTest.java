package org.astraea.topic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartition;
import org.astraea.argument.ArgumentUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ReplicaSyncingMonitorTest {

  private ByteArrayOutputStream mockOutput;
  private final PrintStream stdout = System.out;
  private final List<Runnable> tearDownTasks = new ArrayList<>();

  @BeforeEach
  void setUp() {
    mockOutput = new ByteArrayOutputStream();
    System.setOut(new PrintStream(mockOutput));
  }

  @AfterEach
  void tearDown() {
    tearDownTasks.forEach(Runnable::run);
    tearDownTasks.clear();

    System.setOut(stdout);
  }

  // helper functions
  private static final BiFunction<String, Integer, TopicPartition> topicPartition =
      TopicPartition::new;
  private static final BiFunction<Integer, long[], List<Replica>> replica =
      (count, size) ->
          IntStream.range(0, count)
              .mapToObj(
                  i -> new Replica(i, 0, size[i], i == 0, size[i] == size[0], false, "/tmp/log"))
              .collect(Collectors.toUnmodifiableList());

  @Test
  void execute() throws InterruptedException {
    // arrange
    TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    when(mockTopicAdmin.topicNames()).thenReturn(Set.of("topic-1", "topic-2", "topic-3"));
    when(mockTopicAdmin.replicas(anySet()))
        .thenReturn(
            Map.of(
                /* progress 0% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 0, 0}),
                topicPartition.apply("topic-2", 0), replica.apply(3, new long[] {100, 0, 0}),
                topicPartition.apply("topic-3", 0), replica.apply(3, new long[] {100, 0, 0})))
        .thenReturn(
            Map.of(
                /* progress 50% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 50, 50}),
                topicPartition.apply("topic-2", 0), replica.apply(3, new long[] {100, 50, 50}),
                topicPartition.apply("topic-3", 0), replica.apply(3, new long[] {100, 50, 50})))
        .thenReturn(
            Map.of(
                /* progress 100% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 100, 100}),
                topicPartition.apply("topic-2", 0), replica.apply(3, new long[] {100, 100, 100}),
                topicPartition.apply("topic-3", 0), replica.apply(3, new long[] {100, 100, 100})));

    Thread executionThread =
        new Thread(
            () -> {
              ReplicaSyncingMonitor.execute(
                  mockTopicAdmin,
                  ArgumentUtil.parseArgument(
                      new ReplicaSyncingMonitor.Argument(),
                      new String[] {"--bootstrap.servers", "whatever:9092"}));
            });
    tearDownTasks.add(
        () -> {
          if (executionThread.isAlive()) {
            when(mockTopicAdmin.replicas(anySet())).thenThrow(RuntimeException.class);
            executionThread.interrupt();
          }
        });

    // act
    executionThread.start();
    // TODO: allow client to change wait interval, so we can save some waiting time. I am tired of
    // those slow tests.
    TimeUnit.SECONDS.timedJoin(executionThread, 4);

    // assert execution will exit
    assertSame(Thread.State.TERMINATED, executionThread.getState());

    // assert important info has been printed
    assertTrue(mockOutput.toString().contains("topic-1"));
    assertTrue(mockOutput.toString().contains("topic-2"));
    assertTrue(mockOutput.toString().contains("topic-3"));
    assertTrue(mockOutput.toString().contains("Every replica is synced"));
  }

  @Test
  void executeWithKeepTrack() throws InterruptedException {
    // arrange
    TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    when(mockTopicAdmin.topicNames()).thenReturn(Set.of("topic-1"));
    when(mockTopicAdmin.replicas(anySet()))
        .thenReturn(
            Map.of(
                /* progress 0% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 0, 0})))
        .thenReturn(
            Map.of(
                /* progress 50% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 50, 50})))
        .thenReturn(
            Map.of(
                /* progress 100% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 100, 100})))
        .thenReturn(
            Map.of(
                /* progress 100%, --keep-track should not exit when progress reach 100% */
                topicPartition.apply("topic-1", 0), replica.apply(3, new long[] {100, 100, 100})))
        .thenReturn(
            Map.of(
                /* progress 0%, another alert happened */
                topicPartition.apply("topic-1", 0),
                replica.apply(4, new long[] {100, 100, 100, 0})))
        .thenReturn(
            Map.of(
                /* progress 50% */
                topicPartition.apply("topic-1", 0),
                replica.apply(4, new long[] {100, 100, 100, 50})))
        .thenReturn(
            Map.of(
                /* progress 100% */
                topicPartition.apply("topic-1", 0),
                replica.apply(4, new long[] {100, 100, 100, 100})));

    Thread executionThread =
        new Thread(
            () -> {
              try {
                ReplicaSyncingMonitor.execute(
                    mockTopicAdmin,
                    ArgumentUtil.parseArgument(
                        new ReplicaSyncingMonitor.Argument(),
                        new String[] {"--bootstrap.servers", "whatever:9092", "--keep-track"}));
              } catch (Exception e) {
                // swallow interrupted error
              }
            });
    tearDownTasks.add(
        () -> {
          if (executionThread.isAlive()) {
            when(mockTopicAdmin.replicas(anySet())).thenThrow(RuntimeException.class);
            executionThread.interrupt();
          }
        });

    // act
    executionThread.start();
    // TODO: allow client to change wait interval, so we can save some waiting time. I am tired of
    // those slow tests.
    TimeUnit.SECONDS.timedJoin(executionThread, 7);

    // assert execution will not exit even if all replicas are synced
    assertNotEquals(Thread.State.TERMINATED, executionThread.getState());

    // assert TopicAdmin#replicas call multiple times
    verify(mockTopicAdmin, atLeast(4)).replicas(anySet());

    // assert important info has been printed
    assertTrue(mockOutput.toString().contains("topic-1"));
    assertTrue(mockOutput.toString().contains("Every replica is synced"));
  }

  @Test
  void executeWithTopic() throws InterruptedException {
    // arrange
    TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    when(mockTopicAdmin.replicas(Set.of("target-topic")))
        .thenReturn(
            Map.of(
                topicPartition.apply("target-topic", 0),
                replica.apply(3, new long[] {100, 100, 100})));
    when(mockTopicAdmin.replicas(anySet())).thenThrow(IllegalStateException.class);

    Thread executionThread =
        new Thread(
            () -> {
              try {
                ReplicaSyncingMonitor.execute(
                    mockTopicAdmin,
                    ArgumentUtil.parseArgument(
                        new ReplicaSyncingMonitor.Argument(),
                        new String[] {
                          "--bootstrap.servers", "whatever:9092", "--topic", "target-topic"
                        }));
              } catch (IllegalStateException e) {
                // immediate fail due to bad behavior of --topic flag
                fail();
              }
            });
    tearDownTasks.add(
        () -> {
          if (executionThread.isAlive()) {
            when(mockTopicAdmin.replicas(anySet())).thenThrow(RuntimeException.class);
            executionThread.interrupt();
          }
        });

    // act
    executionThread.start();
    // TODO: allow client to change wait interval, so we can save some waiting time. I am tired of
    // those slow tests.
    TimeUnit.SECONDS.timedJoin(executionThread, 2);

    // assert execution exit
    assertSame(Thread.State.TERMINATED, executionThread.getState());

    // assert TopicAdmin#replicas call at least 1 times with Set.of("target-topic")
    verify(mockTopicAdmin, atLeast(1)).replicas(Set.of("target-topic"));
  }

  @Test
  void findNonSyncedTopicPartition() {
    // arrange
    final TopicAdmin mockTopicAdmin = mock(TopicAdmin.class);
    final Set<String> topics = Set.of("topic1", "topic2");
    final List<Replica> replicaList1 =
        List.of(
            new Replica(0, 0, 0, true, true, false, "/tmp/broker0/logA"),
            new Replica(1, 0, 0, true, true, false, "/tmp/broker1/logA"));
    final List<Replica> replicaList2 =
        List.of(
            new Replica(0, 0, 100, true, false, false, "/tmp/broker0/logB"),
            new Replica(1, 0, 100, true, true, false, "/tmp/broker1/logB"));
    when(mockTopicAdmin.replicas(any()))
        .thenReturn(
            Map.of(
                new TopicPartition("topic1", 0), replicaList1,
                new TopicPartition("topic2", 0), replicaList2,
                new TopicPartition("topic2", 1), replicaList2));

    // act
    Set<TopicPartition> nonSyncedTopicPartition =
        ReplicaSyncingMonitor.findNonSyncedTopicPartition(mockTopicAdmin, topics);

    // assert
    assertTrue(nonSyncedTopicPartition.contains(new TopicPartition("topic2", 1)));
    assertTrue(nonSyncedTopicPartition.contains(new TopicPartition("topic2", 0)));
    assertFalse(nonSyncedTopicPartition.contains(new TopicPartition("topic1", 0)));
  }

  @ParameterizedTest
  @CsvSource(
      delimiterString = "(is",
      value = {
        "--bootstrap.servers localhost:5566                                 (is ok",
        "--bootstrap.servers localhost:5566 --keep-track                    (is ok",
        "--bootstrap.servers localhost:5566 --topic my-topic --keep-track   (is ok",
        "--bootstrap.servers localhost:5566 --whatever                      (is not ok",
        "--bootstrap.servers localhost:5566 sad                             (is not ok",
        "wuewuewuewue                                                      (is not ok",
        "--server                                                          (is not ok",
      })
  void ensureArgumentFlagExists(String args, String exception) {
    switch (exception) {
      case "ok":
        assertDoesNotThrow(
            () ->
                ArgumentUtil.parseArgument(new ReplicaSyncingMonitor.Argument(), args.split(" ")));
        break;
      case "not ok":
        assertThrows(
            Exception.class,
            () ->
                ArgumentUtil.parseArgument(new ReplicaSyncingMonitor.Argument(), args.split(" ")));
        break;
      default:
        fail();
        break;
    }
  }
}
