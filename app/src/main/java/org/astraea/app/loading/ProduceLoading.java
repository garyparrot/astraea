package org.astraea.app.loading;

import com.beust.jcommander.Parameter;
import org.astraea.app.argument.Argument;
import org.astraea.app.common.DataSize;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.LongAdder;

public class ProduceLoading extends Argument {

  @Parameter(names =  "--topic")
  public String topicName;

  @Parameter(names = "--partitions")
  public int partitionSize;

  @Parameter(names = "--replicas")
  public short replicaSize;

  @Parameter(names = "--data.size", converter = DataSize.Field.class)
  public DataSize dataSize;

  public static void main(String[] args) {
    ProduceLoading app = Argument.parse(new ProduceLoading(), args);
    app.run();
  }

  public void run() {
    ForkJoinPool.commonPool().submit()
  }

}