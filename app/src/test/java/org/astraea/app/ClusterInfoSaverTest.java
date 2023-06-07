package org.astraea.app;

import org.astraea.common.ByteUtils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class ClusterInfoSaverTest {

  public static final String realCluster =
      "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655,192.168.103.181:25655,192.168.103.182:25655";

  @Test
  void testClusterInfo() throws IOException {
    try (var admin = Admin.of(realCluster)) {
      ClusterInfo save = admin.topicNames(true)
          .thenCompose(admin::clusterInfo)
          .toCompletableFuture()
          .join();
      byte[] bytes = ByteUtils.toBytes(save);
      Path tempFile = Files.createTempFile("cluster-info", ".bin");
      try (var output = Files.newOutputStream(tempFile)) {
        output.write(bytes);
      }
      System.out.println("ClusterInfo saved at " + tempFile.toAbsolutePath().toString());
    }
  }
}
