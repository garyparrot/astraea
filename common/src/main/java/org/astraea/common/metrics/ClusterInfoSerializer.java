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
package org.astraea.common.metrics;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.consumer.DDeserializer;

public class ClusterInfoSerializer {

  public static void serialize(ClusterInfo clusterBean, OutputStream stream) {
    throw new UnsupportedOperationException();
    // byte[] serialize = Serializer.CLUSTER_INFO.serialize("", List.of(), clusterBean);
    // Utils.packException(() -> stream.write(serialize));
  }

  public static ClusterInfo deserialize(InputStream stream) {
    byte[] bytes = Utils.packException(stream::readAllBytes);
    return DDeserializer.CLUSTER_INFO.deserialize("", List.of(), bytes);
  }
}