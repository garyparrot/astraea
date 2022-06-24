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
package org.astraea.app.balancer.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.astraea.app.metrics.HasBeanObject;

/**
 * Declare the implementation class has the ability to return a collection of {@link HasBeanObject}
 * result from a {@link IdentifiedFetcher} for a specific broker at <strong>the function call
 * moment</strong>.
 */
public interface MetricSource extends AutoCloseable {
  Collection<HasBeanObject> metrics(IdentifiedFetcher fetcher, int brokerId);

  default Map<Integer, Collection<HasBeanObject>> metrics(
      Set<Integer> nodes, IdentifiedFetcher fetcher) {
    return nodes.stream()
        .collect(
            Collectors.toUnmodifiableMap(
                brokerId -> brokerId, brokerId -> metrics(fetcher, brokerId)));
  }

  double warmUpProgress();

  void awaitMetricReady();

  void drainMetrics();

  @Override
  void close();
}
