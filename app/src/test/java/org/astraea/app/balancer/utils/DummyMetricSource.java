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
package org.astraea.app.balancer.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.metrics.HasBeanObject;

public class DummyMetricSource implements MetricSource {

  @Override
  public Collection<HasBeanObject> metrics(IdentifiedFetcher fetcher, int brokerId) {
    return List.of();
  }

  @Override
  public Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> allBeans() {
    return Map.of();
  }

  @Override
  public double warmUpProgress() {
    return 0;
  }

  @Override
  public void awaitMetricReady() {}

  @Override
  public void drainMetrics() {}

  @Override
  public void close() {}
}
