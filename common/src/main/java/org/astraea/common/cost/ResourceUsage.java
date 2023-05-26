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
package org.astraea.common.cost;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/** A collection of resource usage. */
public record ResourceUsage(Map<String, Double> usage) {

  public static final ResourceUsage EMPTY = new ResourceUsage(Map.of());

  public ResourceUsage(Map<String, Double> usage) {
    this.usage = Map.copyOf(usage);
  }

  public ResourceUsage mergeUsage(Stream<ResourceUsage> usages) {
    var sum = new HashMap<>(usage);
    usages.forEach(
        usage -> usage.usage().forEach((k, v) -> sum.put(k, sum.getOrDefault(k, 0.0) + v)));
    return new ResourceUsage(sum);
  }

  public ResourceUsage removeUsage(Stream<ResourceUsage> usages) {
    var sum = new HashMap<>(usage);
    usages.forEach(
        usage -> usage.usage().forEach((k, v) -> sum.put(k, sum.getOrDefault(k, 0.0) - v)));
    return new ResourceUsage(sum);
  }
}