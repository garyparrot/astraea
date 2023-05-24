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
package org.astraea.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class MathUtils {

  static double distance(double[] coordinate0, double[] coordinate1) {
    if (coordinate0.length != coordinate1.length)
      throw new IllegalArgumentException(
          "Mismatch dimension: " + coordinate0.length + ", " + coordinate1.length);

    double value = 0;
    for (int i = 0; i < coordinate0.length; i++)
      value += (coordinate0[i] - coordinate1[i]) * (coordinate0[i] - coordinate1[i]);
    return Math.sqrt(value);
  }

  public static <Element> Collection<List<Element>> kMeans(
      int k, int repeat, List<Element> elements, Function<Element, double[]> toCoordinate) {
    // record Vector(double[] values) {
    //   void add(Vector that) {
    //     for(int i = 0;i < that.values.length; i++)
    //       this.values[i] += that.values[i];
    //   }
    //   void clear() {
    //     Arrays.fill(values, 0);
    //   }
    //   @Override
    //   public boolean equals(Object o) {
    //     if (this == o) return true;
    //     if (o == null || getClass() != o.getClass()) return false;
    //     Vector vector = (Vector) o;
    //     return Arrays.equals(values, vector.values);
    //   }
    //   @Override
    //   public int hashCode() {
    //     return Arrays.hashCode(values);
    //   }
    // }
    record KMeanRun<Element>(double[][] center, Map<Element, Integer> cluster) {}

    var coordinates =
        elements.stream().collect(Collectors.toUnmodifiableMap(x -> x, toCoordinate, (a, b) -> a));

    return IntStream.range(0, repeat)
        .mapToObj(
            ignore -> {
              var clusters = elements.stream().collect(Collectors.toMap(x -> x, x -> -1));
              var centers =
                  Utils.shuffledPermutation(elements).stream()
                      .map(toCoordinate)
                      .map(x -> Arrays.stream(x).boxed().toList())
                      .distinct()
                      .limit(k)
                      .map(x -> x.stream().mapToDouble(v -> v).toArray())
                      .toArray(double[][]::new);

              if (centers.length < k)
                throw new IllegalArgumentException(
                    "Insufficient initial cluster point: " + centers.length + " < " + k);

              int swaps;
              do {
                // initialize swap
                swaps = 0;

                // re-clustering
                for (var element : clusters.keySet()) {
                  double closestDistance = distance(coordinates.get(element), centers[0]);
                  int closestCluster = 0;

                  for (int i = 1; i < k; i++) {
                    if (closestDistance > distance(coordinates.get(element), centers[i])) {
                      closestDistance = distance(coordinates.get(element), centers[i]);
                      closestCluster = i;
                    }
                  }

                  if (closestCluster != clusters.get(element)) {
                    swaps++;
                    clusters.put(element, closestCluster);
                  }
                }

                // update coordination
                int[] count = new int[k];
                for (int i = 0; i < k; i++) Arrays.fill(centers[i], 0.0);
                clusters.forEach(
                    (element, clusterId) -> {
                      var loc = coordinates.get(element);
                      for (int i = 0; i < centers[clusterId].length; i++)
                        centers[clusterId][i] += loc[i];
                      count[clusterId]++;
                    });
                for (int i = 0; i < k; i++)
                  for (int j = 0; j < centers[i].length; j++) centers[i][j] /= count[i];
              } while (swaps > 0);

              return new KMeanRun<>(centers, clusters);
            })
        .min(
            Comparator.comparingDouble(
                kMeanRun ->
                    kMeanRun.cluster.entrySet().stream()
                        .mapToDouble(
                            e -> {
                              var clusterId = e.getValue();
                              var p0 = coordinates.get(e.getKey());
                              var p1 = kMeanRun.center()[clusterId];
                              var dis = distance(p0, p1);
                              return dis * dis;
                            })
                        .sum()))
        .map(
            kMeanRun ->
                kMeanRun.cluster.entrySet().stream()
                    .collect(
                        Collectors.groupingBy(
                            Map.Entry::getValue,
                            Collectors.mapping(Map.Entry::getKey, Collectors.toUnmodifiableList())))
                    .values())
        .orElseThrow();
  }

  private MathUtils() {}
}
