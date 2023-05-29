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

import java.time.Duration;
import java.util.Arrays;
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

  public static <Element> List<List<Element>> kMeans(
      int k, int repeat, List<Element> elements, Function<Element, double[]> toCoordinate) {
    record Vector(double[] values) {
      static double distance(Vector a, Vector b) {
        return MathUtils.distance(a.values, b.values);
      }

      void add(Vector that) {
        for (int i = 0; i < that.values.length; i++) this.values[i] += that.values[i];
      }

      void divide(double value) {
        for (int i = 0; i < this.values.length; i++) this.values[i] /= value;
      }

      void clear() {
        Arrays.fill(values, 0);
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vector vector = (Vector) o;
        return Arrays.equals(values, vector.values);
      }

      @Override
      public int hashCode() {
        return Arrays.hashCode(values);
      }
    }
    record KMeanRun<Element>(Vector[] center, Map<Element, Integer> cluster) {}

    var coordinates =
        elements.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    x -> x, toCoordinate.andThen(Vector::new), (a, b) -> a));

    return IntStream.range(0, repeat)
        .parallel()
        .mapToObj(
            ignore -> {
              long l = System.nanoTime();
              var clusters = elements.stream().collect(Collectors.toMap(x -> x, x -> -1));
              var centers =
                  Utils.shuffledPermutation(elements).stream()
                      .map(toCoordinate)
                      .map(array -> Arrays.copyOf(array, array.length))
                      .map(Vector::new)
                      .limit(k)
                      .toArray(Vector[]::new);

              if (centers.length < k)
                throw new IllegalArgumentException(
                    "Insufficient initial cluster point: " + centers.length + " < " + k);

              int swaps;
              do {
                // initialize swap
                swaps = 0;

                // re-clustering
                for (var element : clusters.keySet()) {
                  var vec = coordinates.get(element);
                  int closestCluster =
                      IntStream.range(0, k)
                          .boxed()
                          .min(
                              Comparator.comparingDouble(
                                  index -> Vector.distance(vec, centers[index])))
                          .orElseThrow();

                  if (closestCluster != clusters.get(element)) {
                    swaps++;
                    clusters.put(element, closestCluster);
                  }
                }

                // update coordination
                int[] count = new int[k];
                for (int i = 0; i < k; i++) centers[i].clear();
                clusters.forEach(
                    (element, clusterId) -> {
                      centers[clusterId].add(coordinates.get(element));
                      count[clusterId]++;
                    });
                for (int i = 0; i < k; i++) centers[i].divide(count[i]);
              } while (swaps > 0);

              long t = System.nanoTime();
              System.out.println("KMeans: " + Duration.ofNanos(t - l).toMillis() + " ms");
              return new KMeanRun<>(centers, clusters);
            })
        // select the one with minimum total variance
        .min(
            Comparator.comparingDouble(
                kMeanRun ->
                    kMeanRun.cluster.entrySet().stream()
                        .mapToDouble(
                            e -> {
                              var clusterId = e.getValue();
                              var p0 = coordinates.get(e.getKey());
                              var p1 = kMeanRun.center()[clusterId];
                              var dis = Vector.distance(p0, p1);
                              return dis * dis;
                            })
                        .sum()))
        // wrap result into return format
        .map(
            kMeanRun ->
                kMeanRun.cluster.entrySet().stream()
                    .collect(
                        Collectors.groupingBy(
                            Map.Entry::getValue,
                            Collectors.mapping(Map.Entry::getKey, Collectors.toUnmodifiableList())))
                    .values())
        .map(List::copyOf)
        .orElseThrow();
  }

  private MathUtils() {}
}
