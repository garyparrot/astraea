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

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class MathUtilsTest {

  @Test
  void testDistance() {
    var a = new double[] {1.5, 6.7, 12};
    var b = new double[] {-6.5, -55.7, 92};
    var distance = MathUtils.distance(a, b);

    var expected = 101.773080;
    var actual = ((int) (distance * 1e6)) / 1e6;
    Assertions.assertEquals(expected, actual);
  }

  @RepeatedTest(100)
  void testKMeans() {
    var input =
        List.of(
            // cluster A
            new double[] {1.0, 1.0},
            new double[] {1.5, 1.5},
            new double[] {1.2, 1.2},
            // cluster B
            new double[] {5.3, 4.0},
            new double[] {5.3, 5.9},
            new double[] {4.6, 4.4},
            // cluster C
            new double[] {9.0, 8.0},
            new double[] {8.0, 8.5},
            new double[] {7.5, 8.5});

    var clusters = List.copyOf(MathUtils.kMeans(3, 20, input, x -> x));

    Assertions.assertEquals(3, clusters.size());
    Assertions.assertEquals(3, clusters.get(0).size());
    Assertions.assertEquals(3, clusters.get(1).size());
    Assertions.assertEquals(3, clusters.get(2).size());
  }
}
