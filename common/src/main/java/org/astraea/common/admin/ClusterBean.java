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
package org.astraea.common.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Lazy;
import org.astraea.common.metrics.HasBeanObject;

/** Used to get beanObject using a variety of different keys . */
public interface ClusterBean {
  ClusterBean EMPTY = ClusterBean.of(Map.of());

  static ClusterBean of(Map<Integer, Collection<HasBeanObject>> allBeans) {
    var lazyReplica =
        Lazy.of(
            () ->
                allBeans.entrySet().stream()
                    .flatMap(
                        entry ->
                            entry.getValue().stream()
                                .filter(
                                    bean ->
                                        bean.beanObject().properties().containsKey("topic")
                                            && bean.beanObject()
                                                .properties()
                                                .containsKey("partition"))
                                .map(
                                    bean ->
                                        Map.entry(
                                            TopicPartitionReplica.of(
                                                bean.beanObject().properties().get("topic"),
                                                Integer.parseInt(
                                                    bean.beanObject()
                                                        .properties()
                                                        .get("partition")),
                                                entry.getKey()),
                                            bean)))
                    .collect(
                        Collectors.groupingBy(
                            Map.Entry::getKey,
                            Collectors.mapping(
                                Map.Entry::getValue, Collectors.toUnmodifiableList())))
                    .entrySet()
                    .stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> (Collection<HasBeanObject>) entry.getValue())));

    return new ClusterBean() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> all() {
        return Collections.unmodifiableMap(allBeans);
      }

      @Override
      public Map<TopicPartitionReplica, Collection<HasBeanObject>> mapByReplica() {
        return lazyReplica.get();
      }
    };
  }

  /**
   * @return a {@link Map} collection that contains broker as key and Collection of {@link
   *     HasBeanObject} as value.
   */
  Map<Integer, Collection<HasBeanObject>> all();

  /**
   * @return a {@link Map} collection that contains {@link TopicPartitionReplica} as key and a
   *     {@link HasBeanObject} as value,note that this can only be used to get partition-related
   *     beanObjects.
   */
  Map<TopicPartitionReplica, Collection<HasBeanObject>> mapByReplica();

  @SuppressWarnings("unchecked")
  default <Bean extends HasBeanObject, Key, Value, QueryResult> QueryResult run(
      ClusterBeanQuery<Bean, Key, Value, QueryResult> query) {
    switch (query.queryType()) {
      case Latest:
        if (query.groupingKey() != null) {
          return (QueryResult)
              all().entrySet().stream()
                  .filter(e -> query.queryTarget().contains(e.getKey()))
                  .flatMap(e -> e.getValue().stream().map(bean -> Map.entry(e.getKey(), bean)))
                  .filter(e -> e.getValue().getClass() == query.metricClass())
                  .map(e -> Map.entry(e.getKey(), query.metricClass().cast(e.getValue())))
                  .filter(e -> query.filter().test(e.getValue()))
                  .collect(
                      Collectors.groupingBy(
                          e -> query.groupingKey().apply(e.getKey(), e.getValue()),
                          Collectors.mapping(
                              Map.Entry::getValue, Collectors.minBy(query.comparator()))))
                  .entrySet()
                  .stream()
                  .collect(
                      Collectors.toUnmodifiableMap(
                          Map.Entry::getKey, e -> e.getValue().orElseThrow()));
        } else {
          return (QueryResult)
              all().entrySet().stream()
                  .filter(e -> query.queryTarget().contains(e.getKey()))
                  .flatMap(e -> e.getValue().stream())
                  .filter(bean -> bean.getClass() == query.metricClass())
                  .map(bean -> query.metricClass().cast(bean))
                  .filter(query.filter())
                  .min(query.comparator())
                  .orElse(null);
        }
      case Window:
        if (query.groupingKey() != null) {
          return (QueryResult)
              all().entrySet().stream()
                  .filter(e -> query.queryTarget().contains(e.getKey()))
                  .flatMap(e -> e.getValue().stream().map(bean -> Map.entry(e.getKey(), bean)))
                  .filter(e -> e.getValue().getClass() == query.metricClass())
                  .map(e -> Map.entry(e.getKey(), query.metricClass().cast(e.getValue())))
                  .filter(e -> query.filter().test(e.getValue()))
                  .sorted(Map.Entry.comparingByValue(query.comparator()))
                  .collect(
                      Collectors.groupingBy(
                          e -> query.groupingKey().apply(e.getKey(), e.getValue()),
                          Collectors.mapping(
                              Map.Entry::getValue, Collectors.toUnmodifiableList())));
        } else {
          return (QueryResult)
              all().entrySet().stream()
                  .filter(e -> query.queryTarget().contains(e.getKey()))
                  .flatMap(e -> e.getValue().stream())
                  .filter(bean -> bean.getClass() == query.metricClass())
                  .map(bean -> query.metricClass().cast(bean))
                  .filter(bean -> query.filter().test(bean))
                  .sorted(query.comparator())
                  .collect(Collectors.toUnmodifiableList());
        }
      default:
        throw new UnsupportedOperationException();
    }
  }

  static void shit() {}
}
