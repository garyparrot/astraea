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

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.astraea.common.metrics.HasBeanObject;

public class ClusterBeanQueryBuilder<
    Bean extends HasBeanObject, GroupKey, GroupValue, QueryResult> {
  private final Class<Bean> metricClass;
  private final Set<Integer> identities;

  private ClusterBeanQuery.QueryType queryType;
  private Predicate<Bean> filter = bean -> true;
  private Comparator<Bean> comparator = Comparator.comparingInt(bean -> 0);
  private BiFunction<Integer, Bean, GroupKey> groupingKey = null;

  public ClusterBeanQueryBuilder(Class<Bean> metricClass, Set<Integer> identities) {
    this.metricClass = metricClass;
    this.identities = identities;
  }

  @SuppressWarnings("unchecked")
  public ClusterBeanQueryBuilder<Bean, GroupKey, List<Bean>, List<Bean>> useWindowQuery() {
    this.queryType = ClusterBeanQuery.QueryType.Window;
    return (ClusterBeanQueryBuilder<Bean, GroupKey, List<Bean>, List<Bean>>) this;
  }

  @SuppressWarnings("unchecked")
  public ClusterBeanQueryBuilder<Bean, GroupKey, Bean, Bean> useLatestQuery() {
    this.queryType = ClusterBeanQuery.QueryType.Latest;
    return (ClusterBeanQueryBuilder<Bean, GroupKey, Bean, Bean>) this;
  }

  public ClusterBeanQueryBuilder<Bean, GroupKey, GroupValue, QueryResult> filterByTime(
      Duration recentInterval) {
    return filterByTime(System.currentTimeMillis() - recentInterval.toMillis());
  }

  public ClusterBeanQueryBuilder<Bean, GroupKey, GroupValue, QueryResult> filterByTime(
      long sinceMs) {
    return filterBy(bean -> sinceMs <= bean.createdTimestamp());
  }

  public ClusterBeanQueryBuilder<Bean, GroupKey, GroupValue, QueryResult> filterBy(
      Predicate<Bean> filter) {
    this.filter = this.filter.and(filter);
    return this;
  }

  public ClusterBeanQueryBuilder<Bean, GroupKey, GroupValue, QueryResult> descendingOrder() {
    this.comparator = Comparator.<Bean>comparingLong(HasBeanObject::createdTimestamp).reversed();
    return this;
  }

  public ClusterBeanQueryBuilder<Bean, GroupKey, GroupValue, QueryResult> ascendingOrder() {
    this.comparator = Comparator.comparingLong(HasBeanObject::createdTimestamp);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <NewGroupKey>
      ClusterBeanQueryBuilder<Bean, NewGroupKey, GroupValue, Map<NewGroupKey, GroupValue>>
          groupingBy(BiFunction<Integer, Bean, NewGroupKey> grouping) {
    this.groupingKey = (BiFunction<Integer, Bean, GroupKey>) grouping;
    return (ClusterBeanQueryBuilder<Bean, NewGroupKey, GroupValue, Map<NewGroupKey, GroupValue>>)
        this;
  }

  public ClusterBeanQuery<Bean, GroupKey, GroupValue, QueryResult> build() {
    return new ClusterBeanQuery<>() {
      private final Class<Bean> metricClass = ClusterBeanQueryBuilder.this.metricClass;
      private final Set<Integer> identities = ClusterBeanQueryBuilder.this.identities;
      private final QueryType queryType = ClusterBeanQueryBuilder.this.queryType;
      private final Predicate<Bean> filter = ClusterBeanQueryBuilder.this.filter;
      private final Comparator<Bean> comparator = ClusterBeanQueryBuilder.this.comparator;
      private final BiFunction<Integer, Bean, GroupKey> groupingKey =
          ClusterBeanQueryBuilder.this.groupingKey;

      @Override
      public Class<Bean> metricClass() {
        return metricClass;
      }

      @Override
      public Set<Integer> queryTarget() {
        return identities;
      }

      @Override
      public QueryType queryType() {
        return queryType;
      }

      @Override
      public Predicate<Bean> filter() {
        return filter;
      }

      @Override
      public Comparator<Bean> comparator() {
        return comparator;
      }

      @Override
      public BiFunction<Integer, Bean, GroupKey> groupingKey() {
        return groupingKey;
      }
    };
  }
}
