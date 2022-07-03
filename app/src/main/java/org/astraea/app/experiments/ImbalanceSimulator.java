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
package org.astraea.app.experiments;

/**
 * This class is used to simulate & generate an imbalance Kafka cluster scenario.
 *
 * <p>This simulator has two stages to run:
 *
 * <ol>
 *   <li>Generate a real-world Kafka log allocation. This process required a real Kafka cluster.
 *       This simulator will attempts to create many topics with different config, the config is
 *       randomly generated based on some probability distribution. The outcome cluster will have
 *       the cluster log allocation formed by the actual Kafka logic. Which is completely up to the
 *       Kafka internal scheduling decision. After this process we will obtain the log allocation
 *       from a real cluster.
 *   <li>Randomly decide the produce/consume loading of each partition log. This process try to
 *       simulate the resource loading for each partition. This process doesn't send/request any
 *       data to the cluster. The process is completely simulated by calculation(the data-flow to
 *       the cluster with some very specific assumption).
 * </ol>
 *
 * Note that this is an initial version. The data-flow simulation part is not very realist, it
 * ignored many technical details. Also, I made many assumptions about how the Kafka cluster looks
 * like & configured. It can't simulate advanced cluster setup at this moment.
 */
public class ImbalanceSimulator {}
