/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.flink.nexmark.queries;

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.example.flink.common.ConfigTool;
import org.example.flink.nexmark.sinks.DummyLatencyCountingSink;
import org.example.flink.nexmark.sources.AuctionSourceFunction;
import org.example.flink.nexmark.sources.PersonSourceFunction;
import org.example.partition.entity.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import static org.example.flink.common.ConfigTool.init;

public class Query3 {

    private static final Logger logger  = LoggerFactory.getLogger(Query3.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = init(params);
        AppConfig jobConfiguartion = (AppConfig) env.getConfig().getGlobalJobParameters();

        final int auctionSrcRate = params.getInt("auction-srcRate", 20000);

        final int personSrcRate = params.getInt("person-srcRate", 10000);

        DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionSrcRate))
                .name("Custom Source: Auctions");
        ConfigTool.configOperator(auctions, jobConfiguartion);

        DataStream<Person> persons = env.addSource(new PersonSourceFunction(personSrcRate))
                .name("Custom Source: Persons");
        ConfigTool.configOperator(persons, jobConfiguartion);

        DataStream<Person> filter = persons.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA"));
            }
        }).name("Filer");

        ConfigTool.configOperator(filter, jobConfiguartion);
        // SELECT Istream(P.name, P.city, P.state, A.id)
        // FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
        // WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA')

        KeyedStream<Auction, Long> keyedAuctions =
                auctions.keyBy(new KeySelector<Auction, Long>() {
                    @Override
                    public Long getKey(Auction auction) throws Exception {
                        return auction.seller;
                    }
                });
//        ConfigTool.configOperator(keyedAuctions, jobConfiguartion);

        KeyedStream<Person, Long> keyedPersons =
                filter.keyBy(new KeySelector<Person, Long>() {
                    @Override
                    public Long getKey(Person person) throws Exception {
                        return person.id;
                    }
                });
//        ConfigTool.configOperator(keyedPersons, jobConfiguartion);

        DataStream<Tuple4<String, String, String, Long>> joined = keyedAuctions.connect(keyedPersons)
                .flatMap(new JoinPersonsWithAuctions()).name("Incremental join");
        ConfigTool.configOperator(joined, jobConfiguartion);

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        DataStream<Object> sink = joined.transform("Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger)).name("Sink");
        ConfigTool.configOperator(sink, jobConfiguartion);

        // execute program
        env.execute("Nexmark-Query3_" + jobConfiguartion.getMode() + "_" + UUID.randomUUID());
    }

    private static final class JoinPersonsWithAuctions extends RichCoFlatMapFunction<Auction, Person, Tuple4<String, String, String, Long>> {

        // person state: id, <name, city, state>
        private HashMap<Long, Tuple3<String, String, String>> personMap = new HashMap<>();

        // auction state: seller, List<id>
        private HashMap<Long, HashSet<Long>> auctionMap = new HashMap<>();

        @Override
        public void flatMap1(Auction auction, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            // check if auction has a match in the person state
            if (personMap.containsKey(auction.seller)) {
                // emit and don't store
                Tuple3<String, String, String> match = personMap.get(auction.seller);
                out.collect(new Tuple4<>(match.f0, match.f1, match.f2, auction.id));
            }
            else {
                // we need to store this auction for future matches
                if (auctionMap.containsKey(auction.seller)) {
                    HashSet<Long> ids = auctionMap.get(auction.seller);
                    ids.add(auction.id);
                    auctionMap.put(auction.seller, ids);
                }
                else {
                    HashSet<Long> ids = new HashSet<>();
                    ids.add(auction.id);
                    auctionMap.put(auction.seller, ids);
                }
            }
        }

        @Override
        public void flatMap2(Person person, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            // store person in state
            personMap.put(person.id, new Tuple3<>(person.name, person.city, person.state));

            // check if person has a match in the auction state
            if (auctionMap.containsKey(person.id)) {
                // output all matches and remove
                HashSet<Long> auctionIds = auctionMap.remove(person.id);
                for (Long auctionId : auctionIds) {
                    out.collect(new Tuple4<>(person.name, person.city, person.state, auctionId));
                }
            }
        }
    }

}