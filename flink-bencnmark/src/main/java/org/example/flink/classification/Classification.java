/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter, Christoph Boden
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.example.flink.classification;


import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Classification {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> conditionalInput = env.readTextFile(Config.pathToConditionals());
        DataSource<String> sumInput = env.readTextFile(Config.pathToSums());

        DataSet<Tuple3<String, String, Long>> conditionals = conditionalInput.map(new ConditionalReader());
        DataSet<Tuple2<String, Long>> sums = sumInput.map(new SumReader());

        DataSource<String> testData = env.readTextFile(Config.pathToTestSet());

        DataSet<Tuple3<String, String, Double>> classifiedDataPoints = testData.map(new Classifier())
                .withBroadcastSet(conditionals, "conditionals")
                .withBroadcastSet(sums, "sums");

        classifiedDataPoints.writeAsCsv(Config.pathToOutput(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);
        classifiedDataPoints.print();
        env.execute();
    }

    public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Long>> {

        @Override
        public Tuple3<String, String, Long> map(String s) throws Exception {
            String[] elements = s.split("\t");
            return new Tuple3<String, String, Long>(elements[0], elements[1], Long.parseLong(elements[2]));
        }
    }

    public static class SumReader implements MapFunction<String, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(String s) throws Exception {
            String[] elements = s.split("\t");
            return new Tuple2<String, Long>(elements[0], Long.parseLong(elements[1]));
        }
    }


    public static class Classifier extends RichMapFunction<String, Tuple3<String, String, Double>>  {

        private final Map<String, Map<String, Long>> wordCounts = Maps.newHashMap();
        private final Map<String, Long> wordSums = Maps.newHashMap();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            List<Tuple3<String,String,Long>> broadcastSet1 = getRuntimeContext().getBroadcastVariable("conditionals");
            List<Tuple2<String,Long>> broadcastSet2 = getRuntimeContext().getBroadcastVariable("sums");
            Iterator i2 = broadcastSet2.iterator();
            while(i2.hasNext()){
                Tuple2<String,Long> tx = (Tuple2<String,Long>) i2.next();
                Iterator i = broadcastSet1.iterator();
                Map<String,Long> subMap = Maps.newHashMap();
                while(i.hasNext()){
                    Tuple3<String,String,Long> ty = (Tuple3<String, String, Long>) i.next();
                    String compTx = tx.f0;
                    String compTy = ty.f0;
                    if(compTx.equals(compTy)){
                        subMap.put(ty.f1,ty.f2);
                    }
                }
                wordCounts.put(tx.f0,subMap);
                wordSums.put(tx.f0,tx.f1);
            }
        }

        @Override
        public Tuple3<String, String, Double> map(String line) throws Exception {

            String[] tokens = line.split("\t");
            String label = tokens[0];
            String[] terms = tokens[1].split(",");
            String predictionLabel = "";
            double maxProbability = Double.NEGATIVE_INFINITY;
            double exp_max_prob = 0.0;
            Set<String> cat = wordSums.keySet();
            Iterator<String> x = cat.iterator();
            while (x.hasNext()){
                double sum = 0.0;
                String temp = x.next();
                Map<String,Long> word = wordCounts.get(temp);
                for(String term:terms){
                    Long l = word.get(term);
                    if(l!=null){
                        sum=sum+Math.log((l+Config.getSmoothingParameter()));
                    }
                    else{
                        sum+=Math.log(Config.getSmoothingParameter());
                    }
                }
                double denom = wordSums.get(temp);
                sum = sum - terms.length*Math.log(denom+0.0004);
                if(sum>maxProbability){
                    maxProbability = sum;
                    predictionLabel = temp;
                    exp_max_prob = Math.exp(maxProbability);
                }
            }
            maxProbability=exp_max_prob;
            return new Tuple3<String, String, Double>(label, predictionLabel, maxProbability);
        }
    }

}
