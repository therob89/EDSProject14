package trident_topologies;

/**
 * Created by robertopalamaro on 05/03/15.
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import clojure.lang.Obj;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class MyExample {


     public static class MYFixedBatchSpout implements IBatchSpout {

         Fields fields;
         int maxBatchSize;
         private FileReader fileReader;
         boolean completed = false;
         private TopologyContext topologyContext;
         private List<List<Object>> lineFile;
         int index = 0;

         HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

         public MYFixedBatchSpout(Fields fields, int maxBatchSize) {
             this.fields = fields;
             this.maxBatchSize = maxBatchSize;
         }

         @Override
         public void open(Map conf, TopologyContext context) {
             index = 0;
             try {
                 this.topologyContext = context;
                 this.fileReader = new FileReader(conf.get("inputFile").toString());
                 this.lineFile = new ArrayList<List<Object>>();
                 String str;
                 BufferedReader reader = new BufferedReader(fileReader);
                 List<Object> single_line;
                 while ((str = reader.readLine()) != null) {
                     single_line = new ArrayList<Object>();
                     single_line.add(str);
                     lineFile.add(single_line);
                 }


             } catch (FileNotFoundException e) {
                 throw new RuntimeException("Error reading file "
                         + conf.get("inputFile"));
             }catch (Exception e) {
                 throw new RuntimeException("Error reading tuple from file", e);
             }finally {
                 System.out.println("*****Total lines: "+lineFile.size());
             }
         }
         @Override
         public void emitBatch(long batchId, TridentCollector collector) {
             List<List<Object>> batch = this.batches.get(batchId);
             if(batch == null){
                 batch = new ArrayList<List<Object>>();
                 if(index>=lineFile.size()) {
                     index = 0;
                 }
                 for(int i=0; index < lineFile.size() && i < maxBatchSize; index++, i++) {
                     System.out.println("We are adding ----------------->"+lineFile.get(index));
                     batch.add(lineFile.get(index));
                 }
                 this.batches.put(batchId, batch);
             }
             for(List<Object> list : batch){
                 collector.emit(list);
             }
         }
         @Override
         public void ack(long batchId) {
             this.batches.remove(batchId);
         }

         @Override
         public void close() {
             try {
                 fileReader.close();
             } catch (IOException e) {
                 e.printStackTrace();
             }
         }

         @Override
         public Map getComponentConfiguration() {
             Config conf = new Config();
             conf.setMaxTaskParallelism(1);
             return conf;
         }

         @Override
         public Fields getOutputFields() {
             return fields;
         }

     }


    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            System.out.println("*******************These are the batch *******"+sentence);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static class FixedBatchSpout2 implements IBatchSpout {

        Fields fields;
        List<Object>[] outputs;
        int maxBatchSize;
        HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

        public FixedBatchSpout2(Fields fields, int maxBatchSize, List<Object>... outputs) {
            this.fields = fields;
            this.outputs = outputs;
            this.maxBatchSize = maxBatchSize;
        }

        int index = 0;
        boolean cycle = false;

        public void setCycle(boolean cycle) {
            this.cycle = cycle;
        }

        @Override
        public void open(Map conf, TopologyContext context) {
            index = 0;
            System.out.println("*********************************OPEN METHOD::::");


        }

        @Override
        public void emitBatch(long batchId, TridentCollector collector) {
            List<List<Object>> batch = this.batches.get(batchId);
            if(batch == null){
                batch = new ArrayList<List<Object>>();
                if(index>=outputs.length && cycle) {
                    index = 0;
                }
                for(int i=0; index < outputs.length && i < maxBatchSize; index++, i++) {
                    System.out.println("We are adding ----------------->"+outputs[index]);
                    batch.add(outputs[index]);
                }
                this.batches.put(batchId, batch);
            }
            for(List<Object> list : batch){
                collector.emit(list);
            }
        }

        @Override
        public void ack(long batchId) {
            this.batches.remove(batchId);
        }

        @Override
        public void close() {
        }

        @Override
        public Map getComponentConfiguration() {
            Config conf = new Config();
            conf.setMaxTaskParallelism(1);
            return conf;
        }

        @Override
        public Fields getOutputFields() {
            return fields;
        }

    }


    public static StormTopology buildTopology(LocalDRPC drpc) {
        /*
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("prova prova prova abcd"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("prova prova prova abcd"));
        spout.setCycle(false);

        FixedBatchSpout2 spout = new FixedBatchSpout2(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(false);*/
        MYFixedBatchSpout spout = new MYFixedBatchSpout(new Fields("sentence"),3);
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
                .parallelismHint(16)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(16);

        /*
        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));*/
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if(args.length==0){
            System.out.println("Insert path to file to process");
            System.exit(-1);
        }
        if (args.length == 1) {
            //LocalDRPC drpc = new LocalDRPC();
            System.out.println("Input File is ::::::"+args[0]);
            conf.put("inputFile", args[0]);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(null));
            /*
            for (int i = 0; i < 3; i++) {
                long start = System.currentTimeMillis();
                System.out.println("DRPC RESULT: " + drpc.execute("words", "prova"));
                //System.out.println("Time elapsed ------->"+(System.currentTimeMillis()-start));
                Thread.sleep(1000);
            }*/
            //drpc.shutdown();
            Thread.sleep(10000);
            cluster.killTopology("wordCounter");
            cluster.shutdown();
        }
        else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[1], conf, buildTopology(null));
        }
    }
}
