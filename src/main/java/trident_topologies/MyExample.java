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
         boolean cycle;
         private TopologyContext topologyContext;
         private List<List<Object>> lineFile;
         int index = 0;

         HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

         public MYFixedBatchSpout(Fields fields, int maxBatchSize) {
             this.fields = fields;
             this.maxBatchSize = maxBatchSize;
             this.cycle = false;
         }

         public void setCycle(boolean v){
             this.cycle = v;
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
                     if(str.length() !=0){
                         single_line = new ArrayList<Object>();
                         single_line.add(str);
                         lineFile.add(single_line);
                     }
                 }
                 reader.close();
                 fileReader.close();

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
             //System.out.println("BatchID = "+batchId);
             List<List<Object>> batch = this.batches.get(batchId);
             if(batch == null){
                 batch = new ArrayList<List<Object>>();
                 if(index>=lineFile.size() && cycle) {
                     //System.out.println("**************With BatchID == "+batchId+" we have reached the end of file!!!");
                     //index = 0;
                     index = 0;
                 }
                 for(int i=0; index < lineFile.size() && i < maxBatchSize; index++, i++) {
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
            //System.out.println("*******************These are the batch *******"+sentence);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }


    public static StormTopology buildTopology(LocalDRPC drpc) {

        MYFixedBatchSpout spout = new MYFixedBatchSpout(new Fields("sentence"),800);
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).name("MySpoutStream")
                .parallelismHint(1)
                .each(new Fields("sentence"), new Split(), new Fields("word")).name("Splitting")
                .groupBy(new Fields("word")).name("Grouping1")
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(20);
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
            LocalDRPC drpc = new LocalDRPC();
            System.out.println("Input File is ::::::"+args[0]);
            conf.put("inputFile", args[0]);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for (int i = 0; i < 100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("words","Beatrice Virgilio Dante Farinata Divina"));
                Thread.sleep(1000);
            }
            drpc.shutdown();

            /*
                The number of batch generated are function of the time
             */
            Thread.sleep(10000);
            cluster.killTopology("wordCounter");
            cluster.shutdown();
        }
        else {
            conf.setNumWorkers(3);
            conf.put("inputFile", args[0]);
            StormSubmitter.submitTopologyWithProgressBar(args[1], conf, buildTopology(null));
        }
    }
}
