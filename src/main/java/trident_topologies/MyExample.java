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
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MyExample {

     public static class MYFixedBatchSpout implements IBatchSpout {

         Fields fields;
         List<Object>[] outputs;
         int maxBatchSize;
         long init_time;
         HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
         int myCounter;
         public MYFixedBatchSpout(Fields fields, int maxBatchSize, List<Object>... outputs) {
             this.fields = fields;
             this.outputs = outputs;
             this.maxBatchSize = maxBatchSize;
             myCounter = 0;
             init_time = System.nanoTime();
         }

         int index = 0;
         boolean cycle = false;

         public void setCycle(boolean cycle) {
             this.cycle = cycle;
         }

         @Override
         public void open(Map conf, TopologyContext context) {
             index = 0;
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
                     batch.add(outputs[index]);
                 }
                 this.batches.put(batchId, batch
                 );
             }
             for(List<Object> list : batch){
                 collector.emit(list);
             }
         }

         @Override
         public void ack(long batchId) {
             myCounter+=1;
             long now = System.nanoTime();
             System.out.print("******************RECEIVED ACK*******::counter is"+myCounter);
             if(now-init_time > 1){
                 System.out.println("Time window elapsed --> "+myCounter/10 +"tuples/ms");
                 init_time = now;

             }
             else{
                 System.out.println("");
             }
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
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        MYFixedBatchSpout spout = new MYFixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("prova prova prova abcd"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("prova prova prova abcd"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("prova prova prova abcd"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("prova prova prova abcd"),
                new Values("to be or not to be the person"));
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();

        TridentState wordCounts = topology.newStream("spout1", spout)
                .parallelismHint(16)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(16);

        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for (int i = 0; i < 3; i++) {
                long start = System.currentTimeMillis();
                System.out.println("DRPC RESULT: " + drpc.execute("words", "prova"));
                System.out.println("Time elapsed ------->"+(System.currentTimeMillis()-start));
                Thread.sleep(1000);
            }
            drpc.shutdown();
            cluster.shutdown();
        }
        else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        }
    }
}
