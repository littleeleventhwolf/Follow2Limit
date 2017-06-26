import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;
import java.util.Vector;

/**
 * Created by hadoop on 17-6-25.
 */
public class FollowAnalyzer {
    public static class Job1Map extends Mapper<LongWritable, Text, Text, TextPair> {
        /**
         * map最终输出的值：
         *      key：用户的ID；
         *      value：TextPair存放好友
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //切分出两个用户id
            String[] _values = value.toString().split("\t");
            System.out.println(_values[0] + " " + _values[1]);
            TextPair pair = new TextPair();
            if(_values[0].compareTo(_values[1]) < 0) {
                pair.set(new Text(_values[0]), new Text(_values[1]));
                //context.write(new Text(_values[0]), new TextPair(new Text(_values[0]), new Text(_values[1])));
                //context.write(new Text(_values[1]), new TextPair(new Text(_values[0]), new Text(_values[1])));
            } else if(_values[0].compareTo(_values[1]) > 0) {
                pair.set(new Text(_values[1]), new Text(_values[0]));
                //context.write(new Text(_values[0]), new TextPair(new Text(_values[1]), new Text(_values[0])));
                //context.write(new Text(_values[1]), new TextPair(new Text(_values[1]), new Text(_values[0])));
            }
            context.write(new Text(_values[0]), pair);
            context.write(new Text(_values[1]), pair);
        }
    }

    public static class Job1Reduce extends Reducer<Text, TextPair, TextPair, Text> {
        @Override
        public void reduce(Text key, Iterable<TextPair> pairs, Context context) throws IOException, InterruptedException {
            Vector<String> friends = new Vector<String>();

            for(TextPair pair : pairs) {
                if(key.equals(pair.getFirst())) {
                    friends.add(pair.getSecond().toString());
                    context.write(pair, new Text("deg1_friends"));
                } else if(key.equals(pair.getSecond())) {
                    friends.add(pair.getFirst().toString());
                    context.write(pair, new Text("deg1_friends"));
                }
            }

            for(int i = 0; i < friends.size(); i++)
                System.out.print(friends.get(i) + ", ");
            System.out.println();

            for(int i = 0; i < friends.size(); i++) {
                for(int j = 0; j < friends.size(); j++) {
                    if (friends.elementAt(i).compareTo(friends.elementAt(j)) < 0) {
                        context.write(
                                new TextPair(friends.get(i), friends.get(j)),
                                new Text("deg2_friends")//潜在的二度好友
                        );
                    }
                }
            }
        }
    }

    public static class Job2Map extends Mapper<LongWritable, Text, TextPair, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineItems = value.toString().split("\t");
            System.out.println(key + "," + lineItems[0] + "," + lineItems[1] + "," + lineItems[2]);

            if(lineItems.length == 3) {
                context.write(
                        new TextPair(lineItems[0], lineItems[1]),
                        new Text(lineItems[2])
                );
            }
        }
    }

    public static class Job2Reduce extends Reducer<TextPair, Text, Text, TextPair> {
        @Override
        public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Vector<String> relationTags = new Vector<String>();

            for(Text val : values)
                relationTags.add(val.toString());

            int support = 0;//计算推荐的支持程度
            boolean isDeg2 = false;//判断两者是否是二度关系
            boolean isDeg1 = false;//判断两者是否是一度关系

            for(int i = 0; i < relationTags.size(); i++) {
                if(relationTags.elementAt(i).equals("deg1_friends")) {
                    isDeg1 = true;
                } else if(relationTags.elementAt(i).equals("deg2_friends")) {
                    isDeg2 = true;
                    support += 1;
                }
            }

            if(!isDeg1 && isDeg2) {
                context.write(new Text(String.valueOf(support)), key);
            }
        }
    }

    /**
     *  自定义Writable类型TextPair
     */
    static class TextPair implements WritableComparable<TextPair> {
        //Text类型的实例变量
        private Text first;
        //Text类型的实例变量
        private Text second;

        public TextPair() {
            set(new Text(), new Text());
        }

        public TextPair(String first, String second) {
            set(new Text(first), new Text(second));
        }

        public TextPair(Text first, Text second) {
            set(first, second);
        }

        public void set(Text first, Text second) {
            this.first = first;
            this.second = second;
        }

        public Text getFirst() {
            return first;
        }

        public Text getSecond() {
            return second;
        }

        //将对象转换成字节流并写入到输出流out中
        public void write(DataOutput dataOutput) throws IOException {
            first.write(dataOutput);
            second.write(dataOutput);
        }

        //从输入流in中读取字节流反序列化为对象
        public void readFields(DataInput dataInput) throws IOException {
            first.readFields(dataInput);
            second.readFields(dataInput);
        }

        @Override
        public int hashCode() {
            return (first + "\t" + second).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof TextPair) {
                TextPair tp = (TextPair) obj;
                return first.equals(tp.first) && second.equals(tp.second);
            }
            return false;
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }

        public int compareTo(TextPair textPair) {
            /*
            int cmp = first.compareTo(textPair.first);
            if(cmp != 0) {
                return cmp;
            }
            return second.compareTo(textPair.second);
            */
            return (first + "\t" + second).compareTo(textPair.first + "\t" + textPair.second);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        if(args.length != 3) {
            System.err.println("usage: FollowAnalyzer <hdfs_defaultFS> <inpath> <outpath>");
            System.exit(2);
        }

        conf.set("fs.defaultFS", args[0]);
        Job job1 = Job.getInstance(conf, "FollowAnalyzer");
        job1.setJarByClass(FollowAnalyzer.class);
        job1.setMapperClass(Job1Map.class);
        job1.setReducerClass(Job1Reduce.class);
        job1.setNumReduceTasks(1);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(TextPair.class);
        //定义一个临时目录，先将任务的输出结果写到临时目录中，下一个排序任务以临时目录为输入目录
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        Path tempDir = new Path("FollowAnalyzer-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        FileOutputFormat.setOutputPath(job1, tempDir);

        if(job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "FollowAnalyzer");
            job2.setJarByClass(FollowAnalyzer.class);
            job2.setMapperClass(Job2Map.class);
            job2.setReducerClass(Job2Reduce.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(TextPair.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, tempDir);
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            FileSystem.get(conf).deleteOnExit(tempDir);//退出时删除临时路径

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
