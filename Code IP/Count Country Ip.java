package cl.ucn.disc.bigdata.code;

import java.io.IOException;
import java.io.InputStream;

import com.maxmind.db.CHMCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import java.io.File;
import java.net.InetAddress;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
//import org.apache.log4j.Logger;

public class TempCount {

    //private static final Logger log = Logger.getLogger(TempCount.class);
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private static Text result_key = new Text();
        private static DatabaseReader reader;
        private static InputStream is;

        @Override
        protected void setup(Context context) throws IOException {
            is = Main.class.getResource("/GeoLite2-City.mmdb").openStream();
            reader = new DatabaseReader.Builder(is).withCache(new CHMCache()).build();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String ip = value.toString().split(" ")[0];
                InetAddress ipAddress = InetAddress.getByName(ip);
                CityResponse response = reader.city(ipAddress);
                City city = response.getCity();
                Country country = response.getCountry();
                if (country.getName() != null) {
                    result_key.set(country.getName()+"|---|"+city.getName());
                    context.write(result_key, one);
                }
            }
            catch (GeoIp2Exception exception)
            {
                System.out.println(exception.getMessage());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException {
            is.close();
            reader.close();
        }
    }


    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TempCount");

        job.setJarByClass(TempCount.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
