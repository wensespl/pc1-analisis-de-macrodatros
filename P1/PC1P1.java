/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package pc1p1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * @author USUARIO
 */
public class PC1P1 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf1 = new JobConf(PC1P1.class);
        job_conf1.setJobName("PC1P1");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(DoubleWritable.class);
        job_conf1.setMapperClass(pc1p1.TMDBMapper.class);
        job_conf1.setReducerClass(pc1p1.TMDBReducer.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));
        my_client.setConf(job_conf1);
        

        try {
            JobClient.runJob(job_conf1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
