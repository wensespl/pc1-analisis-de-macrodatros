/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author USUARIO
 */
public class PC1P2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
    double popularity;
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(";");
        
        String nombre = singleRowData[0];
        
        if(!nombre.equals("title")) {
            String fecha = singleRowData[4];
            String[] fechaData = fecha.split("-");
            int anio = Integer.parseInt(fechaData[0]);

            popularity = Double.parseDouble(singleRowData[8]);

            output.collect(new Text(Integer.toString(anio)), new Text(nombre + ";" + popularity));
        }
    }
}
