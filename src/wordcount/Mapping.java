package wordcount;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapping extends Mapper<Object,Text,Text,FloatWritable>{


    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        try{
            String[] columns=value.toString().split("\\t");
            Text iris=new Text(columns[4].trim());
            FloatWritable sepalLength=new FloatWritable(Float.parseFloat(columns[1].trim()));


            System.out.println(iris+"--"+sepalLength);


            context.write(iris, sepalLength);//word es lo que contiente key y one es el equivalente a 1 y va contando de 1 en 1.
        }
        catch (InterruptedException | IOException ie){
            ie.printStackTrace();
        }
    }
}
