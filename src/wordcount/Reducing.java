package wordcount;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducing extends Reducer<Text,FloatWritable,Text,FloatWritable>{

    public void reduce(Text key,Iterable<FloatWritable>  values,Context context)throws IOException,InterruptedException{

        Float min=Float.MAX_VALUE;
        Float max=Float.MIN_VALUE;
        Float avg=0f,sum=0f;
        int c=0;
        try{
            for(FloatWritable value: values){
                System.out.println(key+"\t"+value);
                float v=value.get();
                min=Math.min(min,v);
                max=Math.max(max,v);
                sum+=v;
                c++;
            }
            avg=sum/c;
            context.write(new Text(key.toString()+"\t_min"),new FloatWritable(min));
            context.write(new Text(key.toString()+"\t_max"),new FloatWritable(max));
            context.write(new Text(key.toString()+"\t_avg"),new FloatWritable(avg));
        }
        catch (InterruptedException | IOException ie){
            ie.printStackTrace();
        }

    }
}
