package com.examples.impala;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;




public class SECommentsDriver extends Configured implements Tool {

public static class SECommentsMap extends Mapper<LongWritable, Text, NullWritable, Text> {
        
       
        public void map(LongWritable key, Text value1,Context context)

        		throws IOException, InterruptedException {

        		                String xmlString = value1.toString();
        		             
        		             SAXBuilder builder = new SAXBuilder();
        		            Reader in = new StringReader(xmlString);
        		    String value="";
        		        try {
        		           
        		            Document doc = builder.build(in);
        		            Element root = doc.getRootElement();
        		           
        		            String id =root.getAttributeValue("Id");
        		            String postId =root.getAttributeValue("PostId");
        		            String score =root.getAttributeValue("Score");
        		            //String text =root.getAttributeValue("Text");
        		            String creationDate =root.getAttributeValue("CreationDate");
        		            String userId =root.getAttributeValue("UserId");
        		            
        		            if((null != id && !"".equalsIgnoreCase(id)) && (null != postId && !"".equalsIgnoreCase(postId)) && (null != creationDate && !"".equalsIgnoreCase(creationDate))) {
        		            	if(null != id && !"".equalsIgnoreCase(id)) {
            		            	value= value + id.trim();
            		            }
            		            
            		            if(null != postId && !"".equalsIgnoreCase(postId)) {
            		            	value= value + "," + postId.trim();
            		            }
            		            
            		            if(null != score && !"".equalsIgnoreCase(score)) {
            		            	value= value + "," + score.trim();
            		            } else {
            		            	value= value + "," + "0";
            		            }
            		            
            		            
            		           /* if(null != text && !"".equalsIgnoreCase(text)) {
            		            	text = text.replaceAll("\n", "");
            		            	text = text.replaceAll("\"","");
            		            	text = text.replaceAll(",","");
            		            	text = text.replaceAll("&gt;", "").replaceAll("&lt;","").replaceAll("&quot;","").replaceAll("&#xA;","").replaceAll("&#xD;","").replaceAll("&amp;","");
            		            	value= value + "," + text.trim();
            		            } else {
            		            	value= value + "," + "NULL";
            		            }*/
            		            
            		            
            		            if(null != creationDate && !"".equalsIgnoreCase(creationDate)) {
            		            	creationDate = creationDate.replace("T", " ");
            		            	value= value + "," + creationDate.trim();
            		            }
            		            
            		            if(null != userId && !"".equalsIgnoreCase(userId)) {
            		            	value= value + "," + userId.trim();
            		            } else {
            		            	value= value + "," + "-1";
            		            }
            		            context.write(NullWritable.get(), new Text(value));
        		            }
        		            
        		            
        		        } catch (JDOMException ex) {
        		            //Logger.getLogger(SECommentsMap.class.getName()).log(Level.SEVERE, null, ex);
        		        } catch (IOException ex) {
        		          //  Logger.getLogger(SECommentsMap.class.getName()).log(Level.SEVERE, null, ex);
        		        }
        		   
        		    }
	}
   
    
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		conf.set("xmlinput.start","<row");
		conf.set("xmlinput.end","/>");
		
		Job job = new Job(conf,"ParseComments");
		XmlInputFormat.setInputPaths(job, new Path(args[0]));
		job.setJarByClass(SECommentsDriver.class);
		job.setMapperClass(SECommentsMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		
		if (dfs.exists(outPath)) {
		dfs.delete(outPath, true);
		}

		job.waitForCompletion(true);
		boolean success = job.waitForCompletion(true);
	    return success ? 0: 1;
	    
		
    }
	   
	    /**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    int result = ToolRunner.run(new SECommentsDriver(), args);
        System.exit(result);
    }
	

}
