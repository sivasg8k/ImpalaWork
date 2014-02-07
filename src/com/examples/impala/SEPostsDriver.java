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




public class SEPostsDriver extends Configured implements Tool {

public static class SEPostsMap extends Mapper<LongWritable, Text, NullWritable, Text> {
        
       
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
        		            if(null != id) {
        		            	value= value + id.trim();
        		            }
        		            
        		            String postTypeId =root.getAttributeValue("PostTypeId");
        		            if(null != postTypeId) {
        		            	value= value + "," + postTypeId.trim();
        		            }
        		            
        		            String accAnsId =root.getAttributeValue("AcceptedAnswerId");
        		            if(null != accAnsId) {
        		            	value= value + "," + accAnsId.trim();
        		            }
        		            
        		            String creationDate =root.getAttributeValue("CreationDate");
        		            if(null != creationDate) {
        		            	creationDate = creationDate.replace("T", " ");
        		            	value= value + "," + creationDate.trim();
        		            }
        		            
        		            String score =root.getAttributeValue("Score");
        		            if(null != score) {
        		            	value= value + "," + score.trim();
        		            }
        		            
        		            String viewCount =root.getAttributeValue("ViewCount");
        		            if(null != viewCount) {
        		            	value= value + "," + viewCount.trim();
        		            }
        		            
        		            /*String body =root.getAttributeValue("Body");
        		            if(null != body) {
        		            	
        		            	body = body.replaceAll("\n", "");
        		            	body = body.replaceAll("\"","");
        		            	body = body.replaceAll(",","");
        		            	body = body.replaceAll("&gt;", "").replaceAll("&lt;","").replaceAll("&quot;","").replaceAll("&#xA;","").replaceAll("&#xD;","").replaceAll("&amp;","");
        		            	
        		            	value= value + "," + body.trim();
        		            }*/
        		            
        		            context.write(NullWritable.get(), new Text(value));
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
		
		Job job = new Job(conf,"ParseVotes");
		XmlInputFormat.setInputPaths(job, new Path(args[0]));
		job.setJarByClass(SEPostsDriver.class);
		job.setMapperClass(SEPostsMap.class);
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
	    int result = ToolRunner.run(new SEPostsDriver(), args);
        System.exit(result);
    }
	

}
