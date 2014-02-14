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




public class SEUsersDriver extends Configured implements Tool {

public static class SEUsersMap extends Mapper<LongWritable, Text, NullWritable, Text> {
        
       
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
        		            String reputation =root.getAttributeValue("Reputation");
        		            String creationDate =root.getAttributeValue("CreationDate");
        		            String displayName =root.getAttributeValue("DisplayName");
        		            String lastAccessDate =root.getAttributeValue("LastAccessDate");
        		            String location =root.getAttributeValue("Location");
        		            String views =root.getAttributeValue("Views");
        		            String upVotes =root.getAttributeValue("UpVotes");
        		            String downVotes =root.getAttributeValue("DownVotes");
        		            String age =root.getAttributeValue("Age");
        		            
        		            if((null != id && !"".equalsIgnoreCase(id)) && (null != creationDate && !"".equalsIgnoreCase(creationDate)) && (null != lastAccessDate && !"".equalsIgnoreCase(lastAccessDate))) {
        		            	if(null != id && !"".equalsIgnoreCase(id)) {
            		            	value= value + id.trim();
            		            }
            		            
            		            
            		            if(null != reputation && !"".equalsIgnoreCase(reputation)) {
            		            	value= value + "," + reputation.trim();
            		            } else {
            		            	value= value + "," + "0";
            		            }
            		            
            		            
            		            if(null != creationDate && !"".equalsIgnoreCase(creationDate)) {
            		            	creationDate = creationDate.replace("T", " ");
            		            	value= value + "," + creationDate.trim();
            		            }
            		            
            		            
            		            if(null != displayName && !"".equalsIgnoreCase(displayName)) {
            		            	value= value + "," + displayName.trim();
            		            } else {
            		            	value= value + "," + "NULL";
            		            }
            		            
            		            
            		            if(null != lastAccessDate && !"".equalsIgnoreCase(lastAccessDate)) {
            		            	lastAccessDate = lastAccessDate.replace("T", " ");
            		            	value= value + "," + lastAccessDate.trim();
            		            }
            		            
            		            
            		            if(null != location && !"".equalsIgnoreCase(location)) {
            		            	
            		            	if(location.indexOf(",") != -1) {
            		            		String[] loc = location.split(",");
                		            	value= value + "," + loc[0].trim() + "," + loc[1].trim();
            		            	} else {
            		            		value= value + "," + location.trim() + ",NULL";
            		            	}
            		            } else {
            		            	value= value + "," + "NULL,NULL";
            		            }
            		            
            		            
            		            if(null != views && !"".equalsIgnoreCase(views)) {
            		            	value= value + "," + views.trim();
            		            } else {
            		            	value= value + "," + "0";
            		            }
            		            
            		            
            		            if(null != upVotes && !"".equalsIgnoreCase(upVotes)) {
            		            	value= value + "," + upVotes.trim();
            		            } else {
            		            	value= value + "," + "0";
            		            }
            		            
            		            
            		            if(null != downVotes && !"".equalsIgnoreCase(downVotes)) {
            		            	value= value + "," + downVotes.trim();
            		            } else {
            		            	value= value + "," + "0";
            		            }
            		            
            		            if(null != age && !"".equalsIgnoreCase(age)) {
            		            	value= value + "," + age.trim();
            		            } else {
            		            	value= value + "," + "0";
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
		job.setJarByClass(SEUsersDriver.class);
		job.setMapperClass(SEUsersMap.class);
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
	    int result = ToolRunner.run(new SEUsersDriver(), args);
        System.exit(result);
    }
	

}
