package display_files;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class Copyfilesfromlocal {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();	
		//FileSystem fs=FileSystem.get(conf);
		FileSystem fs = new DistributedFileSystem();
		// loading configuration and URI
		fs.initialize(new URI("hdfs://localhost:9000"), conf);
		//copying files from local to HDFS location
	 fs.copyFromLocalFile(new Path("/home/acadgild/Desktop/sample/copylocal.txt"),new Path("/user/acadgild/hadoop/"));
		
	}
}
