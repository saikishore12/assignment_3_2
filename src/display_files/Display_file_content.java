package display_files;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Display_file_content {

	public static void main(String[] args) throws Exception {
		// to take HDFS path as argument
		String uri = args[0];
		// loading configuration
		Configuration conf = new Configuration();
		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;

		try {
			// Open the path mentioned in HDFS
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println("End Of file");
		} finally {
			IOUtils.closeStream(in);
		}
	}
}