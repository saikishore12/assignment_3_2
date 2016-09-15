package display_files;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class display_file_timestamp {

	public static void main(String[] args)
			throws IOException, InterruptedException, URISyntaxException, ParseException {
		// start_ts=12/01/2016,end_ts=12/05/2016
		String start_ts = args[0];
		String end_ts = args[1];
		printFilesRecursively("hdfs://localhost:9000/", start_ts, end_ts);

	}

	public static void printFilesRecursively(String Url, String start_ts, String end_ts)
			throws IOException, ParseException {
		try {
			SimpleDateFormat format = new SimpleDateFormat("dd/MM/yy");
			Date Start_date = format.parse(start_ts);
			Date End_date = format.parse(end_ts);
			// Instantiating Distributed file system
			FileSystem fs = new DistributedFileSystem();
			// loading configuration and URI
			fs.initialize(new URI(Url), new Configuration());
			FileStatus[] status = fs.listStatus(new Path(Url));

			for (int i = 0; i < status.length; i++) {
				long modificationDate = status[i].getModificationTime();
				//converting modification time to date 
				Date date = new Date(modificationDate);
				if (date.compareTo(Start_date) > 0 && date.compareTo(End_date) < 0) {
					if (status[i].isDir())
						printFilesRecursively(status[i].getPath().toString(), start_ts, end_ts);
				} else {
					try {
						System.out.println(status[i].getPath().toString());
					} catch (Exception e) {
						System.out.println(e.toString());
					}

				}
			}
		} catch (URISyntaxException ex) {
			System.out.println(ex.toString());
		}

	}
}