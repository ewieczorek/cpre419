import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
	public static void main ( String [] args ) throws Exception {
		// The system configuration
		Configuration conf = new Configuration();
		// Get an instance of the File system
		FileSystem fs = FileSystem.get(conf);
	
		String path_name = "/user/ethantw/lab1/newfile2";
	
		Path path = new Path(path_name);
	
		// The Output Data Stream to write into
		FSDataOutputStream file = fs.create(path);
	
		// Write some data
		file.writeChars("The second Hadoop Program!");
	
		// Close the file and the file system instance
		file.close();
		fs.close();
	}
}