import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class Checksum {
	public static void main ( String [] args ) throws Exception {
		// The system configuration
		Configuration conf = new Configuration();
		
		// Get an instance of the File system
		FileSystem fs = FileSystem.get(conf);
	
		String path_name = "/cpre419/bigdata";
		
		Path path = new Path(path_name);
	
		// The Output Data Stream to write into
		FSDataInputStream file = fs.open(path);
	
		//create an empty byte array to store the data in
		byte[] byteArr = new byte[1000];
		
		//read the file starting at position 1e9, output it into byteArr, an offset of 0 bytes, read 1000 bytes
		file.read((long) 1e9, byteArr, 0, 1000);
		
		// Close the file and the file system instance
		file.close();	
		
		byte checksum = 0;
		
		for (byte b : byteArr) {
			checksum = (byte) (checksum ^ b);
		}
		
		//set path for output file
		path_name = "/user/ethantw/lab1/checksum";
		
		path = new Path(path_name);
	
		// The Output Data Stream to write into
		FSDataOutputStream outputFile = fs.create(path);

		// Write the checksum into the output file
		outputFile.writeChars(String.format("%8s", Integer.toBinaryString(checksum & 0xFF)).replace(' ', '0'));
		
		outputFile.close();
		
		fs.close();
	}
}