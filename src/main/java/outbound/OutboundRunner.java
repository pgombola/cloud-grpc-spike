package outbound;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class OutboundRunner {
	
	private static final int chunkSize = 3 * 1024 * 1024;
	
	private static final int GB = 1024 * 1024 * 1024;
	private static final long KB = 1024;

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options
			.addOption(
					Option.builder()
						.longOpt("server")
						.desc("run in server mode")
						.build())
			.addOption(
					Option.builder()
						.longOpt("client")
						.desc("run in client mode")
						.build())
			.addOption(
					Option.builder("p")
						.longOpt("port")
						.hasArg()
						.type(Integer.class)
						.argName("int")
						.desc("server port to bind")
						.required()
						.build())
			.addOption(
					Option.builder("a")
					.longOpt("addr")
					.hasArg()
					.required()
					.argName("string")
					.type(String.class)
					.desc("server address to connect to")
					.build());
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		
		int serverPort = Integer.parseInt(cmd.getOptionValue("p"));
		String serverAddress = cmd.getOptionValue("addr");
		OutboundVLServer server = null;
		if (cmd.hasOption("server")) {
			server = new OutboundVLServer(serverPort);
			server.start();
		}
		if (cmd.hasOption("client")) {
			StreamingChunkClient client = new StreamingChunkClient(serverPort, serverAddress, chunkSize);
			for (int i = 10; i < GB; i=i*2) {
				long dataSize = i * KB;
				System.out.print(String.format("%d,",dataSize));
				CountDownLatch done = new CountDownLatch(1);
				DataInputStream data = new DataInputStream(new Random().longs(dataSize), dataSize);
				new Thread(data.runnable()).start();
				long start = System.currentTimeMillis();
				client.sendData(data, done);
				done.await();
				System.out.println(System.currentTimeMillis() - start);
			}
			client.stop();
		}
		if (server != null) {
			server.blockUntilShutdown();
		}
	}
				
	private static final class DataInputStream extends InputStream {
		
		private final LongStream data;
		private final long length;
		private final BlockingQueue<Integer> read;
		private long count;
		
		public DataInputStream(LongStream data, long length) {
			this.read = new ArrayBlockingQueue<>(chunkSize / 2);
			this.data = data;	
			this.length = length;
			this.count = 0;
		}
		
		public Runnable runnable() {
			return () -> {
				data.forEach(i -> {
					try {
						read.put((int) i);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				});
			};
		}

		@Override
		public int read() throws IOException {
			try {
				if (count < length) {
					count++;
					return read.take();
				}
				return -1;
			} catch (InterruptedException e) {
				throw new RuntimeException("Error reading data", e);
			}
		}
		
	}

}
