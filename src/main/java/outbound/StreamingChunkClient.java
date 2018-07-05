package outbound;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;

import dataflow.pb.Chunk;
import dataflow.pb.OutboundGrpc;
import dataflow.pb.Result;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class StreamingChunkClient {

	private final int chunkSize;
	private final ManagedChannel channel;
	private final ExecutorService chunkingService;
	
	public StreamingChunkClient(int serverPort, String serverAddress, int chunkSize) {
		System.out.println("Connecting to: " + serverAddress + ":" + serverPort);
		ManagedChannelBuilder<?> chBuilder = ManagedChannelBuilder.forAddress(serverAddress, serverPort).usePlaintext();
		this.channel = chBuilder.build();
		this.chunkingService = Executors.newFixedThreadPool(10);
		this.chunkSize = chunkSize;
	}
	
	public void sendData(final InputStream is, final CountDownLatch done) {
		chunkingService.submit(() -> {
			final Object received = new Object();
			final AtomicLong totalBytes = new AtomicLong();
			StreamObserver<Chunk> chunks = OutboundGrpc.newStub(channel).sendChunk(new StreamObserver<Result>() {
				
				
				@Override
				public void onNext(Result value) {
					long receivedBytes = value.getLength();
//					System.out.print(String.format("%d,", receivedBytes));
					if (receivedBytes == totalBytes.get()) {
						synchronized(received) {
							received.notify();
						}
					}
					// Do some sort of logging here.
				}

				@Override
				public void onError(Throwable t) {
					done.countDown();
				}

				@Override
				public void onCompleted() {
					done.countDown();
				}
			});
					
			byte[] chunk = new byte[chunkSize];
			int nRead = 0;
			try {
				while ((nRead = is.read(chunk, 0, chunkSize)) != -1) {
					chunks.onNext(Chunk.newBuilder().setData(ByteString.copyFrom(chunk, 0, nRead)).build());
					totalBytes.addAndGet(nRead);
				}
				synchronized(received) {
					try {
						received.wait();
					} catch (InterruptedException e) {
						chunks.onError(new RuntimeException("Error waiting for result", e));
					}
				}
				chunks.onCompleted();
			} catch (IOException e) {
				chunks.onError(new RuntimeException("Error reading input stream", e));
			}
		});
	}
	
	public void stop() {
		channel.shutdownNow();
		chunkingService.shutdownNow();
	}
	
}
