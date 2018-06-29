package outbound;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;

import dataflow.pb.Accept;
import dataflow.pb.Flow;
import dataflow.pb.OutboundGrpc;
import dataflow.pb.OutboundGrpc.OutboundBlockingStub;
import dataflow.pb.OutboundGrpc.OutboundFutureStub;
import dataflow.pb.Payload;
import dataflow.pb.Payload.Meta;
import dataflow.pb.Transfer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class OutboundAgentClient {
	
	private static final Semaphore limiter = new Semaphore(25);
	
	private final String clientName = "";
	private final OutboundBlockingStub blocking;
	private final OutboundFutureStub future;
	private final ManagedChannel channel;
	
	public OutboundAgentClient(int serverPort) {
		ManagedChannelBuilder<?> chBuilder = ManagedChannelBuilder.forAddress("ec2-54-236-236-0.compute-1.amazonaws.com", serverPort).usePlaintext();
		this.channel = chBuilder.build();
		this.blocking = OutboundGrpc.newBlockingStub(channel);
		this.future = OutboundGrpc.newFutureStub(channel);
	}
	
	public void beginDataFlow_Blocking() {
		int jobId = new Random().nextInt(Integer.MAX_VALUE);
		int endpointId = new Random().nextInt(Integer.MAX_VALUE);
		
		Flow initReq = Flow.newBuilder()
				.setJobId(jobId)
				.setEndpointId(endpointId)
				.build();
		Accept reply = blocking.initOutboundTransfer(initReq);
		
		
		System.out.println(String.format("*** received init reply jobId=%d;sessionId=%s;clientName=%s", reply.getFlow().getJobId(), reply.getFlow().getSessionId(), clientName));
		
		Map<String, String> headers = Maps.newHashMap();
		headers.put("Content-type", "application/json");
		headers.put("AS2To", "abc");
		headers.put("AS2From", "xyz");
		headers.put("Client-name", clientName);
		Payload.Meta meta = Payload.Meta.newBuilder().setJobId(jobId).putAllHeaders(headers).build();
		Payload payloadReq = Payload.newBuilder().setMetadata(meta).setData(ByteString.copyFromUtf8("da da da data")).build();
		
		Transfer payloadReply =  blocking.sendPayload(payloadReq);
		System.out.println(String.format("*** received payload reply jobId=%s;status=%s;clientName=%s", payloadReply.getJobId(), payloadReply.getStatus().toString(), clientName));
	}
	
	public void initDataFlow_Future(DataFlow... dataFlows) {
		try {
			limiter.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		final int jobId = new Random().nextInt(Integer.MAX_VALUE);
		final int endpointId = new Random().nextInt(Integer.MAX_VALUE);
		ListenableFuture<Accept> res = future.initOutboundTransfer(
			Flow.newBuilder()
				.setJobId(jobId)
				.setEndpointId(endpointId)
				.build());
		
		res.addListener(() -> limiter.release(), MoreExecutors.directExecutor());
		Futures.addCallback(
				res, 
				new FutureCallback<Accept>() {

					@Override
					public void onSuccess(Accept reply) {
						int jobId = reply.getFlow().getJobId();
						String sessionId = reply.getFlow().getSessionId();
						System.out.println(String.format("*** received init reply jobId=%d;sessionId=%s", jobId, sessionId));
						
						sendTransfer_Future(jobId, dataFlows);
					}

					@Override
					public void onFailure(Throwable t) {
						// TODO Auto-generated method stub

					}
				},
				MoreExecutors.directExecutor());
	}
	
	public void justData(int chunkSize, CountDownLatch done, InputStream is) throws IOException {	
		byte[] chunk = new byte[chunkSize];
		int nRead = 0;
		while (is.read(chunk, nRead, chunkSize) != -1) {
			ListenableFuture<Transfer> res = future.sendPayload(Payload.newBuilder()
					.setData(ByteString.copyFrom(chunk))
					.build());
			
			Futures.addCallback(res, new FutureCallback<Transfer>() {

				@Override
				public void onSuccess(Transfer result) {
					done.countDown();
				}

				@Override
				public void onFailure(Throwable t) {
					t.printStackTrace();
					done.countDown();
				}
			});
		}
	}
	
	public void sendTransfer_Future(int jobId, DataFlow... dataFlows) {		
		Map<String, String> headers = Maps.newHashMap();
		headers.put("AS2To", "abc");
		headers.put("AS2From", "xyz");
		headers.put("Client-name", clientName);
		Meta metadata = Meta.newBuilder().putAllHeaders(headers).setJobId(jobId).build();
		
		for (DataFlow df : dataFlows) {
			ListenableFuture<Transfer> res = null;
			try {
				limiter.acquire();
				res = future.sendPayload(Payload.newBuilder()
						.setMetadata(metadata)
						.setContentType(df.contentType)
						.setFilename(df.filename)
						.setData(ByteString.readFrom(df.data))
						.build());
			} catch (IOException e) {
				throw new RuntimeException("DataFlowException", e);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
			
			res.addListener(() -> limiter.release(), MoreExecutors.directExecutor());
			Futures.addCallback(
					res, 
					new FutureCallback<Transfer>() {

						@Override
						public void onSuccess(Transfer result) {
							System.out.println(
								String.format("*** received payload result (jobId=%d;status=%s)", 
									result.getJobId(), result.getStatus()));
						}

						@Override
						public void onFailure(Throwable t) {
							
						}
					},
					MoreExecutors.directExecutor());
		}
	}

	public void stop() {
		channel.shutdownNow();
	}
	
	public static final class DataFlow {
		
		public final String filename;
		public final String contentType;
		public final InputStream data;
		
		public DataFlow(String filename, String contentType, InputStream data) {
			this.filename = filename;
			this.contentType = contentType;
			this.data = data;
		}
		
	}
	
}
