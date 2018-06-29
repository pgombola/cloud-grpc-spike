package outbound;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import dataflow.pb.Accept;
import dataflow.pb.Chunk;
import dataflow.pb.Flow;
import dataflow.pb.OutboundGrpc.OutboundImplBase;
import dataflow.pb.Payload;
import dataflow.pb.Payload.Meta;
import dataflow.pb.Result;
import dataflow.pb.Transfer;
import dataflow.pb.Transfer.Status;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class OutboundVLServer {

	private Server grpcServer;

	public OutboundVLServer(int serverPort) {
		ServerBuilder<?> srvBuilder = ServerBuilder.forPort(serverPort);
		this.grpcServer = srvBuilder.addService(new OutboundService()).build();
	}

	public Server start() throws IOException {
		grpcServer.start();
		System.out.println("*** started outbound server");
		return grpcServer;
	}

	public void stop() throws InterruptedException {
		Server s = grpcServer;
		if (s == null) {
			throw new IllegalStateException("server already stopped");
		}
		grpcServer = null;
		s.shutdown();
		if (s.awaitTermination(1, TimeUnit.SECONDS)) {
			return;
		}
		s.shutdownNow();
		if (s.awaitTermination(1, TimeUnit.SECONDS)) {
			return;
		}
		throw new RuntimeException("unable to shutdown server");
	}

	public void blockUntilShutdown() throws InterruptedException {
		if (grpcServer != null) {
			grpcServer.awaitTermination();
		}
	}

	private static final class OutboundService extends OutboundImplBase {

		@Override
		public StreamObserver<Chunk> sendChunk(final StreamObserver<Result> responseObserver) {
			return new StreamObserver<Chunk>() {

				long totalBytes = 0;
				
				@Override
				public void onNext(Chunk value) {
					totalBytes += value.getData().size();
					responseObserver.onNext(Result.newBuilder().build());
				}

				@Override
				public void onError(Throwable t) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void onCompleted() {
					System.out.println(String.format("Received %d bytes", totalBytes));
					responseObserver.onCompleted();
				}
			};
		}

		@Override
		public void initOutboundTransfer(Flow request, StreamObserver<Accept> responseObserver) {
			int jobId = request.getJobId();
			int endpointId = request.getEndpointId();
			System.out.println(String.format("*** received initTransfer jobId = %d;enpdointId=%d", jobId, endpointId));
			// Validate these somehow.
			int rando = new Random().nextInt(Integer.MAX_VALUE);
			String sessionId = String.format("%d-%d-%d", jobId, endpointId, rando);
			
			
			responseObserver.onNext(
					Accept.newBuilder()
						.setFlow(Flow.newBuilder()
								.setJobId(jobId)
								.setSessionId(sessionId)
								.build())
						.setStatusValue(new Random().nextInt(Accept.InitStatus.values().length))
						.build());
			responseObserver.onCompleted();
		}

		@Override
		public void sendPayload(Payload request, StreamObserver<Transfer> responseObserver) {
			StringBuilder log = new StringBuilder("*** received payload:\n");
			Meta meta = request.getMetadata();
//			log.append(String.format("\tjobId=%d\n", meta.getJobId()));
//			log.append("\tHeaders:\n");
//			for (Map.Entry<String, String> entry : meta.getHeadersMap().entrySet()) {
//				log.append(String.format("\t\t%s=%s\n", entry.getKey(), entry.getValue()));
//			}
//			
//			// Get the data size transferred
//			try (BufferedInputStream is = new BufferedInputStream(request.getData().newInput())) {	
//				byte[] buf = new byte[16384];
//				log.append("\tData:\n");
//				int total = 0;
//				int nRead = 0;
//				while ((nRead = is.read(buf)) != -1) {
//					total += nRead;
//				}
//				log.append(String.format("\t\tlength=%d\n", total));
//			} catch(IOException e) {
//				responseObserver.onError(new Exception("Error reading payload", e));
//			}
			System.out.println(log.toString());
			
			// Return a random status
			Status status = Status.forNumber(new Random().nextInt(2));
			responseObserver.onNext(Transfer.newBuilder().setJobId(meta.getJobId()).setStatus(status).build());
			responseObserver.onCompleted();
		}

	}

}
