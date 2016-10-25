package org.opendaylight.lispflowmapping.poc.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by sheikahm on 10/24/16.
 */
public class HelloWorldClient {
    private static final Logger logger = Logger.getLogger(HelloWorldClient.class.getName());

    private final ManagedChannel blockingChannel;
    private final ManagedChannel nonBlockingChannel;
    private final GreeterGrpc.GreeterBlockingStub blockingStub;
    private final GreeterGrpc.GreeterFutureStub futureStub;

    private final AtomicInteger successCounter = new AtomicInteger();
    private final AtomicInteger failureCount = new AtomicInteger();

    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public HelloWorldClient(String host, int port) {
        blockingChannel = ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build();

        blockingStub = GreeterGrpc.newBlockingStub(blockingChannel);

        nonBlockingChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();

        futureStub = GreeterGrpc.newFutureStub(nonBlockingChannel);
    }

    public void shutdown() throws InterruptedException {
        blockingChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        nonBlockingChannel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
    }

    public void greet(String username) {
        HelloRequest request = HelloRequest.newBuilder()
                .setName(username)
                .build();
//        blockingMock(request);
        nonBlockingMock(request);
    }

    private void blockingMock(HelloRequest request) {
        int i;

        long start = System.currentTimeMillis();
        long firstEnd = 0;
        for(i=1; i <= 100000; i++) {
            blockingStub(request);
            if(i == 1) {
                firstEnd = System.currentTimeMillis();
            }
        }
        long end = System.currentTimeMillis();
        logger.info("**********************************************************");
        logger.info("Blocking");
        logger.info("Completion time        : " + (end - start)/1000.0);
        logger.info("First completion time  : " + (firstEnd - start)) ;
        logger.info("Avg completion time  : " + (end-start) / 100000.0) ;
        logger.info("**********************************************************");
    }

    private void nonBlockingMock(HelloRequest request) {
        int i;

        long start = System.currentTimeMillis();
        //long firstEnd = 0;

        int times = 10;
        for(i=1; i <= times; i++) {
            nonBlockingStub(request);
        }

        while(failureCount.get() + successCounter.get() != times) {

        }
        long end = System.currentTimeMillis();
        logger.info("**********************************************************");
        logger.info("Non Blocking");
        logger.info("Completion time        : " + (end - start)/1000.0);
        //logger.info("First completion time  : " + (firstEnd - start)) ;
        logger.info("Avg completion time  : " + (end-start) / 100000.0) ;
        logger.info("**********************************************************");
    }

    private void blockingStub(HelloRequest request) {
        HelloReply response;
        try {
            response = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
//        logger.info("Greeting: " + response.getMessage());
    }

    private void nonBlockingStub(HelloRequest request) {
        HelloReply response;
        successCounter.set(0);
        failureCount.set(0);
        nonBLockingStubWorker(request, successCounter, failureCount);
    }

    private void nonBLockingStubWorker(HelloRequest request, final AtomicInteger successCounter, final AtomicInteger failureCount) {
        try {
            Futures.addCallback(futureStub.sayHello(request), new FutureCallback<HelloReply>() {
                public void onSuccess(@Nullable HelloReply helloReply) {
                    successCounter.incrementAndGet();
                    //logger.info("Y");
                }

                public void onFailure(Throwable throwable) {
                    failureCount.incrementAndGet();
                    logger.info("Error: " + throwable.getMessage());
                }
            });
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
        }
    }

    public static void main(String[] args) throws Exception {
        int times = 1;
        while(times-->0) {
            HelloWorldClient client = new HelloWorldClient("localhost", 50051);
            try {
                String user = "Pong";
                if (args.length > 0) {
                    user = args[0]; /* Use the arg as the name to greet if provided */
                }
                client.greet(user);
            } finally {
                client.shutdown();
            }
        }
    }

}
