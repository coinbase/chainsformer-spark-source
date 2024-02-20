package org.apache.arrow.flight.spark.datasource;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

public class ClientIDInterceptor implements ClientInterceptor {
    static final Metadata.Key<String> CSF_CLIENT_ID_HEADER_KEY =
            Metadata.Key.of("x-csf-client-id", Metadata.ASCII_STRING_MARSHALLER);

    private final String clientID;
    public ClientIDInterceptor(String clientID) {
        this.clientID = clientID;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel next) {
        ClientCall<ReqT, RespT> call = next.newCall(methodDescriptor, callOptions);
        call = new ClientIDInterceptor.HeaderAttachingClientCall(call);
        return call;
    }

    private final class HeaderAttachingClientCall<ReqT, RespT> extends SimpleForwardingClientCall<ReqT, RespT> {
        private HeaderAttachingClientCall(ClientCall<ReqT, RespT> call) {
            super(call);
        }

        public void start(Listener<RespT> responseListener, Metadata headers) {
            Metadata authHeaders = new Metadata();
            authHeaders.put(CSF_CLIENT_ID_HEADER_KEY, clientID);
            headers.merge(authHeaders);
            super.start(responseListener, headers);
        }
    }
}
