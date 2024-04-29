/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.common.net.ServiceURI;

/**
 * The default implementation of {@link ServiceNameResolver}.
 */
@Slf4j
public class PulsarServiceNameResolver implements ServiceNameResolver {

    private int quarantineTimeSeconds;
    private volatile ServiceURI serviceUri;
    private volatile String serviceUrl;
    private static final AtomicIntegerFieldUpdater<PulsarServiceNameResolver> CURRENT_INDEX_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PulsarServiceNameResolver.class, "currentIndex");
    private volatile int currentIndex;
    private volatile List<InetSocketAddress> addressList;

    private Cache<String, Boolean> quarantinedSocketAddress;

    public PulsarServiceNameResolver() {
    }

    public PulsarServiceNameResolver(int quarantineTimeSeconds) {
        this.quarantineTimeSeconds = quarantineTimeSeconds;
    }

    @Override
    public InetSocketAddress resolveHost() {
        List<InetSocketAddress> list = addressList;

        if (quarantinedSocketAddress != null && quarantinedSocketAddress.size() > 0
                && quarantinedSocketAddress.size() < list.size()) {
            list = list.stream().filter(address ->
                            quarantinedSocketAddress.getIfPresent(buildSocketAddress(address.getHostName(), address.getPort()))
                                    == null)
                    .collect(Collectors.toList());
        }

        checkState(
            list != null, "No service url is provided yet");
        checkState(
            !list.isEmpty(), "No hosts found for service url : " + serviceUrl);
        if (list.size() == 1) {
            return list.get(0);
        } else {
            List<InetSocketAddress> finalList = list;
            int originalIndex = CURRENT_INDEX_UPDATER.getAndUpdate(this, last -> (last + 1) % finalList.size());
            return list.get((originalIndex + 1) % list.size());
        }
    }

    @Override
    public URI resolveHostUri() {
        InetSocketAddress host = resolveHost();
        String hostUrl = serviceUri.getServiceScheme() + "://" + host.getHostString() + ":" + host.getPort();
        return URI.create(hostUrl);
    }

    @Override
    public String getServiceUrl() {
        return serviceUrl;
    }

    @Override
    public ServiceURI getServiceUri() {
        return serviceUri;
    }

    @Override
    public void updateServiceUrl(String serviceUrl) throws InvalidServiceURL {
        ServiceURI uri;
        try {
            uri = ServiceURI.create(serviceUrl);
        } catch (IllegalArgumentException iae) {
            log.error("Invalid service-url {} provided {}", serviceUrl, iae.getMessage(), iae);
            throw new InvalidServiceURL(iae);
        }

        String[] hosts = uri.getServiceHosts();
        List<InetSocketAddress> addresses = new ArrayList<>(hosts.length);
        for (String host : hosts) {
            String hostUrl = uri.getServiceScheme() + "://" + host;
            try {
                URI hostUri = new URI(hostUrl);
                addresses.add(InetSocketAddress.createUnresolved(hostUri.getHost(), hostUri.getPort()));
            } catch (URISyntaxException e) {
                log.error("Invalid host provided {}", hostUrl, e);
                throw new InvalidServiceURL(e);
            }
        }
        this.addressList = addresses;
        this.serviceUrl = serviceUrl;
        this.serviceUri = uri;
        this.currentIndex = randomIndex(addresses.size());

        if (quarantineTimeSeconds > 0) {
            this.quarantinedSocketAddress = CacheBuilder.newBuilder()
                    .expireAfterWrite(quarantineTimeSeconds, TimeUnit.SECONDS)
                    .removalListener((RemovalListener<String, Boolean>) address -> log.info(
                            "InetSocketAddress {} is no longer quarantined", address.getKey())).build();
        }
    }

    @Override
    public void quarantineSocketAddress(String socketAddress) {
        if (quarantinedSocketAddress != null) {
            quarantinedSocketAddress.put(socketAddress, Boolean.TRUE);
            log.warn("InetSocketAddress {} has been quarantined because of connection errors.", socketAddress);
        }
    }

    @Override
    public void quarantineSocketAddress(InetSocketAddress socketAddress) {
        if (socketAddress != null) {
            quarantineSocketAddress(buildSocketAddress(socketAddress.getHostName(), socketAddress.getPort()));
        }
    }

    private static int randomIndex(int numAddresses) {
        return numAddresses == 1
                ?
                0 : io.netty.util.internal.PlatformDependent.threadLocalRandom().nextInt(numAddresses);
    }
}
