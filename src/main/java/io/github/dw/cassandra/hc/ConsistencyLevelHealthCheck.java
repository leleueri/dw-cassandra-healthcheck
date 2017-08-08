package io.github.dw.cassandra.hc;

import com.codahale.metrics.health.HealthCheck;
import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


/**
 * Created by eric on 01/08/17.
 */
public class ConsistencyLevelHealthCheck extends HealthCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyLevelHealthCheck.class);

    private final ConsistencyLevel expectedCL;

    private final String keyspace;

    private final Cluster cassandraCluster;
    private final Metadata metadata;
    private final String localDc;

    public ConsistencyLevelHealthCheck(ConsistencyLevel consistencyLevel, String keyspace, Cluster cassandraCluster) {
        this(consistencyLevel, keyspace, null, cassandraCluster);
    }

    public ConsistencyLevelHealthCheck(ConsistencyLevel consistencyLevel, String keyspace, String datacenter,
                                       Cluster cassandraCluster) {
        LOGGER.debug("Initialize Cassandra HealthCheck with kespace '{}', consistencyLevel '{}' and localDc '{}'",
                keyspace, consistencyLevel, datacenter);
        this.expectedCL = consistencyLevel;
        this.keyspace = keyspace;
        this.cassandraCluster = cassandraCluster;
        this.metadata = cassandraCluster.getMetadata();
        this.localDc = datacenter;
        if (this.localDc == null && this.expectedCL.isDCLocal()) {
            throw new IllegalArgumentException("LOCAL ConsistencyLevel expected but Datacenter parameter is null");
        }
    }

    @Override
    protected Result check() throws Exception {

        List<Status> invalidStatus = metadata.getTokenRanges().stream()
                .map(this::evalConsistencyLevel)
                .filter(status -> status.isInvalid())
        .collect(Collectors.toList());

        if (invalidStatus.isEmpty()) {
            return Result.healthy("ConsistencyLevel '%s' is reached for Keyspace '%s'", keyspace, expectedCL);
        } else {
            return Result.unhealthy("ConsistencyLevel '%s' isn't reached for Keyspace '%s'", keyspace, expectedCL);
        }
    }

    protected Status evalConsistencyLevel(TokenRange range) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Check consistency level '{}' on keyspace '{}' for TokenRange {}",
                    this.expectedCL, this.keyspace, range);
        }

        final Set<Host> hosts = metadata.getReplicas(keyspace, range);
        final int numberOfReplicas = hosts.size();
        final Map<String, List<Host>> hostsByDC = hosts.stream().collect(Collectors.groupingBy(Host::getDatacenter));
        final List<Host> localNodes = hostsByDC.get(this.localDc);
        Status result = new Status(true, range, hosts);
        switch (expectedCL) {
            case ALL:
                if (getAvailableReplicas(hosts) < numberOfReplicas) result = new Status(false, range, hosts);
                break;

            case ONE:
                if (getAvailableReplicas(hosts) == 0) result = new Status(false, range, hosts);
                break;

            case TWO:
                if (getAvailableReplicas(hosts) < 2) result = new Status(false, range, hosts);
                break;

            case THREE:
                if (getAvailableReplicas(hosts) < 3) result = new Status(false, range, hosts);
                break;

            case LOCAL_ONE:
                if (localNodes == null) {
                    LOGGER.warn("No hosts found for DataCenter : {}", this.localDc);
                } else if (getAvailableReplicas(localNodes) == 0) result = new Status(false, range, hosts);
                break;

            case LOCAL_QUORUM:
            case LOCAL_SERIAL:
                if (localNodes == null) {
                    LOGGER.warn("No hosts found for DataCenter : {}", this.localDc);
                } else if (getAvailableReplicas(localNodes) < nodesForQuorum(localNodes.size())) result = new Status(false, range, hosts);
                break;

            case EACH_QUORUM:
                for (List<Host> dcHosts  : hostsByDC.values() ){
                    if (getAvailableReplicas(dcHosts) < nodesForQuorum(dcHosts.size())) result = new Status(false, range, hosts);
                }
                break;

            case QUORUM:
            case SERIAL:
                if (getAvailableReplicas(hosts) < nodesForQuorum(numberOfReplicas)) result = new Status(false, range, hosts);
                break;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Check consistency level '{}' on keyspace '{}' for TokenRange {} = {}",
                    this.expectedCL, this.keyspace, range, result.isValid() ? "OK" : "KO");
        }
        return result;
    }

    private long getAvailableReplicas(Collection<Host> hosts) {
        return hosts.stream().filter(h -> h.isUp()).count();
    }

    private int nodesForQuorum(int totalNodes) {
       return ((totalNodes/2)+1);
    }

    private class Status {

        private boolean clReached = false;
        private TokenRange tokenRange = null;
        private Set<Host> hosts = Collections.EMPTY_SET;

        public Status(boolean clReached, TokenRange tokenRange, Set<Host> hosts) {
            this.clReached = clReached;
            this.tokenRange = tokenRange;
            this.hosts = hosts;
        }

        public boolean isValid() {
            return this.clReached;
        }

        public boolean isInvalid() {
            return !this.clReached;
        }

        public TokenRange getTokenRange() {
            return tokenRange;
        }

        public Set<Host> getHosts() {
            return hosts;
        }

    }
}