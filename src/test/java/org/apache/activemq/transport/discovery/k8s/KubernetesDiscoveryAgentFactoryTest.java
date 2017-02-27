package org.apache.activemq.transport.discovery.k8s;

import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;

import static org.junit.Assert.assertTrue;

public class KubernetesDiscoveryAgentFactoryTest {

  @Test
  public void canCreateKubernetesDiscoveryAgent()
    throws IOException {

    KubernetesDiscoveryAgentFactory factory = new KubernetesDiscoveryAgentFactory();
    DiscoveryAgent agent =
        factory.doCreateDiscoveryAgent(URI.create("k8s://default?podLabelKey=app&amp;podLabelValue=activemq"));
    assertTrue(agent instanceof KubernetesDiscoveryAgent);
  }
}
