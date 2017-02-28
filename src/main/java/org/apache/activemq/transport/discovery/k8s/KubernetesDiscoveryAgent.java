/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.discovery.k8s;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The KubernetDiscoveryAgent can be used to build a network of brokers with
 * other pods running ActiveMQ in a Kubernetes cluster.
 *
 * This agent is based on the {@link org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent}.
 * Instead of pulling the list of services from a static list, we enumerate the pods that match
 * a given label key/value pair.
 *
 * The list of services is periodically refreshed by polling the Kubernetes API.
 */
public class KubernetesDiscoveryAgent implements DiscoveryAgent {

  // members
  private static final Logger LOG = LoggerFactory.getLogger(KubernetesDiscoveryAgent.class);

  private long initialReconnectDelay = 1000;
  private long maxReconnectDelay = initialReconnectDelay * 30;
  private long backOffMultiplier = 2;
  private boolean useExponentialBackOff = true;
  private int maxReconnectAttempts;
  private final Object sleepMutex = new Object();
  private final Object k8sSleepMutex = new Object();
  private long minConnectTime = 5000;
  private DiscoveryListener listener;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private TaskRunnerFactory taskRunner;

  private String namespace = "default";
  private String podLabelKey = "app";
  private String podLabelValue = "activemq";
  private int podPort = 61616;
  private String serviceUrlFormat = "tcp://%s:%s";
  private long sleepDelay = TimeUnit.SECONDS.toMillis(30);

  private final KubernetesClient client = new DefaultKubernetesClient();

  // nested types
  private static class SimpleDiscoveryEvent extends DiscoveryEvent {

    private int connectFailures;
    private long reconnectDelay = -1;
    private long connectTime = System.currentTimeMillis();
    private final AtomicBoolean failed = new AtomicBoolean(false);

    public SimpleDiscoveryEvent(String service) {

      super(service);
    }

    public SimpleDiscoveryEvent(SimpleDiscoveryEvent copy) {

      super(copy);
      connectFailures = copy.connectFailures;
      reconnectDelay = copy.reconnectDelay;
      connectTime = copy.connectTime;
      failed.set(copy.failed.get());
    }

    @Override
    public String toString() {
      return "SimpleDiscoveryEvent{" +
          "connectFailures=" + connectFailures +
          ", reconnectDelay=" + reconnectDelay +
          ", connectTime=" + connectTime +
          ", failed=" + failed +
          ", service=" + serviceName +
          '}';
    }
  }

  private class KubernetesPodEnumerator implements Runnable {
    @Override
    public void run() {

      Set<String> knownPods = new HashSet<>();
      while (running.get()) {
        try {

          LOG.info(
              "Enumerating pods with label key: {} label value: {}",
              podLabelKey,
              podLabelValue);

          Set<String> availablePods = client
              .pods()
              .inNamespace(namespace)
              .withLabel(podLabelKey, podLabelValue)
              .list()
              .getItems()
              .stream()
              .map(pod -> String.format(serviceUrlFormat, pod.getStatus().getPodIP(), podPort))
              .collect(Collectors.toSet());

          // Determine the list of service we need to add
          Set<String> podsToAdd = availablePods
              .stream()
              .filter(svc -> !knownPods.contains(svc))
              .collect(Collectors.toSet());

          // Add them
          for (String pod : podsToAdd) {

            LOG.info("Adding discovery event for pod [{}]", pod);
            listener.onServiceAdd(new SimpleDiscoveryEvent(pod));
            knownPods.add(pod);
          }

          // Determine the list of services we need to remove
          Set<String> podsToRemove = knownPods
              .stream()
              .filter(svc -> !availablePods.contains(svc))
              .collect(Collectors.toSet());

          // Remove them
          for (String pod : podsToRemove) {

            LOG.info("Removing discovery event for pod [{}]", pod);
            listener.onServiceRemove(new SimpleDiscoveryEvent(pod));
            knownPods.remove(pod);
          }
        } catch (RuntimeException e) {

          LOG.warn("Failed to enumerate the pods. Will try again later.", e);
        }

        // Sleep before trying again
        synchronized (k8sSleepMutex) {

          try {

            k8sSleepMutex.wait(sleepDelay);
          } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    }
  }

  // public API
  @Override
  public void registerService(String name)
    throws IOException {}

  @Override
  public void start() {

    LOG.info(
        "Discovery agent config - podPort [{}] maxReconnectAttempts [{}] useExponentialBackOff [{}] initialReconnectDelay [{}] maxReconnectDelay [{}]",
        podPort,
        maxReconnectAttempts,
        useExponentialBackOff,
        initialReconnectDelay,
        maxReconnectDelay);

    taskRunner = new TaskRunnerFactory();
    taskRunner.init();
    running.set(true);
    taskRunner.execute(new KubernetesPodEnumerator());
  }

  @Override
  public void stop()
    throws Exception {

    running.set(false);

    if (taskRunner != null) {

      taskRunner.shutdown();
    }

    synchronized (sleepMutex) {

      sleepMutex.notifyAll();
    }
  }

  @Override
  public void serviceFailed(DiscoveryEvent devent)
    throws IOException {

    SimpleDiscoveryEvent sevent = (SimpleDiscoveryEvent) devent;
    if (running.get() && sevent.failed.compareAndSet(false, true)) {

      listener.onServiceRemove(sevent);
      taskRunner.execute(() -> {
        SimpleDiscoveryEvent event = new SimpleDiscoveryEvent(sevent);

        if (System.currentTimeMillis() > event.connectTime + minConnectTime) {

          LOG.info(
              "Failure occurred to long after the discovery event was generated: {}",
              event);

          event.connectFailures++;

          if (maxReconnectAttempts > 0 && event.connectFailures >= maxReconnectAttempts) {

            LOG.warn(
                "Reconnect attempts exceeded {} tries.  Reconnecting has been disabled for: {}",
                maxReconnectAttempts,
                event);
            return;
          }

          if (!useExponentialBackOff || event.reconnectDelay == -1) {

            event.reconnectDelay = initialReconnectDelay;
          } else {

            /*
            Exponential increment of reconnect delay.
            */
            event.reconnectDelay *= backOffMultiplier;
            if (event.reconnectDelay > maxReconnectDelay) {

              event.reconnectDelay = maxReconnectDelay;
            }
          }

          doReconnectDelay(event);

        } else {

          /* TODO do we really need a grace period for recent connections ? */
          LOG.info(
              "Failure occurred soon after the discovery event was generated: {}",
              event);

          event.connectFailures = 0;
          event.reconnectDelay = initialReconnectDelay;
          doReconnectDelay(event);
        }

        if (!running.get()) {

          LOG.warn("Reconnecting disabled: stopped");
          return;
        }

        LOG.info("Will retry a new connection..");
        event.failed.set(false);
        listener.onServiceAdd(event);
      });
    }
  }

  // internal API
  protected void doReconnectDelay(SimpleDiscoveryEvent event) {

    synchronized (sleepMutex) {

      try {

        if (!running.get()) {

          LOG.debug("Reconnecting disabled: stopped");
          return;
        }

        LOG.info("Waiting {}ms before attempting to reconnect.", event.reconnectDelay);
        sleepMutex.wait(event.reconnectDelay);
      } catch (InterruptedException ie) {

        LOG.debug("Reconnecting disabled: ", ie);
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  // getters & setters
  @Override
  public void setDiscoveryListener(DiscoveryListener listener) {

    this.listener = listener;
  }

  public long getBackOffMultiplier() {
    return backOffMultiplier;
  }

  public void setBackOffMultiplier(long backOffMultiplier) {
    this.backOffMultiplier = backOffMultiplier;
  }

  public long getInitialReconnectDelay() {
    return initialReconnectDelay;
  }

  public void setInitialReconnectDelay(long initialReconnectDelay) {
    this.initialReconnectDelay = initialReconnectDelay;
  }

  public int getMaxReconnectAttempts() {
    return maxReconnectAttempts;
  }

  public void setMaxReconnectAttempts(int maxReconnectAttempts) {
    this.maxReconnectAttempts = maxReconnectAttempts;
  }

  public long getMaxReconnectDelay() {
    return maxReconnectDelay;
  }

  public void setMaxReconnectDelay(long maxReconnectDelay) {
    this.maxReconnectDelay = maxReconnectDelay;
  }

  public long getMinConnectTime() {
    return minConnectTime;
  }

  public void setMinConnectTime(long minConnectTime) {
    this.minConnectTime = minConnectTime;
  }

  public boolean isUseExponentialBackOff() {
    return useExponentialBackOff;
  }

  public void setUseExponentialBackOff(boolean useExponentialBackOff) {
    this.useExponentialBackOff = useExponentialBackOff;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getPodLabelKey() {
    return podLabelKey;
  }

  public void setPodLabelKey(String podLabelKey) {
    this.podLabelKey = podLabelKey;
  }

  public String getPodLabelValue() {
    return podLabelValue;
  }

  public void setPodLabelValue(String podLabelValue) {
    this.podLabelValue = podLabelValue;
  }

  public int getPodPort() {
    return podPort;
  }

  public void setPodPort(int podPort) {
    this.podPort = podPort;
  }

  public String getServiceUrlFormat() {
    return serviceUrlFormat;
  }

  public void setServiceUrlFormat(String serviceUrlFormat) {
    this.serviceUrlFormat = serviceUrlFormat;
  }

  public long getSleepDelay() {
    return sleepDelay;
  }

  public void setSleepDelay(long sleepDelay) {
    this.sleepDelay = sleepDelay;
  }
}
