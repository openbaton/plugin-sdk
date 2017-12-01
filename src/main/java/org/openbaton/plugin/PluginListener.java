package org.openbaton.plugin;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import org.apache.commons.codec.binary.Base64;
import org.openbaton.catalogue.nfvo.PluginAnswer;
import org.openbaton.catalogue.nfvo.images.BaseNfvImage;
import org.openbaton.catalogue.nfvo.networks.BaseNetwork;
import org.openbaton.catalogue.nfvo.viminstances.BaseVimInstance;
import org.openbaton.exceptions.NotFoundException;
import org.openbaton.nfvo.common.configuration.NfvoGsonDeserializerImage;
import org.openbaton.nfvo.common.configuration.NfvoGsonDeserializerNetwork;
import org.openbaton.nfvo.common.configuration.NfvoGsonDeserializerVimInstance;
import org.openbaton.nfvo.common.configuration.NfvoGsonSerializerVimInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Created by lto on 25/11/15.
 */
public class PluginListener implements Runnable {

  private static final String exchange = "openbaton-exchange";
  private String pluginId;
  private Object pluginInstance;
  private Logger log;
  private QueueingConsumer consumer;
  private Channel channel;
  private Gson gson =
      new GsonBuilder()
          .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter())
          .registerTypeAdapter(BaseVimInstance.class, new NfvoGsonDeserializerVimInstance())
          .registerTypeAdapter(BaseNetwork.class, new NfvoGsonDeserializerNetwork())
          .registerTypeAdapter(BaseNfvImage.class, new NfvoGsonDeserializerImage())
          .registerTypeAdapter(BaseVimInstance.class, new NfvoGsonSerializerVimInstance())
          .setPrettyPrinting()
          .create();
  private boolean exit = false;
  private String brokerIp;
  private int brokerPort;
  private String username;
  private String password;
  private String virtualHost;
  private Connection connection;

  public boolean isDurable() {
    return durable;
  }

  public void setDurable(boolean durable) {
    this.durable = durable;
  }

  private boolean durable = true;

  public String getPluginId() {
    return pluginId;
  }

  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  public Object getPluginInstance() {
    return pluginInstance;
  }

  public void setPluginInstance(Object pluginInstance) {
    this.pluginInstance = pluginInstance;
    log = LoggerFactory.getLogger(pluginInstance.getClass().getName());
  }

  public boolean isExit() {
    return exit;
  }

  public void setExit(boolean exit) {
    this.exit = exit;
  }

  private static class ByteArrayToBase64TypeAdapter
      implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {
    public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      return Base64.decodeBase64(json.getAsString());
    }

    public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(Base64.encodeBase64String(src));
    }
  }

  @Override
  public void run() {

    try {
      initRabbitMQ();
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
      setExit(true);
    }
    try {

      while (!exit) {
        QueueingConsumer.Delivery delivery;
        BasicProperties props;
        BasicProperties replyProps;
        try {
          delivery = consumer.nextDelivery();

          props = delivery.getProperties();
          replyProps =
              new BasicProperties.Builder()
                  .correlationId(props.getCorrelationId())
                  //                        .contentType("plain/text")
                  .build();
        } catch (Exception e) {
          e.printStackTrace();
          exit = true;
          continue;
        }
        log.info("\nWaiting for RPC requests");
        String message = new String(delivery.getBody());

        log.debug("Received message");
        log.trace("Message content received: " + message);

        PluginAnswer answer = new PluginAnswer();

        try {
          answer.setAnswer(executeMethod(message));
        } catch (InvocationTargetException e) {
          answer.setException(e.getTargetException());
        } catch (Exception e) {
          e.printStackTrace();
          answer.setException(e);
        }

        String response;
        try {
          response = gson.toJson(answer);

          log.trace("Answer is: " + response);
          log.debug("Reply queue is: " + props.getReplyTo());

          channel.basicPublish(exchange, props.getReplyTo(), replyProps, response.getBytes());

          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Exception e) {
          e.printStackTrace();
          answer.setException(e);
          log.debug("Answer is: " + answer);
          log.debug("Reply queue is: " + props.getReplyTo());

          channel.basicPublish(
              exchange, props.getReplyTo(), replyProps, gson.toJson(answer).getBytes());

          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          setExit(true);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      if (channel == null || connection == null) System.exit(3);
      channel.close();
      connection.close();
    } catch (IOException | TimeoutException e) {
      e.printStackTrace();
    }
  }

  private Serializable executeMethod(String pluginMessageString)
      throws InvocationTargetException, IllegalAccessException, NotFoundException {

    JsonObject pluginMessageObject = gson.fromJson(pluginMessageString, JsonObject.class);

    List<Object> params = new ArrayList<>();

    for (JsonElement param : pluginMessageObject.get("parameters").getAsJsonArray()) {
      Object p = gson.fromJson(param, Object.class);
      if (p != null) {
        params.add(p);
      }
    }

    Class pluginClass = pluginInstance.getClass();

    log.debug("There are " + params.size() + " parameters");
    String methodName = pluginMessageObject.get("methodName").getAsString();
    log.debug("Looking for method: " + methodName);

    for (Method m : pluginClass.getMethods()) {
      log.trace(
          "Method checking is: "
              + m.getName()
              + " with "
              + m.getParameterTypes().length
              + " parameters");
      byte[] avoid = new byte[0];
      if (m.getName().equals(methodName)
          && m.getParameterTypes().length == params.size()
          && !m.getParameterTypes()[m.getParameterTypes().length - 1]
              .getCanonicalName()
              .equals(avoid.getClass().getCanonicalName())) {
        if (!m.getReturnType().equals(Void.class)) {
          if (params.size() != 0) {
            params =
                getParameters(
                    pluginMessageObject.get("parameters").getAsJsonArray(), m.getParameterTypes());
            for (Object p : params) {
              log.trace("param class is: " + p.getClass());
            }
            return (Serializable) m.invoke(pluginInstance, params.toArray());
          } else {
            return (Serializable) m.invoke(pluginInstance);
          }
        } else {
          if (params.size() != 0) {
            params =
                getParameters(
                    pluginMessageObject.get("parameters").getAsJsonArray(), m.getParameterTypes());
            for (Object p : params) {
              log.trace("param class is: " + p.getClass());
            }
            m.invoke(pluginInstance, params.toArray());
          } else {
            m.invoke(pluginInstance);
          }

          return null;
        }
      }
    }

    throw new NotFoundException("method not found");
  }

  private List<Object> getParameters(JsonArray parameters, Class<?>[] parameterTypes) {
    List<Object> res = new LinkedList<Object>();
    for (int i = 0; i < parameters.size(); i++) {
      res.add(gson.fromJson(parameters.get(i), parameterTypes[i]));
    }
    return res;
  }

  private void initRabbitMQ() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(brokerIp);
    factory.setPort(brokerPort);
    factory.setPassword(password);
    factory.setUsername(username);
    factory.setVirtualHost(virtualHost);

    connection = factory.newConnection();
    channel = connection.createChannel();

    channel.queueDeclare(pluginId, this.durable, false, true, null);
    channel.queueBind(pluginId, exchange, pluginId);

    channel.basicQos(1);

    consumer = new QueueingConsumer(channel);
    channel.basicConsume(pluginId, false, consumer);
  }

  public String getBrokerIp() {
    return brokerIp;
  }

  public void setBrokerIp(String brokerIp) {
    this.brokerIp = brokerIp;
  }

  public int getBrokerPort() {
    return brokerPort;
  }

  public void setBrokerPort(int brokerPort) {
    this.brokerPort = brokerPort;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getVirtualHost() {
    return this.virtualHost;
  }

  public void setVirtualHost(String virtualHost) {
    this.virtualHost = virtualHost;
  }
}
