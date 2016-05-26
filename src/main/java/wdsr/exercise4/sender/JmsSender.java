package wdsr.exercise4.sender;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.Order;

import javax.jms.*;
import java.math.BigDecimal;
import java.util.Map;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);

	private final String queueName;
	private final String topicName;
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
	}

	private void setUp() throws JMSException{
		connection = connectionFactory.createConnection();
		connection.start();

		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	private void close() throws JMSException{
		session.close();
		connection.close();
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		try {
			setUp();

			Destination destination = session.createQueue(queueName);
			MessageProducer producer = session.createProducer(destination);
			Order order = new Order(orderId,product, price);
			ObjectMessage message = session.createObjectMessage(order);
			message.setJMSType("Order");
			message.setStringProperty("WDSR-System", "OrderProcessor");
			producer.send(message);

			close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		try {
			setUp();

			Destination destination = session.createQueue(queueName);
			MessageProducer producer = session.createProducer(destination);
			TextMessage message = session.createTextMessage(text);
			producer.send(message);

			close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		try {
			setUp();

			Destination destination = session.createTopic(topicName);
			MessageProducer producer = session.createProducer(destination);
			MapMessage message = session.createMapMessage();
			for (Map.Entry<String, String> entry : map.entrySet())
			{
				message.setString(entry.getKey(), entry.getValue());
			}
			producer.send(message);

			close();
		} catch (JMSException e) {
			log.error(e.getMessage());
		}

	}
}