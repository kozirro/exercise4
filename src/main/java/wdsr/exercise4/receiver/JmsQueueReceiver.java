package wdsr.exercise4.receiver;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

import javax.jms.*;
import java.math.BigDecimal;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 *
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	static final String SERVER_ADRESS = "tcp://localhost:62616";
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private final String queueName;
	static final String PRICE_ALERT_TYPE = "PriceAlert";
	static final String VOLUME_ALERT_TYPE = "VolumeAlert";

	Destination destination;
	MessageConsumer consumer;
	private AlertService alertService;

	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) {
		this.queueName = queueName;
		connectionFactory = new ActiveMQConnectionFactory(SERVER_ADRESS);
		connectionFactory.setTrustAllPackages(true);
	}


	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */

	public void registerCallback(AlertService alertService) {
		this.alertService = alertService;
		try {
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue(queueName);
			consumer = session.createConsumer(destination);
			consumer.setMessageListener(new MessageListener() {
				@Override
				public void onMessage(Message message) {
					try {
						String messageType = message.getJMSType().toString();
						if (message instanceof TextMessage){
							textMessageConsumer (message, messageType);
						} else if (message instanceof ObjectMessage){
							objectMessageConsumer (message, messageType);
						}
					} catch (JMSException e) {
						e.printStackTrace();
					}
				}
			});

			connection.start();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void textMessageConsumer (Message message, String messageType) {
		TextMessage textMessage = (TextMessage) message;
		String stringMessage = null;
		try {
			stringMessage = textMessage.getText();
			String delims = "[\n]";
			String[] stringsItems = stringMessage.split(delims);

			if ( messageType.equals(VOLUME_ALERT_TYPE)){
				String[] volumeAlertSplit = stringMessage.split("=|\\r?\\n");
				VolumeAlert volumeAlertObject = null;
				if(volumeAlertSplit.length == 6) {
					Long timestamp = Long.parseLong(volumeAlertSplit[1]);
					String stock = volumeAlertSplit[3];
					Long floatingVolume = Long.parseLong(volumeAlertSplit[5]);
					volumeAlertObject = new VolumeAlert(timestamp,stock ,floatingVolume );
				}
				alertService.processVolumeAlert(volumeAlertObject);
			} else if ( messageType.equals(PRICE_ALERT_TYPE )) {
				String[] priceAlertSplit = stringMessage.split("=|\\r?\\n");
				PriceAlert priceAlertObject = null;
				if (priceAlertSplit.length == 6) {
					Long timestamp = Long.parseLong(priceAlertSplit[1]);
					String stock = priceAlertSplit[3];
					BigDecimal currentPrice = BigDecimal.valueOf(Long.parseLong(priceAlertSplit[5].replaceAll("\\D+","")));
					priceAlertObject = new PriceAlert(timestamp,stock ,currentPrice);
					alertService.processPriceAlert(priceAlertObject);
				}
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void objectMessageConsumer (Message message, String messageType) {
		try {
			ObjectMessage objectMessage = (ObjectMessage) message;
			if ( messageType.equals(VOLUME_ALERT_TYPE)){
				VolumeAlert volumeAlert = (VolumeAlert) objectMessage.getObject();
				alertService.processVolumeAlert(volumeAlert);
			} else if ( messageType.equals(PRICE_ALERT_TYPE )) {
				PriceAlert priceAlert = (PriceAlert) objectMessage.getObject();
				alertService.processPriceAlert(priceAlert);
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}



	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		try {
			connection.close();
			session.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	// TODO
	// This object should start consuming messages when registerCallback method is invoked.

	// This object should consume two types of messages:
	// 1. Price alert - identified by header JMSType=PriceAlert - should invoke AlertService::processPriceAlert
	// 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke AlertService::processVolumeAlert
	// Use different message listeners for and a JMS selector

	// Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert or VolumeAlert class)
	// or as a TextMessage.
	// Text for PriceAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Price=<long value>
	// Text for VolumeAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Volume=<long value>

	// When shutdown() method is invoked on this object it should remove the listeners and close open connection to the broker.
}