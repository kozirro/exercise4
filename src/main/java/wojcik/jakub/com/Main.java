package wojcik.jakub.com;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class Main {
    private static Connection connection;
    private static Logger log = LoggerFactory.getLogger(Main.class);
    private static String SERVER_ADRESS = "tcp://localhost:61616";
    private static ActiveMQConnectionFactory connectionFactory;
    private static Session session;
    private static Destination destination;
    private static MessageProducer producer;

    public static void main(String[] args) {
        System.out.println("10000 persistent messages sent in " + persistanceProducerTest() + " milliseconds");
        System.out.println("10000 non-persistent messages sent in " + nonPersistanceProducerTest() + " milliseconds");
    }
    private static void start () {
        try {
            connectionFactory = new ActiveMQConnectionFactory(SERVER_ADRESS);
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private static void close () {
        try {
            connection.close();
            session.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private static long persistanceProducerTest(){
        long duration = 0;

            start();
            long startTime = System.nanoTime();
            for (int i = 1; i < 10001; i++){
                sendTextToQueue("test_" + i, DeliveryMode.PERSISTENT, "kozirro.QUEUE");
            }
            long endTime = System.nanoTime();
            close();
            duration = (endTime - startTime);


        return duration;
    }

    private static long nonPersistanceProducerTest(){
        long duration = 0;
            start();
            long startTime = System.nanoTime();
            for (int i = 10001; i < 20001; i++){
                sendTextToQueue("test_" + i, DeliveryMode.NON_PERSISTENT, "kozirro.QUEUE" );
            }
            long endTime = System.nanoTime();
            close();
            duration = (endTime - startTime);
        return duration;
    }

    public static void sendTextToQueue(String text, int deliveryMode, String queueName){
        try {
            destination = session.createQueue(queueName);
            producer = session.createProducer(destination);
            producer.setDeliveryMode(deliveryMode);
            connection.start();
            TextMessage message = session.createTextMessage(text);
            producer.send(message);

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }


}
