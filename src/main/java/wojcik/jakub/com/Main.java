package wojcik.jakub.com;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;

public class Main {


    static final String SERVER_ADRESS = "tcp://localhost:61616";
    private static  String queueName ;
    private static ActiveMQConnectionFactory connectionFactory;
    private static Connection connection;
    private static Session session;
    static Destination destination;
    static MessageConsumer consumer;



    public static void createSession() throws JMSException {

        connection = connectionFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createQueue(queueName);
        connection.start();
        consumer = session.createConsumer(destination);
    }

    public static List<String> getMessage() {
        List<String> messageList = new ArrayList<>();
        try {
            Message message = consumer.receive(100);
            while (message != null){
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    messageList.add(textMessage.getText());
                }
                message = consumer.receive(100);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return messageList;
    }

    public static void shutdown() {
        try {
            connection.close();
            session.close();
        } catch (JMSException e) {
            System.out.print(e.getMessage());
        }
    }
    public static void main(String[] args) {
        queueName = "kozirro.QUEUE";
        connectionFactory = new ActiveMQConnectionFactory(SERVER_ADRESS);
        try {
            createSession();
            List<String> messageList = getMessage();
            for (String message : messageList) {
                System.out.println("Received message =" + message );
            }
            System.out.println(messageList.size());
            shutdown();
        } catch (JMSException e) {
                System.out.print(e.getMessage());
        }
    }
}
