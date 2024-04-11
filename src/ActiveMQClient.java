import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;

import javax.jms.*;

import static java.lang.Thread.sleep;

public class ActiveMQClient {

    // Define the queue names
    private static final String QUEUE_NAME = "ExampleQueue";
    private static final String TOPIC_NAME = "ExampleTopic";
    private static final String BROKER_URL = "tcp://localhost:61616";

    public static void main(String[] args) {

        getNewConsumer().start();

        getNewProducer().start();
    }


    private static Thread getNewProducer() {
        return new Thread(() -> {
            try {

                ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
                ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
                connection.start();

                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                //Destination destination = session.createQueue(QUEUE_NAME);
                Destination destination = session.createTopic(TOPIC_NAME);
                MessageProducer producer = session.createProducer(destination);

                for (int i=0;i<100;i++){
                    TextMessage message = session.createTextMessage("Hello, ActiveMQ!");
                    try {
                        producer.send(message);
                        //throw new RuntimeException("Error occurred during message processing");
                    }catch (Exception e){
                        //producer.send(message);
                        session.recover();
                    }
                    sleep(10);
                }

                //System.out.println("Producer sent: " + message.getText());

                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private static Thread getNewConsumer() {
        return new Thread(() -> {
            try {

                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
                ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
                connection.start();


                Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                //Destination destination = session.createQueue(QUEUE_NAME); // Queue name
                Destination destination = session.createTopic(TOPIC_NAME);
                MessageConsumer consumer = session.createConsumer(destination);

                consumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        if  (message instanceof TextMessage) {
                            try {
                                System.out.println("Consumer received: " + ((TextMessage) message).getText());
                                throw new RuntimeException("Simulated processing error");
                            } catch (Exception e) {
                                System.err.println(e.getMessage());
                                try {
                                    session.recover();
                                } catch (JMSException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }
                        }
                    }
                });

                sleep(30000);

                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e){
                e.printStackTrace();
            }
        });
    }
}