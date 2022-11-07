import MPI.Consumer;
import MPI.Producer;
import Util.Config;

public class testRabbitMQ {

    public static void main(String []args)
    {
        Config.ActiveMQBrokerURL = args[0]+"://"+args[1]+":"+args[2];
        Config.ActiveMQUsername = "admin";
        Config.ActiveMQPassword = "admin";

        Producer messageProducer=new Producer();
        messageProducer.connect();
        messageProducer.send("test_mq",args[3]);
        messageProducer.close();


        Consumer consumer=new Consumer();
        consumer.connect("test_mq");

        String msg = consumer.receive();

        System.out.println(msg);

        consumer.close();

        System.out.println("Done.");
    }

}
