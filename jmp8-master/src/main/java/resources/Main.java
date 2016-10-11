package resources;

/**
 * Created by Dmitry on 29.08.2016.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new FourSidedDeadlock(1, 2, "Tr1");
        Thread t2 = new FourSidedDeadlock(2, 3, "Tr2");
        Thread t3 = new FourSidedDeadlock(3, 4, "Tr3");
        Thread t4 = new FourSidedDeadlock(4, 1, "Tr4");

        t1.start();
        t2.start();
        t3.start();
        t4.start();
    }

}
