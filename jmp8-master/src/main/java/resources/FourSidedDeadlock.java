package resources;

/**
 * Created by Dmitry on 29.08.2016.
 */
public class FourSidedDeadlock extends Thread {
    private Integer crossroad1Number;
    private Integer crossroad2Number;
    private String name;

    public FourSidedDeadlock(Integer crossroad1Number, Integer crossroad2Number, String name) {
        this.crossroad1Number = crossroad1Number;
        this.crossroad2Number = crossroad2Number;
        this.name = name;
    }

    @Override
    public void run() {
        synchronized (crossroad1Number) {
            System.out.println("Thread " + name + " blocks resource " + crossroad1Number);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }

            synchronized (crossroad2Number) {
                System.out.println("Thread "  + name + " locked second resource no " + crossroad2Number);
            }
            System.out.println("Thread " + name + " released resource " + crossroad1Number);
        }
    }

}
