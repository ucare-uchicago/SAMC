package mc;

public abstract class Workload {
    
    protected boolean isFinished;
    protected WorkloadFeeder feeder;
    public boolean check;
    
    public Workload() {
        isFinished = false;
        this.feeder = null;
        check = true;
    }
    
    public Workload(WorkloadFeeder feeder) {
        isFinished = false;
        this.feeder = feeder;
        check = true;
    }
    
    public void finish() {
        if (!isFinished) {
            if (feeder != null) {
                feeder.notifyFinished();
            }
            isFinished = true;
        }
    }
    
    public void reset() {
        isFinished = false;
    }
    
    public abstract void run();
    
    public abstract void stop();
        
}
