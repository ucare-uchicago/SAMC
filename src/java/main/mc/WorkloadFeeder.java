package mc;

import java.util.LinkedList;

public class WorkloadFeeder {
    
    public final LinkedList<Workload> allWorkload;
    public final LinkedList<SpecVerifier> allVerifiers;
    public int numFinished;
    
    public WorkloadFeeder(LinkedList<Workload> allWorkload, 
            LinkedList<SpecVerifier> allVerifiers) {
        this.allWorkload = allWorkload;
        this.allVerifiers = allVerifiers;
        numFinished = 0;
    }
    
    public void runAll() {
        for (Workload load : allWorkload) {
            load.run();
        }
    }
    
    public void stopAll() {
        for (Workload load : allWorkload) {
            load.stop();
        }
    }
    
    public void resetAll() {
        for (Workload load : allWorkload) {
            load.reset();
        }
        numFinished = 0;
    }
    
    public void notifyFinished() {
        numFinished++;
    }
    
    public boolean areAllWorkDone() {
        return numFinished == allWorkload.size();
    }
    
    public boolean verify() {
        for (SpecVerifier verifiers : allVerifiers) {
            if (!verifiers.verify()) {
                return false;
            }
        }
        return true;
    }
    
    public boolean isCheck() {
        for (Workload load : allWorkload) {
            if (!load.check) {
                return false;
            }
        }
        return true;
    }
    
}
