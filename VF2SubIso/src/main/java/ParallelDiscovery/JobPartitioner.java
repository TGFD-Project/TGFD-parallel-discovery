package ParallelDiscovery;

import Discovery.Job;

import java.util.*;

public class JobPartitioner {

    private JobEstimator estimator;
    private HashMap<Integer, ArrayList<Job>> jobsByFragmentID;

    public JobPartitioner(JobEstimator estimator)
    {
        this.estimator=estimator;
    }

    public HashMap<Integer, ArrayList<Job>> partition()
    {
        int i=0;
        ArrayList<Job> valuesList = new ArrayList<>();
        for (Map.Entry<Integer, ArrayList<Job>> entry : estimator.getJobsByFragmentID().entrySet()) {
            valuesList.addAll(entry.getValue());
        }
        Job[] allJobs = valuesList.toArray(new Job[0]);

        int []fragmentSize=new int[estimator.getNumberOfProcessors()];
        jobsByFragmentID=new HashMap<>();
        for (i=0;i< estimator.getNumberOfProcessors();i++)
        {
            jobsByFragmentID.put(i+1,new ArrayList<>());
        }
        HashSet<Integer> visited=new HashSet<>();
        while (true)
        {
            int id=-1;
            double min=Double.MAX_VALUE;
            for (i=0;i<allJobs.length;i++) {
                if(!visited.contains(i) && allJobs[i].getSize()<min)
                {
                    id=i;
                    min=allJobs[i].getSize();
                }
            }
            if(id!=-1)
            {
                visited.add(id);
                int minLoad=Integer.MAX_VALUE;
                int selectedFragment=-1;
                for (i=0;i<fragmentSize.length;i++)
                {
                    if(fragmentSize[i]<minLoad)
                    {
                        minLoad=fragmentSize[i];
                        selectedFragment=i;
                    }
                }
                fragmentSize[selectedFragment]+=allJobs[id].getSize();
                jobsByFragmentID.get(selectedFragment+1).add(allJobs[id]);
            }
            else
                break;
        }
        for (i=0;i<fragmentSize.length;i++)
            System.out.print("F"+(i+1) + ": " + fragmentSize[i] + "  **  ");
        System.out.println("\nPartitioner is Done. Number of jobs: " + allJobs.length);
        return jobsByFragmentID;
    }

}
