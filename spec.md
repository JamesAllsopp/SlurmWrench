We are going to set up a slurm array job that will take 4000 jobs and run them 5 at time. Each of these jobs will run on an icelake node, and launch 70 jobs of it's own.

So 350 jobs, starting with those of the lowest distance will run at one time, and 800 will run in total, meaning a total of 280,000. Each job will then be run another two times to cover the full 700,000 target.

The python script loaded by the slurm will use get_simulations in the get_next_block.py file (delete the get_simulations_for_a_block file). We need to set the start date and status for all the simulations in the block, so should move get_simulations and this update into a transaction that can successfully roll-back

Once we have the simulations, the python script will run ccmd with the directory option as a background system task, which should allow us to use all of the cores on the icelake system. Each of these tasks needs to have it's status recorded, and when it finishes, we can update the simulations table with the status and end time. 

Once the Python script finishes, It should update the block status with the endtime TODO.

It seems like a time out is the way to go to handle the issue with concurrency.

 