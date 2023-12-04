# Memory tracker for filesystems

This project dynamically updates database with memory statistics on filesystems and notifies of a memory drop.

## Why is this useful?

At the University of Richmond (UR), we have a high perfomance computing (HPC) cluster with a number of filesystems. UR's students and faculty conduct complex calculations using cluster. It happens that due to a mistake, they can take up the memory of the whole filesystem, such as /scratch or /home directory. This program tracks the availability of the filesystems and notifies HPC system administrators of a memory drop. That way, this program helps prevent a system crash and allows for any memory issues to be addressed in a timely manner.

## How does it work?

The program runs as a deamon 
```nohup python dfstat.py &```

It wakes up every once in a while, collects information on memory availability of the filesystems by running
``` df -h ```

Then, information on filesystems of interest is inserted into the database. The program calculates KPSS statistics to identify data stationarity for each filesystem. If data is non-stationary and if there is a memory drop, the email notification that prompts system adminsitrators to check on the cluster is sent.

