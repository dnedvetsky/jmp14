This is a simple four-sided locker:

It creates one thread, locks resources 1 and 2;
Then - second, locking resources 3 and 4;
This way every thread blocks execution of thread connected to it. Like:

Thread 1 blocks Thread 4, which blocks thread 3 and etc.

Removing one of the threads from execution resolves deadlock issue;

This implementation is extension of simple 2x deadlocks.