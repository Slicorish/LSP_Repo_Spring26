Part1: 
Shared Resource #1: nextId, which is the integer counter used to generate unique request IDs. Every thread calling addRequest() reads and writes it.

Shared Resource #2: requests, which is the ArrayList of Strings that stores every request. Every thread calling addRequest() mutates it.

Concurrency Problem: nextId++ could lead to a race condition because is three steps: read, add 1, write back. If two threads run getNextId() concurrently, they can both read the same value increment it, and return the increment value, which could  produce duplicate IDs. In addition, ArrayList.add is not thread-safe because concurrent calls can corrupt the internal array, causing lose writes or throw an ArrayIndexOutOfBoundsException error. 

Why addRequest() is unsafe: It performs three operations that together must be atomic: getting a unique ID, building the request string with that ID, and appending to the list. However, there is nothing in place that protects that sequence order. Threads can interleave between any two steps, two threads could receive the same ID, or the list could be corrupted by concurrent add() calls. In addition, the order of IDs relative to insertion order is not guaranteed.


Part 2: 
Fix A: public synchronized int getNextId() { ... } is incorrect. This makes the ID-generation step atomic, so no two threads get a duplicate ID. However, addRequest()still does more than just get an ID. After getNextId() returns, the thread releases the lock and then calls requests.add(...) with no protection. This means that two threads can call requests.add concurrently on an ArrayList, which is not thread-safe. Furthermore, data corruption, lost writes, and the wrong ordering of IDs in the list are still possible.

Fix B: public synchronized void addRequest(String studentName) { ... } is correct. The synchronized keyword on addRequest locks the current RequestManager instance for the entire method. The reading of nextId, writing it back, building the string, and appending of nextID to the list now runs atomically. Because getNextId() is only called from the inside of this locked method, there is no gap. Every call produces a consistent nextId, so no two threads get the same ID, and the list is mutated only one thread at a time.


Fix C: public synchronized List<String> getRequests() { ... } is incorrect. This synchronizes the reading of the list reference but doesn't address addRequest() at all. The race conidtion issue with nextId and the unsafe concurrent add are not addressed. Additionally, the returned reference still point to the internal list, so calls to the function can continue to mutate or iterate the list outside the lock.

Part 3: getNextId() should not be public. This is because according to Riel's heuristics, the priority is to minimize a class's public interface and to hide implementation details. getNextId() only exists to help addRequest() produce a unique identifier. Making it publicly accessible would allow a wider than neccessary scope of access, allowing clients to bypass addRequest() and hand out IDs that are never recorded in the list and it makes the class harder to make thread-safe because any synchronization would have to account for direct external calls. 

Part 4: 
Description: Instead of the synchronized keyword, use an explicit lock object from java.util.concurrent.locks,specifically ReentrantLock. The thread calls lock.lock() before entering the critical section and lock.unlock() in a finally block to guarantee release even if an exception is thrown. This gives the same mutual-exclusion guarantee as synchronized but with more control. For addRequest(), the lock wraps the same section: reading/updating nextId, building the string, and appending to the list so that the whole sequence is atomic.

Code Snippet: 
private final ReentrantLock lock = new ReentrantLock();

public void addRequest(String studentName) {
    lock.lock();
    try {
        int id = nextId++;
        requests.add("Request-" + id + " from " + studentName);
    } finally {
        lock.unlock();
    }
}

