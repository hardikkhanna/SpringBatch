# SpringBatch

<h2>What is SpringBatch ? </h2>
 <p>Spring batch is a lightweight framework which is used to develop Batch Applications
 that are used in Enterprise Applications.</p>
 <p>In addition to bulk processing, this framework provides functions for –</p>
   <ul><li>Including logging and tracing</li>
  <li> Transaction management</li>
  <li> Job processing statistics</li>
  <li>Job restart</li>
  <li>Skip and Resource management</li></ul>


<h2> Spring Batch Architecture </h2>



<h2>How Spring Batch Works?</h2>

1. Job - In a Spring Batch application, a job is the batch process that is to be executed. It runs from start to finish without interruption. This job is further divided into steps (or a job contains steps).

2. Step - A Step that delegates to a Job to do its work. This is a great tool for managing dependencies between jobs, and also to modularise complex step logic into something that is testable in isolation. The job is executed with parameters that can be extracted from the step execution, hence this step can also be usefully used as the worker in a parallel or partitioned execution.

3. ItemReader - Strategy interface for providing the data. Implementations are expected to be stateful and will be called multiple times for each batch, with each call to read() returning a different value and finally returning null when all input data is exhausted. Implementations need not be thread-safe and clients of a ItemReader need to be aware that this is the case. A richer interface (e.g. with a look ahead or peek) is not feasible because we need to support transactions in an asynchronous batch.

4. ItemProcessor - Interface for item transformation. Given an item as input, this interface provides an extension point which allows for the application of business logic in an item oriented processing scenario. It should be noted that while it's possible to return a different type than the one provided, it's not strictly necessary. Furthermore, returning null indicates that the item should not be continued to be processed.

5. ItemStreamWriter - Basic interface for generic output operations. Class implementing this interface will be responsible for serializing objects as necessary. Generally, it is responsibility of implementing class to decide which technology to use for mapping and how it should be configured. The write method is responsible for making sure that any internal buffers are flushed. If a transaction is active it will also usually be necessary to discard the output on a subsequent rollback. The resource to which the writer is sending data should normally be able to handle this itself.
