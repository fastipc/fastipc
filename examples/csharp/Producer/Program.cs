// Producer/Program.cs
using System.Text;
using FastIpc;

// 4 KiB of messages per producer, 1 producer and 1 consumer:
// this process owns the queue and is its producer
using (var queue = FipcQueue.Create("my_queue", 4096, 1, 1))
using (var producer = FipcQueue.Join("my_queue",
           FipcQueueRole.Producer, 5000))
{
    for (int i = 1; i <= 3; i++)
    {
        // waits in the queue until a consumer takes it
        if (producer.Send(Encoding.UTF8.GetBytes($"job {i}"))
            != FipcResult.Ok)
            return 1;
    }
    // waits for the consumer to join
    if (queue.WaitForMembers(FipcQueueRole.Consumer, 1, out _)
        != FipcResult.Ok)
        return 1;
}
// disposing the queue ends it: the consumer gets
// Disconnected after the jobs
return 0;
