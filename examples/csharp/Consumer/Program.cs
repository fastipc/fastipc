// Consumer/Program.cs
using System.Text;
using FastIpc;

using var consumer = FipcQueue.Join("my_queue",
    FipcQueueRole.Consumer, 5000);
FipcResult result;
while ((result = consumer.Receive(out byte[] job))
       == FipcResult.Ok)
    Console.WriteLine(Encoding.UTF8.GetString(job));
// job 1, job 2, job 3, then Disconnected: the queue
// ended
return result == FipcResult.Disconnected ? 0 : 1;
