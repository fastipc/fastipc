// Subscriber/Program.cs
using System.Text;
using FastIpc;

// gated: misses nothing (the publisher waits for it)
using var sub = FipcBroadcast.Join("my_broadcast",
    FipcSubscriberRole.Gated, 5000);
FipcResult result;
while ((result = sub.Receive(out byte[] tick))
       == FipcResult.Ok)
    Console.WriteLine(Encoding.UTF8.GetString(tick));
// tick 1, tick 2, tick 3, then Disconnected: the
// publisher closed
return result == FipcResult.Disconnected ? 0 : 1;
