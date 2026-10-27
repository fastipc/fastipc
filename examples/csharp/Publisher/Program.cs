// Publisher/Program.cs
using System.Text;
using FastIpc;

// 64 KiB of messages, up to 8 subscribers; one that holds
// the publisher back 5 s is dropped
using (var publisher = FipcBroadcast.Create("my_broadcast",
           1 << 16, 8, 5000))
{
    // waits for the first subscriber
    if (publisher.WaitForMembers(1, out _) != FipcResult.Ok)
        return 1;
    for (int i = 1; i <= 3; i++)
        publisher.Send(Encoding.UTF8.GetBytes($"tick {i}"));
}
// disposing the publisher ends the broadcast: subscribers
// get Disconnected after the ticks
return 0;
