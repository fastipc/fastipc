// ServiceServer/Program.cs
using System.Text;
using FastIpc;

// 4 KiB per client, requests and replies of up to 256 bytes,
// 1 client and 1 server: this process owns the service and
// is its server
using (var service = FipcService.Create("my_service", 4096,
           256, 1, 1))
using (var server = FipcService.Join("my_service",
           FipcServiceRole.Server, 5000))
{
    // three requests, then the end
    for (int i = 1; i <= 3; i++)
    {
        // the next request of any client
        if (server.Receive(out byte[] request) != FipcResult.Ok)
            return 1;
        string upper = Encoding.UTF8.GetString(request)
            .ToUpperInvariant();
        // the reply goes to its client, and never waits
        if (server.Send(Encoding.UTF8.GetBytes(upper),
                Fipc.NoWait) != FipcResult.Ok)
            return 1;
    }
}
// disposing the service ends it: the client still receives
// the replies sent
return 0;
