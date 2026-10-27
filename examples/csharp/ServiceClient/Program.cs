// ServiceClient/Program.cs
using System.Text;
using FastIpc;

using var client = FipcService.Join("my_service",
    FipcServiceRole.Client, 5000);
for (int i = 1; i <= 3; i++)
{
    // sends the request and returns its reply; Abandoned if
    // the server ended before it replied
    if (client.Call(Encoding.UTF8.GetBytes($"ping {i}"),
            out byte[] reply) != FipcResult.Ok)
        return 1;
    // PING 1, PING 2, PING 3
    Console.WriteLine(Encoding.UTF8.GetString(reply));
}
return 0;
