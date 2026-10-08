// Client/Program.cs
using System.Text;
using FastIpc;

if (FipcConnection.Connect("my_channel",
        out FipcConnection? conn, 5000) != FipcResult.Ok)
    return 1;
using (conn)
{
    conn!.RpcSubmit(opcode: 1, "ping"u8, out ulong id,
        Fipc.Forever);
    conn.RpcReceive(out FipcRpcHeader response,
        out byte[] reply, Fipc.Forever);
    Console.WriteLine(Encoding.UTF8.GetString(reply));  // PING
}
return 0;
