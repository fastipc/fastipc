// Server/Program.cs
using System.Text;
using FastIpc;

// rings of 1 MiB each way
FipcListener.Listen("my_channel", 1 << 20,
    out FipcListener? listener);
using (listener)
{
    // waits for a client
    listener!.Accept(out FipcConnection? conn, Fipc.Forever);
    using (conn)
    {
        if (conn!.RpcReceive(out FipcRpcHeader request,
                out byte[] payload, Fipc.Forever)
            == FipcResult.Ok)
        {
            string upper = Encoding.UTF8.GetString(payload)
                .ToUpperInvariant();
            conn.RpcRespond(request.Id, request.Opcode, 0,
                Encoding.UTF8.GetBytes(upper), Fipc.Forever);
        }
    }
}
