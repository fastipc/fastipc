// Server/Program.cs
using System.Text;
using FastIpc;

// rings of 1 MiB each way; Accept waits for a client
using var listener = FipcStream.Create("my_channel", 1 << 20);
using var conn = listener.Accept();
if (conn.Receive(out byte[] message) == FipcResult.Ok)
{
    string upper = Encoding.UTF8.GetString(message)
        .ToUpperInvariant();
    conn.Send(Encoding.UTF8.GetBytes(upper));
}
