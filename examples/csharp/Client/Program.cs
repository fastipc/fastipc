// Client/Program.cs
using System.Text;
using FastIpc;

// waits up to 5 s for the server, else throws
using var conn = FipcStream.Connect("my_channel", 5000);
conn.Send("ping"u8);
conn.Receive(out byte[] reply);
Console.WriteLine(Encoding.UTF8.GetString(reply));  // PING
