using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace FastIpc.IntegrationTests
{
    /// <summary>
    /// Runs a pair of the example projects (examples/csharp) that this project references, each in a process of its own
    /// (their assemblies are in this project's output).
    /// </summary>
    internal static class ExamplePairs
    {
        /// <summary>
        /// The <paramref name="sender"/> and the <paramref name="receiver"/>, in both start orders, the first one
        /// second ahead of the other: receiver first, its join waits up to 5 s for the name; sender first, it waits for
        /// the receiver. The receiver prints <paramref name="expected"/> (in any order if <paramref name="anyOrder"/>:
        /// a queue promises none), and both exit with 0.
        /// </summary>
        public static void Run(string sender, string receiver, string[] expected, bool anyOrder)
        {
            foreach (bool receiverFirst in new[] { true, false })
            {
                using Process first = Start(receiverFirst ? receiver : sender);
                Thread.Sleep(1000);
                using Process second = Start(receiverFirst ? sender : receiver);
                Process receiving = receiverFirst ? first : second;
                Process sending = receiverFirst ? second : first;
                Task<string> printed = receiving.StandardOutput.ReadToEndAsync();
                foreach ((Process process, string what) in new[] { (receiving, receiver), (sending, sender) })
                {
                    if (!process.WaitForExit(20_000))
                    {
                        process.Kill();
                        Assert.Fail($"the {what} example did not exit within 20 s ({receiver} first: {receiverFirst})");
                    }
                    Assert.True(process.ExitCode == 0,
                        $"the {what} example failed: exit code {process.ExitCode} ({receiver} first: {receiverFirst})");
                }
                string[] lines = printed.GetAwaiter().GetResult().Split('\n', StringSplitOptions.RemoveEmptyEntries)
                    .Select(line => line.TrimEnd('\r')).ToArray();
                Assert.Equal(expected, anyOrder ? lines.Order(StringComparer.Ordinal).ToArray() : lines);
            }
        }

        /// <summary>The dotnet host that runs this test, to run the example projects' assemblies with.</summary>
        private static string DotnetHost()
        {
            // The runtime's directory is <dotnet root>/shared/Microsoft.NETCore.App/<version>/
            string root = Path.GetFullPath(Path.Combine(
                System.Runtime.InteropServices.RuntimeEnvironment.GetRuntimeDirectory(), "..", "..", ".."));
            string host = Path.Combine(root, OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet");
            return File.Exists(host) ? host : "dotnet";
        }

        private static Process Start(string project)
        {
            var start = new ProcessStartInfo(DotnetHost())
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
            };
            start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, project + ".dll"));
            return Process.Start(start)!;
        }
    }
}
