using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using Xunit;

#nullable enable

namespace FastIpc.IntegrationTests
{
    /// <summary>
    /// Every C# block of the documentation is code that compiles: each block of the READMEs, the website's C# page and
    /// the overview must appear, line for line, in examples/csharp or in Snippets/ (which <see cref="Snippets.PatternTests"/>
    /// runs); each XML block in this project's file. Blank lines and indentation don't count.
    /// </summary>
    public class DocsTests
    {
        /// <summary>The lines that count: trimmed, without blank ones.</summary>
        private static string[] Lines(string text) =>
            text.Split('\n').Select(l => l.Trim()).Where(l => l.Length > 0).ToArray();

        /// <summary>The blocks of one language in a Markdown file (```csharp) or an HTML page (class="language-csharp").</summary>
        private static IEnumerable<string> Blocks(string path, string language)
        {
            string text = File.ReadAllText(path).Replace("\r\n", "\n");
            string pattern = path.EndsWith(".html")
                ? $"<code class=\"language-{language}\">(.*?)</code>"
                : $"```{language}\n(.*?)\n```";
            return Regex.Matches(text, pattern, RegexOptions.Singleline)
                .Select(m => path.EndsWith(".html") ? WebUtility.HtmlDecode(m.Groups[1].Value) : m.Groups[1].Value);
        }

        private static bool Contains(string[] source, string[] block) =>
            Enumerable.Range(0, Math.Max(0, source.Length - block.Length + 1))
                .Any(i => source.Skip(i).Take(block.Length).SequenceEqual(block));

        private static string Repository()
        {
            string? dir = AppContext.BaseDirectory;
            while (dir != null && !File.Exists(Path.Combine(dir, "include", "fipc.h")))
                dir = Path.GetDirectoryName(dir);
            return dir ?? throw new InvalidOperationException("no checkout above " + AppContext.BaseDirectory);
        }

        [Fact]
        public void EveryCSharpBlockOfTheDocs_IsCompiled()
        {
            string repo = Repository();
            string tests = Path.Combine(repo, "tests", "csharp", "Fipc.IntegrationTests");
            var sources = Directory.GetFiles(Path.Combine(repo, "examples", "csharp"), "Program.cs",
                    SearchOption.AllDirectories)
                .Concat(Directory.GetFiles(Path.Combine(tests, "Snippets"), "*.cs"))
                .Select(f => Lines(File.ReadAllText(f).Replace("\r\n", "\n")))
                .ToArray();
            string[] project = Lines(File.ReadAllText(Path.Combine(tests, "Fipc.IntegrationTests.csproj")));

            string[] docs =
            {
                Path.Combine(repo, "README.md"),
                Path.Combine(repo, "bindings", "csharp", "README.md"),
                Path.Combine(repo, "website", "csharp.html"),
                Path.Combine(repo, "website", "index.html"),
            };
            int checkedBlocks = 0;
            foreach (string doc in docs)
            {
                foreach (string block in Blocks(doc, "csharp"))
                {
                    string[] lines = Lines(block);
                    Assert.True(sources.Any(source => Contains(source, lines)),
                        $"{doc}: this block is compiled nowhere (examples/csharp, {tests}/Snippets):\n{block}");
                    checkedBlocks++;
                }
                foreach (string block in Blocks(doc, "xml"))
                {
                    Assert.True(Contains(project, Lines(block)),
                        $"{doc}: this block isn't in Fipc.IntegrationTests.csproj:\n{block}");
                    checkedBlocks++;
                }
            }
            Assert.True(checkedBlocks >= 30, $"only {checkedBlocks} blocks found");
        }
    }
}
