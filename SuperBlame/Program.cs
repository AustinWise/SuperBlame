using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SuperBlame
{
    class Program
    {
        private static readonly Dictionary<string, string> NameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "Niesner, Eduard (Consultant)", "Eduard Niesner" },
            { "3lpeana","Lucian Peana" },
            { "Cirstea, Cosmin", "Cosmin Cirstea" },
            { "Florea, Sergiu (Consultant)", "Sergiu Florea" },
            { "Dinh, Anthony", "Anthony Dinh" },
            { "Cosas, Deian (Consultant)", "Deian Cosas" },
            { "Wrye, Nathanael", "Nathanael Wrye" },
            { "dsun", "David Sun" },
            { "Sun, David", "David Sun" },
            { "Wang", "Roy Wang" },
            { "admin", "unknown" },
            { "Graciano, Justo", "Justo Graciano" },
            { "Adam, Florin (Consultant)", "Florin Adam" },
            { "Merrill, Jeffrey", "Jeffrey Merrill" },
            { "Carra-Farago, Adina (Consultant)", "Adina Carra-Farago" },
        };

        private static readonly SortedSet<string> sInitalInportCommits = new SortedSet<string>()
        {
            "93ba65338271090e9bdb8df4481b11b091a7acab",
            "c96a9c7503ec4bf0e335cab0ec019c03f1b5732b",
            "05dce6c64638d6f7a6e2aaa98d9cc7e9dbec9ab0",
            "a78baacf19106b877f28d2a527ed3ca062c02605",
            "3f532468d00e55a1e35fb41c8441388751fcd670",
            "b3d39695557431bff38dcac76289b37bf5084400",
            "e1c13af86ddbc9b522c06d5550aa12e3f39847f3",
            "7f74f9bf1604dd1f3776c9117497f9be07d19245",
            "57c29f08edeca152ee5e3af1ec89dc87448799a1",
            "a0b615815c674d6dc0a9d682ff42338e6e8fd070",
            "4dddc46a0de6f4b901699fb773bdc72e36f532b2",
        };

        const string DIR = @"d:\src\EC";
        static void Main(string[] args)
        {
            Main().Wait();
        }

        static async Task Main()
        {
            var p = StartWithOutputRedirect(@"ls-files -z HEAD -- *.cs");
            var stuff = p.StandardOutput.ReadToEnd().Split('\0');
            p.WaitForExit();
            if (p.ExitCode != 0)
                throw new Exception();


            var sumEachFile = new TransformBlock<string, Dictionary<string, int>>(
                new Func<string, Task<Dictionary<string, int>>>(ProcessBlame),
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = 12
                });

            var batcher = new BatchBlock<Dictionary<string, int>>(100);

            var linkOption = new DataflowLinkOptions() { PropagateCompletion = true };
            sumEachFile.LinkTo(batcher, linkOption);

            var tIn = PushAllIn(stuff, sumEachFile);

            var tOut = GetAllOut(batcher);

            await Task.WhenAll(tIn, tOut);

            var total = await tOut;

            foreach (var kvp in total.OrderByDescending(kvp => kvp.Value))
            {
                Console.WriteLine($"{kvp.Key}: {kvp.Value}");
            }

            Console.WriteLine();
        }

        private static async Task<Dictionary<string, int>> GetAllOut(BatchBlock<Dictionary<string, int>> batcher)
        {
            var totalSum = new Dictionary<string, int>();
            while (await batcher.OutputAvailableAsync())
            {
                Dictionary<string, int>[] batchedDicList;
                if (!batcher.TryReceive(out batchedDicList))
                    continue;
                foreach (var dic in batchedDicList)
                {
                    foreach (var kvp in dic)
                    {
                        int currentCount;
                        string realName;
                        if (!NameMap.TryGetValue(kvp.Key, out realName))
                            realName = kvp.Key;
                        if (!totalSum.TryGetValue(realName, out currentCount))
                            currentCount = 0;
                        currentCount += kvp.Value;
                        totalSum[realName] = currentCount;
                    }
                }
            }

            return totalSum;
        }

        private static async Task PushAllIn(string[] stuff, TransformBlock<string, Dictionary<string, int>> sumEachFile)
        {
            foreach (var file in stuff)
            {
                bool sent = await sumEachFile.SendAsync(file);
                if (!sent)
                    throw new Exception();
            }
            sumEachFile.Complete();
        }

        static readonly char[] Space = new[] { ' ' };
        static async Task<Dictionary<string, int>> ProcessBlame(string filePath)
        {
            var ret = new Dictionary<string, int>();
            if (string.IsNullOrEmpty(filePath))
                return ret;
            if (filePath.EndsWith(".designer.cs", StringComparison.OrdinalIgnoreCase))
                return ret;

            var p = StartWithOutputRedirect($"blame -p -w HEAD -- \"{filePath}\"");
            var stdout = p.StandardOutput;

            var commitToAuthorMap = new Dictionary<string, string>();

#if DEBUG
            var sb = new StringBuilder();
#endif

            string line;
            while (null != (line = await stdout.ReadLineAsync()))
            {
#if DEBUG
                sb.AppendLine(line);
#endif

                var splits = line.Split(Space);
                Debug.Assert(splits.Length == 4);
                Debug.Assert(splits[0].Length == 40);
                if (splits.Length != 4 || splits[0].Length != 40)
                {
                    Console.WriteLine("bad: " + filePath);
                    return new Dictionary<string, int>();
                }

                string hash = splits[0];
                int lineCount = int.Parse(splits[3], NumberStyles.None, CultureInfo.InvariantCulture);
                int linesToEat = lineCount * 2;

                if (sInitalInportCommits.Contains(hash))
                    lineCount = 0;

                if (commitToAuthorMap.ContainsKey(hash))
                {
                    ret[commitToAuthorMap[hash]] += lineCount;
                    linesToEat -= 1;
                }
                else
                {
                    linesToEat -= 2;

                    string author = null;
                    while (null != (line = await stdout.ReadLineAsync()))
                    {
#if DEBUG
                        sb.AppendLine(line);
#endif

                        if (line[0] == '\t')
                            break;
                        const string AUTHOR = "author ";
                        if (line.StartsWith(AUTHOR))
                        {
                            author = line.Substring(AUTHOR.Length);
                        }
                    }

                    if (author == null)
                        throw new Exception();

                    commitToAuthorMap[hash] = author;
                    if (ret.ContainsKey(author))
                        ret[author] += lineCount;
                    else
                        ret.Add(author, lineCount);
                }

                for (int i = 0; i < linesToEat; i++)
                {
                    line = await stdout.ReadLineAsync();
#if DEBUG
                    sb.AppendLine(line);
#endif
                    if (line == null)
                        throw new Exception();
                }
            }

            if (p.ExitCode != 0)
                throw new Exception();

            return ret;
        }

        static Process StartWithOutputRedirect(string args)
        {
            var psi = new ProcessStartInfo("git", args);
            psi.WorkingDirectory = DIR;
            psi.UseShellExecute = false;
            psi.CreateNoWindow = true;
            psi.RedirectStandardOutput = true;

            var p = Process.Start(psi);

            return p;
        }
    }
}
