using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SuperBlame
{
    class Program
    {
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

            var sumEachFile = new TransformBlock<string, LineCount>(
                new Func<string, Task<LineCount>>(ProcessBlame),
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount,
                });

            var batcher = new BatchBlock<LineCount>(100);

            var linkOption = new DataflowLinkOptions() { PropagateCompletion = true };
            sumEachFile.LinkTo(batcher, linkOption);

            var tIn = Task.Run(() => PushAllIn(stuff, sumEachFile));

            var tOut = Task.Run(() => GetAllOut(batcher));

            await Task.WhenAll(tIn, tOut);

            var total = await tOut;

            using (var sw = new StreamWriter("countByAuthor.csv"))
            {
                foreach (var kvp in total.ByAuthor.OrderByDescending(kvp => kvp.Value))
                {
                    await sw.WriteLineAsync($"{kvp.Key},{kvp.Value}");
                }
            }

            using (var sw = new StreamWriter("countByTimeStamp.csv"))
            {
                var q = total.ByUnixTimeStamp.Select(kvp => Tuple.Create(DateTimeOffset.FromUnixTimeSeconds(kvp.Key), kvp.Value))
                                             .GroupBy(tup => tup.Item1.Date)
                                             .OrderByDescending(group => group.Key);
                foreach (var day in q)
                {
                    await sw.WriteLineAsync($"{day.Key:yyyy/MM/dd},{day.Sum(s => s.Item2)}");
                }
            }

            Console.WriteLine("Done.");
        }

        private static async Task<LineCount> GetAllOut(BatchBlock<LineCount> batcher)
        {
            var totalSum = new LineCount();
            while (await batcher.OutputAvailableAsync())
            {
                LineCount[] batchedDicList;
                if (!batcher.TryReceive(out batchedDicList))
                    continue;
                foreach (var lc in batchedDicList)
                {
                    foreach (var kvp in lc.ByAuthor)
                    {
                        int currentCount;
                        if (!totalSum.ByAuthor.TryGetValue(kvp.Key, out currentCount))
                            currentCount = 0;
                        currentCount += kvp.Value;
                        totalSum.ByAuthor[kvp.Key] = currentCount;
                    }
                    foreach (var kvp in lc.ByUnixTimeStamp)
                    {
                        int currentCount;
                        if (!totalSum.ByUnixTimeStamp.TryGetValue(kvp.Key, out currentCount))
                            currentCount = 0;
                        currentCount += kvp.Value;
                        totalSum.ByUnixTimeStamp[kvp.Key] = currentCount;
                    }
                }
            }

            return totalSum;
        }

        private static async Task PushAllIn(string[] stuff, TransformBlock<string, LineCount> sumEachFile)
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
        static async Task<LineCount> ProcessBlame(string filePath)
        {
            var ret = new LineCount();
            if (string.IsNullOrEmpty(filePath))
                return ret;
            if (filePath.EndsWith(".designer.cs", StringComparison.OrdinalIgnoreCase))
                return ret;

            var p = StartWithOutputRedirect($"blame -p -w HEAD -- \"{filePath}\"");
            var stdout = p.StandardOutput;

            var commitToAuthorMap = new Dictionary<string, string>();
            var commitToTimestampMap = new Dictionary<string, long>();

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
                    return new LineCount();
                }

                string hash = splits[0];
                int lineCount = int.Parse(splits[3], NumberStyles.None, CultureInfo.InvariantCulture);
                int linesToEat = lineCount * 2;

                if (sInitalInportCommits.Contains(hash))
                    lineCount = 0;

                if (commitToAuthorMap.ContainsKey(hash))
                {
                    ret.ByAuthor[commitToAuthorMap[hash]] += lineCount;
                    ret.ByUnixTimeStamp[commitToTimestampMap[hash]] += lineCount;
                    linesToEat -= 1;
                }
                else
                {
                    linesToEat -= 2;

                    string author = null;
                    long? authorTime = null;
                    while (null != (line = await stdout.ReadLineAsync()))
                    {
#if DEBUG
                        sb.AppendLine(line);
#endif

                        if (line[0] == '\t')
                            break;

                        const string AUTHOR = "author ";
                        const string AUTHOR_TIME = "author-time ";

                        if (line.StartsWith(AUTHOR))
                        {
                            author = line.Substring(AUTHOR.Length);
                        }
                        else if (line.StartsWith(AUTHOR_TIME))
                        {
                            authorTime = long.Parse(line.Substring(AUTHOR_TIME.Length));
                        }
                    }

                    if (author == null || authorTime == null)
                        throw new Exception();

                    commitToAuthorMap[hash] = author;
                    int countForAuthor;
                    if (!ret.ByAuthor.TryGetValue(author, out countForAuthor))
                        countForAuthor = 0;
                    countForAuthor += lineCount;
                    ret.ByAuthor[author] = countForAuthor;

                    commitToTimestampMap[hash] = authorTime.Value;
                    int countForTimeStamp;
                    if (!ret.ByUnixTimeStamp.TryGetValue(authorTime.Value, out countForTimeStamp))
                        countForTimeStamp = 0;
                    countForTimeStamp += lineCount;
                    ret.ByUnixTimeStamp[authorTime.Value] = countForTimeStamp;
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

            p.WaitForExit();

            if (p.ExitCode != 0)
                throw new Exception();

            return ret;
        }

        class LineCount
        {
            public Dictionary<string, int> ByAuthor { get; } = new Dictionary<string, int>();
            public Dictionary<long, int> ByUnixTimeStamp { get; } = new Dictionary<long, int>();
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
