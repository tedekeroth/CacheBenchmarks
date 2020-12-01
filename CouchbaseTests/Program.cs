using System;
using System.Threading.Tasks;

namespace CouchbaseTests
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            CouchbaseTests ct = new CouchbaseTests(args[0]);
            await ct.Init();

            if (args.Length <= 1 || args[1].Contains("C"))
                await ct.CreateJobs(1000000, CouchbaseTests.Database.RediSql );
            if (args.Length <= 1 || args[1].Contains("R"))
                await ct.SelectRandomJobs(1000, CouchbaseTests.Database.RediSql);
        }
    }
}
