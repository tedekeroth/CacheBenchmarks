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
            await ct.CreateJobs(100000, CouchbaseTests.Database.Couchbase);
            await ct.SelectRandomJobs(1000, CouchbaseTests.Database.Couchbase);
        }
    }
}
